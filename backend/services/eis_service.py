import os
import re
import shutil
import sqlite3
import time
import random
import traceback
import asyncio
import sys
import html
import zipfile
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Optional, Set, Dict, Any, Tuple
from urllib.parse import urlencode, urljoin, urlparse, parse_qs, unquote
import logging
import requests

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PwTimeoutError

# Fix for Windows NotImplementedError with Playwright
if sys.platform == 'win32':
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    except Exception:
        pass

from backend.services.auto_ssh import RfProxyTunnelConfig, RfProxyHttpClient

from backend.config import DOCUMENTS_ROOT, DATA_DIR

# =========================
# НАСТРОЙКИ
# =========================
BASE = "https://zakupki.gov.ru"
SEARCH_URL = f"{BASE}/epz/order/extendedsearch/results.html"

# --- PATHS (Relative to project root) ---
DB_PATH = os.path.join(DATA_DIR, "seen.sqlite")
STATE_PATH = os.path.join(DATA_DIR, "pw_state.json")

USE_PROXY = os.getenv("USE_PROXY", "true").lower() == "true"
LOCAL_SOCKS_PORT = 1080

DIRECT_CONNECT_TIMEOUT = int(os.getenv("EIS_DIRECT_CONNECT_TIMEOUT", "15"))
DIRECT_READ_TIMEOUT = int(os.getenv("EIS_DIRECT_READ_TIMEOUT", "60"))
PROXY_CONNECT_TIMEOUT = int(os.getenv("EIS_PROXY_CONNECT_TIMEOUT", "25"))
PROXY_READ_TIMEOUT = int(os.getenv("EIS_PROXY_READ_TIMEOUT", "120"))
DOCS_HTTP_TIMEOUT = int(os.getenv("EIS_DOCS_HTTP_TIMEOUT", "30"))

def ensure_dir(p: str):
    if p:
        os.makedirs(p, exist_ok=True)

# Use a specific logger for this service
logger = logging.getLogger("EIS_Service")
# logger.setLevel(logging.INFO) - Удалено для использования общего уровня DEBUG

def log(message: str):
    logger.info(message)

def log_skip(message: str):
    logger.info(message)

def log_exception(prefix: str, exc: Exception):
    logger.error(f"{prefix}: {exc}", exc_info=True)

# =========================
# ТУННЕЛЬ / ПРОКСИ
# =========================
SSH_TAILSCALE_IP = "100.75.209.12"
SSH_USER = "vitt"

RF_CFG = RfProxyTunnelConfig(
    ssh_host=SSH_TAILSCALE_IP,
    ssh_user=SSH_USER,
    local_socks_port=LOCAL_SOCKS_PORT,
    allowed_domains=("zakupki.gov.ru",),
    warmup_url="https://zakupki.gov.ru/epz/main/public/home.html",
)

RF_CLIENT_PROXY: Optional[RfProxyHttpClient] = None
RF_CLIENT_DIRECT: Optional[requests.Session] = None

# =========================
# SQLite + CSV
# =========================
def db_init():
    ensure_dir(os.path.dirname(DB_PATH))
    with sqlite3.connect(DB_PATH) as con:
        con.execute(
            "CREATE TABLE IF NOT EXISTS seen (regNumber TEXT PRIMARY KEY, ts DATETIME DEFAULT CURRENT_TIMESTAMP)"
        )

def is_seen(reg: str) -> bool:
    with sqlite3.connect(DB_PATH) as con:
        cur = con.execute("SELECT 1 FROM seen WHERE regNumber=?", (reg,))
        return cur.fetchone() is not None

def mark_seen(reg: str):
    with sqlite3.connect(DB_PATH) as con:
        con.execute("INSERT OR IGNORE INTO seen(regNumber) VALUES(?)", (reg,))

# Initialize on module load
try:
    db_init()
except Exception as e:
    print(f"Init error: {e}")

# =========================
# РЕГЕКСЫ И КОНСТАНТЫ
# =========================
NOTICE_LINK_SELECTOR = "a[href*='/epz/order/notice/']"

REGNUMBER_RE = re.compile(r"[?&]regNumber=(\d+)")
NOTICEINFOID_RE = re.compile(r"[?&]noticeInfoId=(\d+)")

TECHNICAL_NOTICE_PARTS = (
    "/printForm/",
    "/print-form/",
    "listModal",
)

PREFERRED_NOTICE_PARTS = (
    "/common-info",
    "/documents",
    "/contract-info",
    "/plan.html",
)

RUB_PRICE_RE = re.compile(
    r"(\d[\d\s\xa0]*,\d{2}\s*(?:₽|руб\.?|рублей))",
    flags=re.IGNORECASE,
)

DEADLINE_RE = re.compile(
    r"\b\d{2}\.\d{2}\.\d{4}(?:\s+\d{2}:\d{2})?\b"
)

NO_RESULTS_PATTERNS = [
    "по вашему запросу ничего не найдено",
    "ничего не найдено",
    "результаты не найдены",
    "не найдено",
]

DOC_URL_MARKERS = (
    "download",
    "downloadfile",
    "download.html?id=",
    "attachment",
    "filestore",
)

FILE_EXT_RE = re.compile(r"\.(pdf|doc|docx|xls|xlsx|zip|rar|7z|rtf|ods)\b", re.IGNORECASE)


def safe_log_text(value: str) -> str:
    text = (value or "").replace("\n", " ").replace("\r", " ").strip()
    text = re.sub(r"\s+", " ", text)
    return text[:500]


def normalize_candidate_filename(value: str) -> str:
    value = fix_header_filename(value or "")
    value = value.replace("\xa0", " ").strip()
    value = re.sub(r"\s+", " ", value)
    return safe_filename(value)


def guess_filename_from_anchor(a) -> str:
    candidates = []

    for attr in ("download", "title", "data-original-title", "aria-label"):
        val = a.get(attr)
        if val:
            candidates.append(val)

    text = a.get_text(" ", strip=True)
    if text:
        candidates.append(text)

    row = a.find_parent(["tr", "li", "div"])
    if row is not None:
        row_text = row.get_text(" ", strip=True)
        if row_text:
            candidates.append(row_text)

    for raw in candidates:
        cleaned = normalize_candidate_filename(raw)
        if not cleaned:
            continue
        if FILE_EXT_RE.search(cleaned):
            return cleaned

    for raw in candidates:
        cleaned = normalize_candidate_filename(raw)
        if cleaned and len(cleaned) >= 3:
            return cleaned

    return ""


def build_documents_url(notice: "Notice") -> str:
    if notice.docs_url:
        return notice.docs_url

    href = notice.href or ""
    parsed = urlparse(href)
    qs = parse_qs(parsed.query)

    notice_info_id = qs.get("noticeInfoId", [""])[0]
    reg_number = qs.get("regNumber", [""])[0] or notice.reg

    if notice_info_id and "notice223" in href:
        return f"{BASE}/epz/order/notice/notice223/documents.html?noticeInfoId={notice_info_id}"

    if notice.ntype and reg_number:
        return f"{BASE}/epz/order/notice/{notice.ntype}/view/documents.html?regNumber={reg_number}"

    return href


def analyze_docs_page(html: str) -> Tuple[bool, bool, str]:
    soup = BeautifulSoup(html, "html.parser")
    has_card_attachments = bool(
        soup.select("div.blockFilesTabDocs, .attachment, .attachments, .card-attachment, table, tr a[href]")
    )
    has_download_href = False
    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip().lower()
        if any(marker in href for marker in DOC_URL_MARKERS):
            has_download_href = True
            break
    page_title = soup.title.get_text(" ", strip=True) if soup.title else ""
    return has_card_attachments, has_download_href, safe_log_text(page_title)

def human_sleep(min_s: float = 1.5, max_s: float = 4.5):
    time.sleep(random.uniform(min_s, max_s))

def long_pause_every(n: int, counter: int, min_s: float = 10.0, max_s: float = 25.0):
    if counter > 0 and counter % n == 0:
        time.sleep(random.uniform(min_s, max_s))

@dataclass
class Notice:
    reg: str
    ntype: str
    keyword: str
    search_url: str
    title: str = ""
    href: str = ""
    object_info: str = ""
    initial_price: str = ""
    application_deadline: str = ""
    seen: bool = False
    docs_url: str = ""

# =========================
# ФАЙЛЫ
# =========================
def safe_filename(name: str) -> str:
    name = (name or "").strip()
    name = re.sub(r'[<>:"/\\|?*\x00-\x1F]', "_", name)
    name = name.strip().strip(".")
    return name[:180] if len(name) > 180 else name

def looks_like_mojibake(s: str) -> bool:
    return any(ch in s for ch in ["Ð", "Ñ", "Ã", "Â"]) and not re.search(r"[А-Яа-яЁё]", s)

def fix_header_filename(s: str) -> str:
    if not s:
        return s
    s = s.strip().strip('"').strip()
    if re.search(r"[А-Яа-яЁё]", s):
        return s
    if looks_like_mojibake(s):
        try:
            restored = s.encode("latin1", errors="ignore").decode("utf-8", errors="ignore")
            if re.search(r"[А-Яа-яЁё]", restored):
                return restored
        except Exception:
            pass
    try:
        restored = s.encode("latin1", errors="ignore").decode("cp1251", errors="ignore")
        if re.search(r"[А-Яа-яЁё]", restored):
            return restored
    except Exception:
        pass
    return s

def filename_from_content_disposition(cd: str) -> Optional[str]:
    if not cd:
        return None

    m = re.search(r"filename\*\s*=\s*UTF-8''([^;]+)", cd, flags=re.IGNORECASE)
    if m:
        try:
            return safe_filename(unquote(m.group(1)))
        except Exception:
            pass

    m = re.search(r'filename\s*=\s*"?([^";]+)"?', cd, flags=re.IGNORECASE)
    if m:
        raw = fix_header_filename(m.group(1))
        return safe_filename(raw)

    return None

def guess_extension_from_content_type(ct: str) -> str:
    ct = (ct or "").lower()
    if "pdf" in ct:
        return ".pdf"
    if "wordprocessingml" in ct:
        return ".docx"
    if "msword" in ct:
        return ".doc"
    if "spreadsheetml" in ct:
        return ".xlsx"
    if "excel" in ct:
        return ".xls"
    if "zip" in ct:
        return ".zip"
    if "rar" in ct:
        return ".rar"
    if "7z" in ct or "7-zip" in ct:
        return ".7z"
    return ""

def uid_from_url(u: str) -> str:
    qs = parse_qs(urlparse(u).query)
    return qs.get("uid", [""])[0]

def parse_docs_block(docs_html: str) -> List[tuple[str, str]]:
    soup = BeautifulSoup(docs_html, "html.parser")

    items: List[Tuple[str, str]] = []
    seen_urls = set()

    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if not href:
            continue

        href_lower = href.lower()
        onclick_lower = (a.get("onclick") or "").lower()

        is_download_link = (
            any(marker in href_lower for marker in DOC_URL_MARKERS)
            or any(marker in onclick_lower for marker in DOC_URL_MARKERS)
        )

        if not is_download_link:
            continue

        if href.startswith("javascript:"):
            continue

        full_href = urljoin(BASE, href)
        if full_href in seen_urls:
            continue

        suggested = guess_filename_from_anchor(a)
        if not suggested:
            uid = uid_from_url(full_href) or str(abs(hash(full_href)))
            suggested = f"file_{uid}"

        suggested = normalize_candidate_filename(suggested) or "file.bin"

        seen_urls.add(full_href)
        items.append((full_href, suggested))

    return items

def get_http_client(force_direct: bool = False):
    global RF_CLIENT_PROXY, RF_CLIENT_DIRECT

    if force_direct or not USE_PROXY:
        if RF_CLIENT_DIRECT is None:
            RF_CLIENT_DIRECT = requests.Session()
            RF_CLIENT_DIRECT.headers.update({
                "User-Agent": "Mozilla/5.0",
                "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
            })
        return RF_CLIENT_DIRECT

    if RF_CLIENT_PROXY is None:
        from backend.services.auto_ssh import RfProxyHttpClient
        RF_CLIENT_PROXY = RfProxyHttpClient(RF_CFG)
    return RF_CLIENT_PROXY


def _request_timeout(force_direct: bool):
    if force_direct:
        return 10   # Быстрый фейл: если DNS не резолвится за 10 сек — не ждём
    return (PROXY_CONNECT_TIMEOUT, PROXY_READ_TIMEOUT)


def _looks_like_html_bytes(blob: bytes) -> bool:
    if not blob:
        return False
    try:
        text = blob[:4096].decode("utf-8", errors="ignore").lower()
    except Exception:
        return False
    return (
        "<html" in text
        or "<!doctype html" in text
        or "<head" in text
        or "<body" in text
    )


def _looks_like_pdf_bytes(blob: bytes) -> bool:
    return bool(blob[:5] == b"%PDF-")


def _looks_like_zip_bytes(blob: bytes) -> bool:
    return bool(blob[:4] == b"PK\x03\x04" or blob[:4] == b"PK\x05\x06" or blob[:4] == b"PK\x07\x08")


def _looks_like_ole_bytes(blob: bytes) -> bool:
    return bool(blob[:8] == b"\xD0\xCF\11\xE0\xA1\xB1\x1A\xE1")


def _guess_file_family(filename: str, content_type: str) -> str:
    filename = (filename or "").lower()
    content_type = (content_type or "").lower()

    if filename.endswith(".pdf") or "pdf" in content_type:
        return "pdf"
    if filename.endswith(".docx") or "wordprocessingml" in content_type:
        return "zip_docx"
    if filename.endswith(".xlsx") or "spreadsheetml" in content_type:
        return "zip_xlsx"
    if filename.endswith(".zip") or "application/zip" in content_type:
        return "zip"
    if filename.endswith(".doc") or "msword" in content_type:
        return "ole_doc"
    if filename.endswith(".xls") or "application/vnd.ms-excel" in content_type:
        return "ole_xls"
    return "generic"


def _validate_download_prefix(prefix: bytes, filename: str, content_type: str) -> None:
    if not prefix:
        raise ValueError("Пустой ответ от сервера")

    if _looks_like_html_bytes(prefix):
        raise ValueError("Сервер вернул HTML вместо файла")

    family = _guess_file_family(filename, content_type)

    if family == "pdf" and not _looks_like_pdf_bytes(prefix):
        raise ValueError("Файл заявлен как PDF, но сигнатура PDF отсутствует")

    if family in {"zip_docx", "zip_xlsx", "zip"} and not _looks_like_zip_bytes(prefix):
        raise ValueError("Файл заявлен как DOCX/XLSX/ZIP, но ZIP-сигнатура отсутствует")

    if family in {"ole_doc", "ole_xls"} and not _looks_like_ole_bytes(prefix):
        logger.warning(
            "Файл %s не имеет OLE-сигнатуры. Возможен XML/RTF/нестандартный контейнер от ЕИС. Сохраняем файл без блокировки.",
            filename,
        )
        return


def _validate_saved_file(path: str, filename: str, content_type: str) -> None:
    if not os.path.exists(path):
        raise ValueError("Файл не был сохранен на диск")

    size = os.path.getsize(path)
    if size <= 0:
        raise ValueError("Сохраненный файл пустой")

    family = _guess_file_family(filename, content_type)

    if family in {"zip_docx", "zip_xlsx", "zip"} and not zipfile.is_zipfile(path):
        raise ValueError("Сохраненный DOCX/XLSX/ZIP поврежден или неполон")

def download_file_with_real_name(file_url: str, reg_dir: str, suggested_title: str) -> str:
    last_errors = []

    for force_direct in ([False, True] if USE_PROXY else [True]):
        mode = "direct" if force_direct else "proxy"
        temp_path = None

        try:
            client = get_http_client(force_direct=force_direct)
            response = client.get(
                file_url,
                timeout=_request_timeout(force_direct),
                stream=True,
            )
            response.raise_for_status()

            cd = response.headers.get("Content-Disposition", "")
            ct = response.headers.get("Content-Type", "")

            filename = filename_from_content_disposition(cd)
            if not filename:
                ext = guess_extension_from_content_type(ct)
                if suggested_title:
                    if ext and suggested_title.lower().endswith(ext):
                        filename = suggested_title
                    else:
                        filename = suggested_title + (ext if ext else "")
                else:
                    uid = uid_from_url(file_url) or str(abs(hash(file_url)))
                    filename = uid + (ext if ext else ".bin")

            filename = safe_filename(filename) or "file.bin"

            base, ext = os.path.splitext(filename)
            out_path = os.path.join(reg_dir, filename)
            counter = 1
            while os.path.exists(out_path):
                out_path = os.path.join(reg_dir, f"{base}_{counter}{ext}")
                counter += 1

            content = response.content
            url = file_url

            # Проверяем что скачанный контент является реальным документом,
            # а не HTML-страницей редиректа или ошибки
            content_lower = content[:2000].lower() if content else b""
            is_html_response = (
                content_lower.startswith(b"<!doctype html") or
                content_lower.startswith(b"<html") or
                b"<html" in content_lower[:500]
            )

            if is_html_response:
                html_preview = content[:500].decode("utf-8", errors="replace")
                logger.error(
                    f"[DOWNLOAD_VALIDATION] Got HTML instead of file content! "
                    f"url={url} | size={len(content)} | "
                    f"preview={html_preview[:200]!r}"
                )
                raise ValueError(
                    f"Server returned HTML page instead of file content. "
                    f"URL may require authentication or redirect. url={url}"
                )

            # Проверяем минимальный размер файла (менее 100 байт — явно ошибка)
            if len(content) < 100:
                logger.error(
                    f"[DOWNLOAD_VALIDATION] File too small ({len(content)} bytes), "
                    f"likely an error page. url={url}"
                )
                raise ValueError(
                    f"Downloaded content too small ({len(content)} bytes). "
                    f"Likely an error response. url={url}"
                )

            if ext in ("doc", "xls"):
                ole_magic = b'\xD0\xCF\x11\xE0'
                xml_magic_variants = [b'<?xml', b'<html', b'PK\x03\x04']
                is_ole = content[:4] == ole_magic[:4]
                is_xml_or_zip = any(
                    content[:len(m)] == m for m in xml_magic_variants
                )
                if not is_ole and not is_xml_or_zip:
                    # Предупреждаем но НЕ бросаем исключение
                    # Файлы с госзакупок могут быть RTF или другого формата
                    logger.warning(
                        f"[OLE_CHECK] File {filename} has unexpected signature. "
                        f"First bytes: {content[:8].hex()}. "
                        f"Saving anyway — user can open manually."
                    )
                elif not is_ole and is_xml_or_zip:
                    logger.info(
                        f"[OLE_CHECK] File {filename} is XML/ZIP format "
                        f"(not binary OLE). This is normal for modern .doc/.xls files."
                    )
                # НЕ бросаем исключение ни в каком случае — просто сохраняем

            # Всё в порядке — сохраняем файл
            with open(out_path, "wb") as f:
                f.write(content)

            logger.info(
                "Downloaded file successfully | mode=%s | url=%s | path=%s",
                mode,
                file_url,
                out_path,
            )
            return out_path

        except Exception as e:
            if temp_path and os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except Exception:
                    pass

            error_text = f"{mode}: {e}"
            last_errors.append(error_text)
            logger.warning("Download attempt failed | %s | url=%s", error_text, file_url)

    raise RuntimeError(" ; ".join(last_errors))

class SearchLogger:
    def __init__(self, search_id: str):
        self.search_id = search_id

    def info(self, prefix: str, msg: str):
        logger.info(f"[{prefix}] [SearchID:{self.search_id}] {msg}")

    def debug(self, prefix: str, msg: str):
        logger.debug(f"[{prefix}] [SearchID:{self.search_id}] {msg}")

    def warning(self, prefix: str, msg: str):
        logger.warning(f"[{prefix}] [SearchID:{self.search_id}] {msg}")

    def error(self, prefix: str, msg: str):
        logger.error(f"[{prefix}] [SearchID:{self.search_id}] {msg}")

class EisService:
    _DOC_LINK_RE = re.compile(
        r"/download/(?:download|file)\.html\?(?:[^\"'#>\s]*\b)?id=\d+",
        re.IGNORECASE,
    )
    _SIGN_LINK_RE = re.compile(
        r"/download/signs/render\.html\?(?:[^\"'#>\s]*\b)?id=\d+",
        re.IGNORECASE,
    )
    _FILE_EXT_RE = re.compile(
        r"\.(pdf|doc|docx|xls|xlsx|rtf|zip|rar|7z|jpg|jpeg|png|tif|tiff)($|\s)",
        re.IGNORECASE,
    )
    _GENERIC_FILE_NAMES = {
        "microsoft word document",
        "microsoft excel document",
        "adobe acrobat document",
        "просмотреть эп",
    }

    def _clean_ws(self, value: str | None) -> str:
        if not value:
            return ""
        value = html.unescape(value)
        value = re.sub(r"\s+", " ", value)
        return value.strip()

    def _tooltip_to_text(self, raw: str | None) -> str:
        if not raw:
            return ""
        raw = html.unescape(raw)
        raw = re.sub(r"<[^>]+>", " ", raw)
        raw = re.sub(r"\s+", " ", raw)
        return raw.strip()

    def _extract_id_from_href(self, href: str) -> str:
        try:
            parsed = urlparse(href)
            q = parse_qs(parsed.query)
            value = q.get("id", [""])[0]
            return str(value).strip()
        except Exception:
            return ""

    def _looks_like_filename(self, value: str) -> bool:
        if not value:
            return False
        value_l = value.strip().lower()
        if value_l in self._GENERIC_FILE_NAMES:
            return False
        return bool(self._FILE_EXT_RE.search(value))

    def _guess_ext(self, file_name: str, file_url: str) -> str:
        for candidate in (file_name or "", urlparse(file_url).path or ""):
            ext = Path(candidate).suffix.lower().strip()
            if ext:
                return ext
        return ""

    def _extract_section_title_for_anchor(self, a_tag) -> str:
        current = a_tag
        while current is not None:
            classes = current.get("class") or []
            if isinstance(classes, str):
                classes = classes.split()
            if "card-attachments__block" in classes:
                title_tag = current.find("div", class_="title")
                if title_tag:
                    return self._clean_ws(title_tag.get_text(" ", strip=True))
            current = current.parent
        return ""

    def _extract_file_name_from_anchor(self, a_tag) -> str:
        candidates: list[str] = []

        for attr in ("data-tooltip", "title", "aria-label"):
            raw = a_tag.get(attr)
            if raw:
                text = self._tooltip_to_text(raw)
                if text:
                    candidates.append(text)

        text_inside = self._clean_ws(a_tag.get_text(" ", strip=True))
        if text_inside:
            candidates.append(text_inside)

        parent_text = self._clean_ws(a_tag.parent.get_text(" ", strip=True) if a_tag.parent else "")
        if parent_text:
            candidates.append(parent_text)

        # сначала ищем реальное имя файла с расширением
        for candidate in candidates:
            if self._looks_like_filename(candidate):
                return candidate

        # потом берём самый длинный непустой текст, если расширения нет
        filtered = [x for x in candidates if x and x.lower() not in self._GENERIC_FILE_NAMES]
        if filtered:
            return max(filtered, key=len)

        return ""

    def parse_fz44_docs_page(self, html: str, reg_number: str) -> list:
        """
        Парсит HTML страницы документов тендера ФЗ-44 (ea20 / zk20).

        Структура страницы (из анализа реальной страницы zakupki.gov.ru):
          <div class="attachment row">
            <span class="section__value">
              <a href="https://zakupki.gov.ru/44fz/filestore/public/1.0/download/priz/file.html?uid=XXXXX"
                 title="filename.xlsx">filename.xlsx</a>
            </span>
          </div>

        Возвращает список словарей:
          [{"url": "https://...uid=XXX", "title": "filename.xlsx", "uid": "XXX"}, ...]
        """
        if not html:
            logger.warning(f"[PARSE_FZ44] Empty HTML for {reg_number}")
            return []

        soup = BeautifulSoup(html, "html.parser")
        files = []
        seen_uids = set()

        # Ищем все блоки с прикреплёнными файлами
        attachment_rows = soup.find_all("div", class_=lambda c: c and "attachment" in c and "row" in c)

        if not attachment_rows:
            # Запасной вариант: ищем напрямую по паттерну URL
            logger.info(
                f"[PARSE_FZ44] No 'attachment row' divs found for {reg_number}. "
                f"Trying direct href search."
            )
            # Ищем все ссылки с /44fz/filestore/ в href
            for a_tag in soup.find_all("a", href=re.compile(r"/44fz/filestore/.*?uid=")):
                href = a_tag.get("href", "")
                uid_match = re.search(r"uid=([A-F0-9a-f]+)", href)
                if not uid_match:
                    continue
                uid = uid_match.group(1)
                if uid in seen_uids:
                    continue
                seen_uids.add(uid)
                title = (
                    a_tag.get("title")
                    or a_tag.get_text(strip=True)
                    or f"document_{uid[:8]}.bin"
                )
                download_url = href if href.startswith("http") else f"https://zakupki.gov.ru{href}"
                files.append({"url": download_url, "title": title, "uid": uid})
                logger.info(
                    f"[PARSE_FZ44] Found file (direct): '{title}' | uid={uid[:16]}..."
                )
            return files

        # Обрабатываем каждый attachment row
        for row in attachment_rows:
            # Ищем ссылку для скачивания (содержит /44fz/filestore/)
            download_a = row.find(
                "a",
                href=re.compile(r"/44fz/filestore/.*?uid=|/44fz/.*?download")
            )
            if not download_a:
                # Также пробуем ссылки с /epz/ для старого формата
                download_a = row.find(
                    "a",
                    href=re.compile(r"download|priz/file")
                )
            if not download_a:
                continue

            href = download_a.get("href", "")
            if not href:
                continue

            # Извлекаем UID
            uid_match = re.search(r"uid=([A-F0-9a-f]+)", href, re.IGNORECASE)
            uid = uid_match.group(1) if uid_match else ""

            # Дедупликация по UID
            if uid and uid in seen_uids:
                continue
            if uid:
                seen_uids.add(uid)

            # Получаем имя файла из title, затем из текста ссылки
            title = (
                download_a.get("title", "").strip()
                or download_a.get_text(strip=True)
            )
            if not title:
                # Пробуем получить имя из alt картинки рядом
                img = row.find("img", alt=True)
                ext_map = {
                    "xlsx": ".xlsx", "xls": ".xls",
                    "docx": ".docx", "doc": ".doc",
                    "pdf": ".pdf", "zip": ".zip",
                    "rar": ".rar", "txt": ".txt",
                }
                img_alt = (img.get("src", "") if img else "").lower()
                ext = next((v for k, v in ext_map.items() if k in img_alt), ".bin")
                title = f"document_{uid[:8] if uid else len(files)}{ext}"

            # Нормализуем URL
            download_url = href if href.startswith("http") else f"https://zakupki.gov.ru{href}"

            files.append({"url": download_url, "title": title, "uid": uid})
            logger.info(
                f"[PARSE_FZ44] Found file: '{title}' "
                f"| uid={uid[:16] if uid else 'n/a'}... "
                f"| url={download_url[:80]}"
            )

        logger.info(
            f"[PARSE_FZ44] Total files found for {reg_number}: {len(files)}"
        )
        return files

    def _parse_docs_html_universal(self, html_text: str, base_url: str) -> list[dict]:
        soup = BeautifulSoup(html_text or "", "html.parser")

        sign_urls_by_id: dict[str, str] = {}
        for a_tag in soup.select("a[href]"):
            raw_href = self._clean_ws(a_tag.get("href"))
            if not raw_href:
                continue
            if self._SIGN_LINK_RE.search(raw_href):
                abs_href = urljoin(base_url, raw_href)
                file_id = self._extract_id_from_href(abs_href)
                if file_id:
                    sign_urls_by_id[file_id] = abs_href

        files: list[dict] = []
        seen_urls: set[str] = set()

        for a_tag in soup.select("a[href]"):
            raw_href = self._clean_ws(a_tag.get("href"))
            if not raw_href:
                continue
            if not self._DOC_LINK_RE.search(raw_href):
                continue

            abs_href = urljoin(base_url, raw_href)
            if abs_href in seen_urls:
                continue
            seen_urls.add(abs_href)

            file_id = self._extract_id_from_href(abs_href)
            file_name = self._extract_file_name_from_anchor(a_tag)
            section_title = self._extract_section_title_for_anchor(a_tag)
            ext = self._guess_ext(file_name, abs_href)

            files.append(
                {
                    "file_id": file_id,
                    "name": file_name or (f"file_{file_id}" if file_id else "unknown_file"),
                    "url": abs_href,
                    "download_url": abs_href,
                    "sign_url": sign_urls_by_id.get(file_id, ""),
                    "section_title": section_title,
                    "ext": ext,
                    "source_page": base_url,
                }
            )

        return files

    def _resolve_documents_url(self, page, tender_url: str) -> str:
        current_url = page.url or tender_url
        if "documents.html" in current_url:
            return current_url

        candidates = [
            'a.tabsNav__item[href*="documents"]',
            'a[href*="documents"]:has-text("Документы")',
            'a[href*="documents"]',
        ]

        for selector in candidates:
            try:
                locator = page.locator(selector).first
                count = locator.count()
                if count > 0:
                    href = locator.get_attribute("href")
                    if href:
                        return urljoin(current_url, href)
            except Exception:
                pass

        rewritten = re.sub(r"/view/common-info\.html", "/view/documents.html", current_url)
        if rewritten != current_url:
            return rewritten

        rewritten = re.sub(r"/common-info\.html", "/documents.html", current_url)
        if rewritten != current_url:
            return rewritten

        return current_url

    def _wait_and_extract_docs_files(self, page, docs_url: str, tender_id: str, timeout_ms: int = 20000) -> list[dict]:
        started = time.monotonic()
        deadline = started + timeout_ms / 1000.0
        last_html = ""

        selectors = [
            'a[href*="/download/download.html?id="]',
            'a[href*="/download/file.html?id="]',
            '.card-attachments-container',
            '.card-attachments__block',
            'text=Документация по закупке',
            'text=Документы',
        ]

        while time.monotonic() < deadline:
            for selector in selectors:
                try:
                    locator = page.locator(selector)
                    if locator.count() > 0:
                        last_html = page.content()
                        files = self._parse_docs_html_universal(last_html, docs_url)
                        if files:
                            return files
                except Exception:
                    pass

            try:
                page.mouse.wheel(0, 3000)
            except Exception:
                pass

            page.wait_for_timeout(750)
            last_html = page.content()
            if "download.html?id=" in last_html or "card-attachments-container" in last_html:
                files = self._parse_docs_html_universal(last_html, docs_url)
                if files:
                    return files

        # финальная попытка после reload
        try:
            page.reload(wait_until="domcontentloaded")
            page.wait_for_timeout(1500)
            last_html = page.content()
        except Exception:
            pass

        return self._parse_docs_html_universal(last_html, docs_url)

    async def _fresh_retry_playwright(self, reg_number: str, ntype: str) -> list:
        """
        Повторная попытка получить список файлов тендера ФЗ-44
        через Playwright с чистым браузерным контекстом.

        Использует новый парсер parse_fz44_docs_page() который умеет
        читать реальную структуру страницы zakupki.gov.ru для ФЗ-44.

        Параметры:
          reg_number : регистрационный номер тендера (например 0853500000326001897)
          ntype      : тип закупки из БД (ea20, zk20 и т.д.)
        """
        logger.info(
            f"[FRESH_RETRY_PW] Starting fresh Playwright retry "
            f"for {reg_number} | ntype={ntype}"
        )

        # Формируем URL документов на основе реального ntype из БД
        docs_url = (
            f"https://zakupki.gov.ru/epz/order/notice/{ntype}/view/"
            f"documents.html?regNumber={reg_number}"
        )
        common_url = (
            f"https://zakupki.gov.ru/epz/order/notice/{ntype}/view/"
            f"common-info.html?regNumber={reg_number}"
        )

        logger.info(f"[FRESH_RETRY_PW] docs_url={docs_url}")

        from playwright.async_api import async_playwright
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            context = await browser.new_context(
                viewport={"width": 1280, "height": 900},
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
            )
            page = await context.new_page()

            try:
                # Шаг 1: Заходим на common-info (создаём реферер)
                logger.info(
                    f"[FRESH_RETRY_PW] Step 1/2: GOTO common-info: {common_url}"
                )
                try:
                    await page.goto(
                        common_url,
                        wait_until="domcontentloaded",
                        timeout=30_000
                    )
                    await page.wait_for_timeout(1_500)
                except Exception as e:
                    logger.warning(
                        f"[FRESH_RETRY_PW] common-info load warning: {e}"
                    )

                # Шаг 2: Переходим на страницу документов
                logger.info(
                    f"[FRESH_RETRY_PW] Step 2/2: GOTO documents: {docs_url}"
                )
                await page.goto(
                    docs_url,
                    wait_until="domcontentloaded",
                    timeout=35_000
                )

                # Ждём появления блока с файлами (до 15 сек)
                try:
                    await page.wait_for_selector(
                        "div.attachment.row, div[class*='attachment'][class*='row'], "
                        "a[href*='44fz/filestore'], a[href*='priz/file']",
                        timeout=15_000
                    )
                    logger.info(
                        f"[FRESH_RETRY_PW] Attachment block found for {reg_number}"
                    )
                except Exception:
                    logger.warning(
                        f"[FRESH_RETRY_PW] Attachment selector not found in 15s. "
                        f"Parsing whatever is loaded."
                    )

                # Получаем HTML и парсим новым парсером
                html_content = await page.content()
                files = self.parse_fz44_docs_page(html_content, reg_number)

                if files:
                    logger.info(
                        f"[FRESH_RETRY_PW] SUCCESS: {len(files)} files "
                        f"found for {reg_number}"
                    )
                else:
                    # Дополнительная диагностика
                    page_title = await page.title()
                    current_url = page.url
                    logger.warning(
                        f"[FRESH_RETRY_PW] No files found after Playwright retry. "
                        f"page_title='{page_title}' | current_url={current_url}"
                    )

                return files

            except Exception as e:
                logger.error(
                    f"[FRESH_RETRY_PW] Exception for {reg_number}: {e}"
                )
                return []
            finally:
                await browser.close()
                logger.info(
                    f"[FRESH_RETRY_PW] Browser closed for {reg_number}"
                )

    def redownload_tender_documents(self, notice: Notice, clear_dir: bool = True) -> Dict[str, Any]:
        tender_dir = os.path.join(DOCUMENTS_ROOT, notice.reg)

        if clear_dir and os.path.isdir(tender_dir):
            shutil.rmtree(tender_dir, ignore_errors=True)

        result = self.process_tenders([notice])
        if not result:
            return {"ok": False, "reason": "empty_result", "files": [], "meta": {}}

        item = result[0]
        ok = item.get("status") == "selected" and bool(item.get("files"))

        return {
            "ok": ok,
            "reason": item.get("reason", "" if ok else "download_failed"),
            "files": item.get("files", []),
            "meta": item,
        }

    def process_tenders(self, notices: List[Notice]) -> List[Dict]:
        if sys.platform == "win32":
            try:
                asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
            except Exception:
                pass

        results = []
        proxy_enabled = USE_PROXY

        if proxy_enabled:
            try:
                client = get_http_client(force_direct=False)
                logger.info("Warming up proxy session...")
                client.warmup()
            except Exception as e:
                logger.warning("Proxy warmup failed, switching to direct mode: %s", e)
                proxy_enabled = False

        try:
            with sync_playwright() as p:
                try:
                    logger.info("Launching Chromium for processing...")
                    browser = p.chromium.launch(
                        headless=self.HEADLESS,
                        slow_mo=self.SLOWMO_MS,
                        proxy={"server": f"socks5://127.0.0.1:{LOCAL_SOCKS_PORT}"} if proxy_enabled else None,
                    )
                except Exception as browser_err:
                    err_msg = str(browser_err)
                    if "playwright install" in err_msg.lower() or "executable doesn't exist" in err_msg.lower():
                        raise RuntimeError(
                            "Браузер Playwright не найден. Пожалуйста, выполните команду в терминале: "
                            ".venv\\Scripts\\python.exe -m playwright install chromium"
                        )
                    raise browser_err

                try:
                    if os.path.exists(STATE_PATH):
                        context = browser.new_context(
                            locale="ru-RU",
                            user_agent=self.REQ_HEADERS["User-Agent"],
                            storage_state=STATE_PATH,
                            viewport={"width": 1920, "height": 1080},
                        )
                        logger.info("[state] loaded: %s", STATE_PATH)
                    else:
                        context = browser.new_context(
                            locale="ru-RU",
                            user_agent=self.REQ_HEADERS["User-Agent"],
                            viewport={"width": 1920, "height": 1080},
                        )
                        logger.info("[state] fresh context (no saved state yet)")

                    page = context.new_page()

                    for n in notices:
                        log(f"--- Processing {n.reg} ---")
                        d_url = build_documents_url(n)

                        try:
                            self.goto_with_human_delays(page, d_url, wait="domcontentloaded", timeout=60000, retries=2)
                            page.wait_for_timeout(1500)

                            raw_items = self._wait_and_extract_docs_files(page, d_url, n.reg)
                            if not raw_items:
                                # Пробуем распарсить страницу новым парсером ФЗ-44
                                logger.info(
                                    f"[PROCESS] Primary selector found nothing. "
                                    f"Trying FZ44 parser for {n.reg}..."
                                )
                                try:
                                    html_content = page.content()
                                    raw_items = self.parse_fz44_docs_page(html_content, n.reg)
                                except Exception as parse_err:
                                    logger.warning(
                                        f"[PROCESS] FZ44 parser failed for {n.reg}: {parse_err}"
                                    )
                                    raw_items = []

                            if not raw_items:
                                # Если и новый парсер не нашёл — делаем Playwright retry
                                ntype_val = getattr(n, 'ntype', None) or 'ea20'
                                logger.warning(
                                    f"[PROCESS] No files found on primary docs page for "
                                    f"{n.reg} | ntype={ntype_val}. "
                                    f"Starting Playwright fresh retry..."
                                )
                                raw_items = asyncio.run(self._fresh_retry_playwright(n.reg, ntype_val))

                            if not raw_items:
                                html_text = page.content()
                                has_card_attachments, has_download_href, page_title = analyze_docs_page(html_text)
                                results.append({
                                    "reg": n.reg,
                                    "status": "skip",
                                    "reason": "no_files_found",
                                    "docs_url": d_url,
                                    "files": [],
                                    "has_card_attachments": has_card_attachments,
                                    "has_download_href": has_download_href,
                                    "page_title": page_title,
                                })
                                continue

                        except PwTimeoutError as e:
                            log_exception(f"Timeout fetching docs page {d_url}", e)
                            results.append({
                                "reg": n.reg,
                                "status": "skip",
                                "reason": "documents_timeout",
                                "docs_url": d_url,
                                "files": [],
                            })
                            continue
                        except Exception as e:
                            log_exception(f"Failed to fetch docs page {d_url}", e)
                            results.append({
                                "reg": n.reg,
                                "status": "error",
                                "reason": f"fetch_docs_page:{e}",
                                "docs_url": d_url,
                                "files": [],
                            })
                            continue

                        normalized_items = []
                        seen_downloads = set()

                        for item in raw_items:
                            if isinstance(item, dict):
                                file_url = (item.get("download_url") or item.get("url") or "").strip()
                                suggested_title = normalize_candidate_filename(item.get("name") or "")
                            else:
                                file_url = (item[0] or "").strip()
                                suggested_title = normalize_candidate_filename(item[1] or "")

                            if not file_url:
                                continue

                            dedup_key = (file_url, suggested_title)
                            if dedup_key in seen_downloads:
                                continue

                            seen_downloads.add(dedup_key)
                            normalized_items.append((file_url, suggested_title or "file.bin"))

                        if not normalized_items:
                            results.append({
                                "reg": n.reg,
                                "status": "skip",
                                "reason": "no_valid_download_links",
                                "docs_url": d_url,
                                "files": [],
                            })
                            continue

                        reg_dir = os.path.join(DOCUMENTS_ROOT, n.reg)
                        ensure_dir(reg_dir)

                        downloaded_files = []
                        failed_downloads = []

                        for file_url, suggested_title in normalized_items:
                            try:
                                out_path = download_file_with_real_name(file_url, reg_dir, suggested_title)
                                downloaded_files.append(out_path)
                                time.sleep(random.uniform(0.3, 1.0))
                            except Exception as e:
                                log_exception(f"Failed to download {file_url}", e)
                                failed_downloads.append({
                                    "url": file_url,
                                    "title": suggested_title,
                                    "error": str(e),
                                })

                        if downloaded_files:
                            mark_seen(n.reg)
                            results.append({
                                "reg": n.reg,
                                "status": "selected",
                                "docs_url": d_url,
                                "files": downloaded_files,
                                "failed_downloads": failed_downloads,
                            })
                        else:
                            results.append({
                                "reg": n.reg,
                                "status": "skip",
                                "reason": "all_downloads_failed",
                                "docs_url": d_url,
                                "files": [],
                                "failed_downloads": failed_downloads,
                            })

                    try:
                        neutral_page = context.new_page()
                        neutral_page.goto(
                            "https://zakupki.gov.ru",
                            wait_until="domcontentloaded",
                            timeout=15000,
                        )
                        ensure_dir(os.path.dirname(STATE_PATH))
                        context.storage_state(path=STATE_PATH)
                        neutral_page.close()
                        logger.info("[state] saved (from neutral page): %s", STATE_PATH)
                    except Exception as e:
                        logger.warning("[state] Failed to save state: %s", e)

                finally:
                    browser.close()
                    logger.info("Browser closed.")

        except Exception as global_err:
            logger.error("Playwright Global Error: %s", global_err, exc_info=True)
            return [{"status": "error", "reason": str(global_err)}]

        return results
    def __init__(self):
        self.RECORDS_PER_PAGE = 50
        self.MAX_PAGES = 5
        self.OKPD2_IDS_WITH_NESTED = True
        self.OKPD2_IDS = "8873861,8873862,8873863"
        self.OKPD2_IDS_CODES = "A,B,C"
        self.HEADLESS = True
        self.SLOWMO_MS = 0
        self.REQ_HEADERS = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122 Safari/537.36",
            "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
        }
        self._cancel_flag = False

    def cancel_search(self):
        self._cancel_flag = True
        logger.info("Search cancellation requested.")

    def _publish_date_from_str(self, days_back: int) -> str:
        dt = datetime.now() - timedelta(days=days_back)
        return dt.strftime("%d.%m.%Y")

    def build_search_url(self, keyword: str, page_number: int, fz44: bool, fz223: bool, only_application_stage: bool, publish_days_back: int) -> str:
        params = {
            "searchString": keyword,
            "morphology": "on",
            "search-filter": "Дате размещения",
            "pageNumber": str(page_number),
            "sortDirection": "false",
            "recordsPerPage": f"_{self.RECORDS_PER_PAGE}",
            "showLotsInfoHidden": "false",
            "sortBy": "PUBLISH_DATE",
            "publishDateFrom": self._publish_date_from_str(publish_days_back),
            "currencyIdGeneral": "-1",
        }

        if only_application_stage:
            params["af"] = "on"
        if fz44:
            params["fz44"] = "on"
        if fz223:
            params["fz223"] = "on"

        if self.OKPD2_IDS_WITH_NESTED:
            params["okpd2IdsWithNested"] = "on"
        if self.OKPD2_IDS:
            params["okpd2Ids"] = self.OKPD2_IDS
        if self.OKPD2_IDS_CODES:
            params["okpd2IdsCodes"] = self.OKPD2_IDS_CODES

        return SEARCH_URL + "?" + urlencode(params)

    def _normalize_text(self, text: str) -> str:
        return re.sub(r"\s+", " ", (text or "").strip())

    def _extract_field_by_label(self, block, labels: List[str]) -> str:
        if block is None:
            return ""

        text = block.get_text("\n", strip=True)
        text = re.sub(r"\n+", "\n", text)

        stop_labels = [
            "Объект закупки", "Начальная цена", "Начальная (максимальная) цена контракта",
            "Начальная (максимальная) цена договора", "Начальная сумма цен единиц товара, работы, услуги",
            "Окончание подачи заявок", "Дата окончания срока подачи заявок", "Цена", "Заказчик",
            "Организация, осуществляющая размещение", "Дата размещения", "Размещено", "Обновлено",
            "Способ определения поставщика", "Регион", "Валюта", "Преимущества, требования к участникам",
            "Информация о лоте", "Этап закупки",
        ]

        for label in labels:
            other_labels = [x for x in stop_labels if x != label]
            stop_pattern = "|".join(re.escape(x) for x in other_labels)
            pattern = rf"{re.escape(label)}\s*(.*?)(?=\n(?:{stop_pattern})\b|$)"
            m = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
            if m:
                value = self._normalize_text(m.group(1))
                if value:
                    return value
        return ""

    def _extract_initial_price(self, block) -> str:
        if block is None:
            return ""

        value = self._extract_field_by_label(
            block,
            [
                "Начальная цена", "Начальная (максимальная) цена контракта",
                "Начальная (максимальная) цена договора", "Начальная сумма цен единиц товара, работы, услуги",
            ],
        )
        if value:
            m = RUB_PRICE_RE.search(value)
            if m:
                return self._normalize_text(m.group(1))
            return value

        text = block.get_text(" ", strip=True)
        text = self._normalize_text(text)
        m = RUB_PRICE_RE.search(text)
        if m:
            return self._normalize_text(m.group(1))
        return ""

    def _extract_application_deadline(self, block) -> str:
        if block is None:
            return ""

        value = self._extract_field_by_label(
            block,
            ["Окончание подачи заявок", "Дата окончания срока подачи заявок"],
        )
        if value:
            m = DEADLINE_RE.search(value)
            if m:
                return self._normalize_text(m.group(0))
            return value

        text = block.get_text(" ", strip=True)
        text = self._normalize_text(text)
        m = DEADLINE_RE.search(text)
        if m:
            return self._normalize_text(m.group(0))
        return ""

    def _extract_notice_key_from_href(self, href: str):
        full_href = urljoin(BASE, (href or "").strip())
        if "/epz/order/notice/" not in full_href:
            return None

        m = REGNUMBER_RE.search(full_href)
        if m:
            return ("regNumber", m.group(1))

        m = NOTICEINFOID_RE.search(full_href)
        if m:
            return ("noticeInfoId", m.group(1))

        return None

    def _extract_notice_type_from_href(self, href: str) -> str:
        full_href = urljoin(BASE, (href or "").strip())
        m = re.search(r"/epz/order/notice/([^/]+)/", urlparse(full_href).path)
        return m.group(1) if m else ""

    def _is_technical_notice_href(self, href: str) -> bool:
        path = urlparse(urljoin(BASE, (href or "").strip())).path.lower()
        return any(part.lower() in path for part in TECHNICAL_NOTICE_PARTS)

    def _href_rank(self, href: str) -> int:
        path = urlparse(urljoin(BASE, (href or "").strip())).path.lower()

        # Самый предпочтительный вариант — обычный common-info, не printForm
        if "/common-info" in path and "/printform/" not in path:
            return 0

        if "/documents" in path and "/printform/" not in path:
            return 1

        if "/contract-info" in path and "/printform/" not in path:
            return 2

        # plan.html - отбрасываем как канонический
        if "/plan.html" in path:
            return 100

        # printForm/common-info оставляем только как запасной fallback
        if "/printform/" in path and "/common-info" in path:
            return 50

        # Остальные технические ссылки — самый низкий приоритет
        if self._is_technical_notice_href(href):
            return 1000

        return 10

    def _choose_better_href(self, current_href: str, candidate_href: str) -> str:
        if not current_href:
            return candidate_href or ""
        if not candidate_href:
            return current_href or ""

        return candidate_href if self._href_rank(candidate_href) < self._href_rank(current_href) else current_href

    def _find_result_card(self, anchor):
        # Пытаемся найти контейнер карточки результата
        for parent in anchor.parents:
            try:
                if not getattr(parent, "name", None):
                    continue

                classes = " ".join(parent.get("class") or []).lower()

                # Наиболее точные классы карточки ЕИС
                if "search-registry-entry-block" in classes or "registry-entry__form" in classes:
                    return parent
            except Exception:
                continue

        # Если не нашли по точным классам, ищем по тексту и общим классам
        for parent in anchor.parents:
            try:
                if not getattr(parent, "name", None):
                    continue

                parent_text = self._normalize_text(parent.get_text(" ", strip=True))
                if not parent_text:
                    continue

                classes = " ".join(parent.get("class") or []).lower()

                if (
                    "registry-entry" in classes
                    or "search-registry-entry" in classes
                    or "card" in classes
                    or "row" in classes
                ):
                    return parent

                if (
                    "Объект закупки" in parent_text
                    or "Начальная цена" in parent_text
                    or "Начальная (максимальная) цена" in parent_text
                    or "Окончание подачи заявок" in parent_text
                    or "Дата окончания срока подачи заявок" in parent_text
                ):
                    return parent
            except Exception:
                continue

        return anchor.parent

    def _collect_notice_hrefs_from_page(self, page) -> List[str]:
        try:
            raw_hrefs = page.locator(NOTICE_LINK_SELECTOR).evaluate_all(
                "els => els.map(el => el.getAttribute('href') || '').filter(Boolean)"
            )
        except Exception:
            return []

        cleaned: List[str] = []
        seen: Set[str] = set()

        for href in raw_hrefs:
            full_href = urljoin(BASE, (href or "").strip())
            if not full_href:
                continue

            key = self._extract_notice_key_from_href(full_href)
            if not key:
                continue

            if "/plan.html" in full_href.lower():
                continue

            # Служебные printForm/listModal не используем как самостоятельный сигнал наличия выдачи,
            # кроме случая, когда больше вообще ничего нет
            if self._is_technical_notice_href(full_href) and "/common-info" not in full_href.lower():
                continue

            if full_href not in seen:
                seen.add(full_href)
                cleaned.append(full_href)

        cleaned.sort(key=self._href_rank)
        return cleaned

    def _get_visible_notice_keys(self, page) -> List[str]:
        keys: List[str] = []
        seen: Set[str] = set()

        for href in self._collect_notice_hrefs_from_page(page):
            key = self._extract_notice_key_from_href(href)
            if not key:
                continue

            key_name, key_value = key
            marker = f"{key_name}:{key_value}"
            if marker not in seen:
                seen.add(marker)
                keys.append(marker)

        return keys

    def _extract_notices_from_results(self, html: str, keyword: str, search_url: str, page_number: int, slog: 'SearchLogger') -> List[Notice]:
        soup = BeautifulSoup(html, "html.parser")
        results: List[Notice] = []
        
        cards_map = {}
        links = soup.select(NOTICE_LINK_SELECTOR)
        for a in links:
            card = self._find_result_card(a)
            if card:
                cid = id(card)
                if cid not in cards_map:
                    cards_map[cid] = {'card': card, 'links': []}
                cards_map[cid]['links'].append(a)
            else:
                cid = id(a)
                cards_map[cid] = {'card': a, 'links': [a]}

        card_index = 0
        for cid, data in cards_map.items():
            card_index += 1
            card = data['card']
            card_links = data['links']
            
            valid_links = []
            for a in card_links:
                href = (a.get("href") or "").strip()
                if not href: continue
                full_href = urljoin(BASE, href)
                key = self._extract_notice_key_from_href(full_href)
                if not key: continue
                valid_links.append((full_href, key, a))
                
            if not valid_links:
                slog.info("SEARCH_CARD", f"page={page_number} | card_index={card_index} | status=skipped | reason=no_valid_links")
                continue
                
            valid_links.sort(key=lambda x: self._href_rank(x[0]))
            
            best_link_tuple = None
            for vl in valid_links:
                rank = self._href_rank(vl[0])
                if rank < 100:
                    best_link_tuple = vl
                    break
                    
            if not best_link_tuple:
                slog.info("SEARCH_CARD", f"page={page_number} | card_index={card_index} | status=skipped | reason=only_plan_or_technical_links")
                continue
                
            best_href, best_key, best_a = best_link_tuple
            key_name, key_value = best_key
            
            # Для 223-ФЗ добавляем префикс, чтобы не путать с ФЗ-44
            if key_name == "noticeInfoId":
                reg_id = f"223-{key_value}"
            else:
                reg_id = key_value
                
            ntype = self._extract_notice_type_from_href(best_href)
            
            title = self._normalize_text(best_a.get_text(" ", strip=True))
            if not title:
                title = self._normalize_text(best_a.get("title") or "")
                
            for vl in valid_links:
                link_text = self._normalize_text(vl[2].get_text(" ", strip=True))
                if link_text and len(link_text) > len(title):
                    title = link_text
                    
            object_info = self._extract_field_by_label(card, ["Объект закупки", "Наименование объекта закупки"])
            initial_price = self._extract_initial_price(card)
            application_deadline = self._extract_application_deadline(card)
            
            docs_url = ""
            for vl in valid_links:
                if "/documents" in vl[0].lower() and "/printform/" not in vl[0].lower():
                    docs_url = vl[0]
                    break
            if not docs_url and "/documents" in best_href.lower():
                docs_url = best_href
                
            plan_dropped = any('/plan.html' in vl[0].lower() for vl in valid_links)
            
            slog.info("SEARCH_CARD", f"page={page_number} | card_index={card_index} | key={key_name}:{key_value} | title='{title[:50]}...' | object_info={'found' if object_info else 'not_found'} | price='{initial_price}' | href={best_href} | plan_dropped={plan_dropped} | status=added")
            
            n = Notice(
                reg=reg_id,
                ntype=ntype,
                keyword=keyword,
                search_url=search_url,
                title=title,
                href=best_href,
                object_info=object_info,
                initial_price=initial_price,
                application_deadline=application_deadline,
                seen=is_seen(reg_id),
                docs_url=docs_url,
            )
            n.page_number = page_number
            results.append(n)
            
        slog.info("SEARCH_PAGE_SUMMARY", f"page={page_number} | cards_found={len(cards_map)} | results_added={len(results)}")
        return results

    def _has_notice_results(self, page) -> bool:
        try:
            return len(self._get_visible_notice_keys(page)) > 0
        except Exception:
            return False

    def _has_no_results_banner(self, page) -> bool:
        try:
            html = page.content().lower()
        except Exception:
            return False
        return any(p in html for p in NO_RESULTS_PATTERNS)

    def _wait_results_or_empty(self, page, timeout_ms: int = 15000, stable_rounds: int = 3) -> bool:
        deadline = time.time() + timeout_ms / 1000.0
        last_keys: List[str] = []
        stable_hits = 0

        while time.time() < deadline:
            keys = self._get_visible_notice_keys(page)

            if keys:
                if keys == last_keys:
                    stable_hits += 1
                else:
                    last_keys = keys
                    stable_hits = 1

                if stable_hits >= stable_rounds:
                    return True
            else:
                if self._has_no_results_banner(page):
                    return False

            page.wait_for_timeout(500)

        return bool(last_keys)

    def _get_first_notice_href(self, page) -> str:
        hrefs = self._collect_notice_hrefs_from_page(page)
        if not hrefs:
            return ""
        hrefs.sort(key=self._href_rank)
        return hrefs[0]

    def _ensure_fresh_search_results(self, page) -> bool:
        initial_has_results = self._wait_results_or_empty(page, timeout_ms=15000)
        if not initial_has_results:
            logger.info("На странице результатов карточек нет")
            return False

        page.wait_for_timeout(300)

        before_keys = self._get_visible_notice_keys(page)
        before_href = self._get_first_notice_href(page)

        try:
            btn = page.get_by_role("button", name=re.compile(r"применить", re.I))
            if btn.count() > 0:
                btn.first.click()
                logger.info("Нажата кнопка 'Применить' через get_by_role")
            else:
                btn2 = page.locator("input[type='submit'][value*='Применить'], button:has-text('Применить')")
                if btn2.count() > 0:
                    btn2.first.click()
                    logger.info("Нажата кнопка 'Применить' через locator")
                else:
                    logger.info("Кнопка 'Применить' не найдена, используем текущую выдачу")
                    return initial_has_results
        except Exception as e:
            logger.info(f"Не удалось нажать 'Применить': {e}")
            return initial_has_results

        page.wait_for_timeout(700)

        deadline = time.time() + 15.0
        last_keys: List[str] = []
        stable_hits = 0

        while time.time() < deadline:
            keys = self._get_visible_notice_keys(page)

            if not keys:
                if self._has_no_results_banner(page):
                    logger.info("После 'Применить': has_results=False (banner)")
                    return False
                page.wait_for_timeout(500)
                continue

            first_href = self._get_first_notice_href(page)
            changed = (keys != before_keys) or (first_href != before_href)

            if keys == last_keys:
                stable_hits += 1
            else:
                last_keys = keys
                stable_hits = 1

            if changed and stable_hits >= 3:
                logger.info(
                    f"После 'Применить': has_results=True, unique_notice_keys={len(keys)}, first_href={first_href}"
                )
                return True

            # Если выдача не изменилась, но уже стабильно существует, тоже принимаем её как валидную
            if not changed and stable_hits >= 4:
                logger.info(
                    f"После 'Применить': has_results=True, unique_notice_keys={len(keys)}, first_href={first_href}"
                )
                return True

            page.wait_for_timeout(500)

        has_results = len(self._get_visible_notice_keys(page)) > 0
        logger.info(f"После 'Применить': has_results={has_results}")
        return has_results

    def goto_with_human_delays(self, page, url: str, wait: str = "domcontentloaded", timeout: int = 60000, op_counter: Optional[int] = None, retries: int = 2, slog: 'SearchLogger' = None, page_number: int = 0):
        last_exc = None
        for attempt in range(1, retries + 1):
            try:
                human_sleep(1.2, 3.2)
                if slog:
                    slog.info("SEARCH_PAGE", f"GOTO -> {url} (attempt {attempt}/{retries})")
                else:
                    logger.info(f"GOTO -> {url} (attempt {attempt}/{retries})")
                page.goto(url, wait_until=wait, timeout=timeout)
                human_sleep(0.8, 2.2)
                if op_counter is not None:
                    long_pause_every(25, op_counter)
                if slog:
                    slog.info("SEARCH_PAGE", f"page={page_number} | URL loaded successfully")
                return
            except PwTimeoutError as e:
                last_exc = e
                if slog:
                    slog.warning("SEARCH_PAGE_ERROR", f"page={page_number} | attempt={attempt} | url={url} | type=TimeoutError | outcome={'retry' if attempt < retries else 'abort'}")
                else:
                    logger.error(f"GOTO timeout on {url} (attempt {attempt}/{retries})")
                if attempt < retries:
                    human_sleep(3.0, 7.0)
                    continue
                raise RuntimeError(
                    f"Превышено время ожидания при подключении к zakupki.gov.ru. "
                    "Сайт может быть недоступен или блокировать ваш IP. Попробуйте VPN с российским IP."
                )
            except Exception as e:
                last_exc = e
                err_msg = str(e)
                if slog:
                    slog.warning("SEARCH_PAGE_ERROR", f"page={page_number} | attempt={attempt} | url={url} | type=Exception | outcome={'retry' if attempt < retries else 'abort'} | error={err_msg}")
                else:
                    logger.error(f"GOTO error on {url} (attempt {attempt}/{retries}): {err_msg}")
                if attempt < retries:
                    human_sleep(3.0, 7.0)
                    continue
                
                if "ERR_CONNECTION_TIMED_OUT" in err_msg or "ERR_CONNECTION_RESET" in err_msg:
                    raise RuntimeError(
                        f"Ошибка подключения к zakupki.gov.ru ({err_msg}). "
                        "Сайт может блокировать доступ с вашего IP. Попробуйте VPN с российским IP или настройте прокси в .env."
                    )
                raise
        if last_exc:
            raise last_exc

    def search_tenders(self, query: str, fz44: bool = True, fz223: bool = True, only_application_stage: bool = True, publish_days_back: int = 30, search_id: str = None):
        import uuid
        if not search_id:
            search_id = uuid.uuid4().hex[:8]
        slog = SearchLogger(search_id)
        start_time = time.time()

        slog.info("SEARCH_START", f"query='{query}', fz44={fz44}, fz223={fz223}, only_application_stage={only_application_stage}, publish_days_back={publish_days_back}")

        # Fix for Windows NotImplementedError with Playwright inside the method
        if sys.platform == 'win32':
            try:
                asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
            except Exception:
                pass

        if USE_PROXY:
            try:
                client = get_http_client() # This ensures tunnel is up
                # client is RfProxyHttpClient
                logger.info("Warming up proxy session...")
                client.warmup()
            except Exception as e:
                logger.error(f"Proxy warmup failed: {e}")
                raise e

        self._cancel_flag = False
        slog.info("SEARCH_START", f"Searching EIS via Playwright for: {query}")
        collected: List[Notice] = []
        op_counter = 0

        try:
            with sync_playwright() as p:
                try:
                    logger.info("Launching Chromium...")
                    browser = p.chromium.launch(
                        headless=self.HEADLESS,
                        slow_mo=self.SLOWMO_MS,
                        proxy={"server": f"socks5://127.0.0.1:{LOCAL_SOCKS_PORT}"} if USE_PROXY else None
                    )
                except Exception as browser_err:
                    err_msg = str(browser_err)
                    if "playwright install" in err_msg.lower() or "executable doesn't exist" in err_msg.lower():
                        raise RuntimeError("Браузер Playwright не найден. Пожалуйста, выполните команду в терминале: .venv\\Scripts\\python.exe -m playwright install chromium")
                    raise browser_err
                
                try:
                    if os.path.exists(STATE_PATH):
                        context = browser.new_context(
                            locale="ru-RU",
                            user_agent=self.REQ_HEADERS["User-Agent"],
                            storage_state=STATE_PATH,
                            viewport={"width": 1920, "height": 1080}
                        )
                        logger.info(f"[state] loaded: {STATE_PATH}")
                    else:
                        context = browser.new_context(
                            locale="ru-RU",
                            user_agent=self.REQ_HEADERS["User-Agent"],
                            viewport={"width": 1920, "height": 1080}
                        )
                        logger.info("[state] fresh context (no saved state yet)")

                    page = context.new_page()

                    keywords = [k.strip() for k in query.split(',')] if ',' in query else [query]

                    for kw in keywords:
                        if self._cancel_flag:
                            slog.info("SEARCH_START", "Search cancelled by user.")
                            break
                        for pn in range(1, self.MAX_PAGES + 1):
                            if self._cancel_flag:
                                slog.info("SEARCH_START", "Search cancelled by user.")
                                break
                            url = self.build_search_url(kw, pn, fz44, fz223, only_application_stage, publish_days_back)
                            slog.info("SEARCH_START", f"Generated URL for kw='{kw}', page={pn}: {url} | OKPD2: {self.OKPD2_IDS} | OKPD2_CODES: {self.OKPD2_IDS_CODES}")

                            try:
                                self.goto_with_human_delays(page, url, op_counter=op_counter, retries=2, slog=slog, page_number=pn)
                                op_counter += 1

                                has_results = self._ensure_fresh_search_results(page)
                                if not has_results:
                                    slog.info("SEARCH_PAGE", f"no results on page {pn} for kw='{kw}' -> stop pages for this keyword")
                                    break

                            except PwTimeoutError as e:
                                slog.error("SEARCH_PAGE", f"timeout kw='{kw}' page={pn}: {e}")
                                break
                            except Exception as e:
                                slog.error("SEARCH_PAGE", f"error kw='{kw}' page={pn}: {e}")
                                break

                            items = self._extract_notices_from_results(page.content(), kw, url, pn, slog)
                            slog.info("SEARCH_PAGE", f"found notices after per-page dedup: {len(items)}")
                            
                            if not items:
                                break

                            collected.extend(items)
                            slog.info("SEARCH_PAGE_SUMMARY", f"cumulative_collected_total={len(collected)}")

                    ensure_dir(os.path.dirname(STATE_PATH))
                    context.storage_state(path=STATE_PATH)
                    logger.info(f"[state] saved: {STATE_PATH}")

                except Exception as nav_err:
                    slog.error("SEARCH_PAGE", f"Navigation/Page Error: {nav_err}")
                finally:
                    browser.close()
                    slog.info("SEARCH_PAGE", "Browser closed.")

                # Filter and merge
                slog.info("SEARCH_DEDUP", f"Starting deduplication. Raw items collected: {len(collected)}")
                slog.info("SEARCH_DEDUP", "deduplication_key = reg")

                from collections import defaultdict
                grouped = defaultdict(list)
                for n in collected:
                    grouped[n.reg].append(n)

                merged: Dict[str, Notice] = {}
                duplicates_removed = 0
                duplicate_regs = []

                for reg, items in grouped.items():
                    if len(items) == 1:
                        n = items[0]
                        n.seen = is_seen(n.reg)
                        merged[reg] = n
                    else:
                        duplicates_removed += (len(items) - 1)
                        duplicate_regs.append(reg)
                        slog.info("SEARCH_DEDUP_ITEM", f"reg: {reg} | Found {len(items)} records")
                        for idx, it in enumerate(items):
                            slog.info("SEARCH_DEDUP_ITEM", f"  Record {idx+1}: title='{it.title[:30]}...', price='{it.initial_price}', href='{it.href}', page='{getattr(it, 'page_number', 'unknown')}'")

                        # Apply merge logic
                        best = items[0]
                        for current in items[1:]:
                            if len(current.title) > len(best.title):
                                slog.info("SEARCH_DEDUP_ITEM", f"  -> Updating best title: '{best.title[:20]}...' -> '{current.title[:20]}...'")
                                best.title = current.title
                            if current.object_info and len(current.object_info) > len(best.object_info):
                                best.object_info = current.object_info
                            if current.initial_price and len(current.initial_price) > len(best.initial_price):
                                best.initial_price = current.initial_price
                            if current.application_deadline and len(current.application_deadline) > len(best.application_deadline):
                                best.application_deadline = current.application_deadline
                            if not best.href:
                                best.href = current.href

                        best.seen = is_seen(best.reg)
                        merged[reg] = best
                        slog.info("SEARCH_DEDUP_ITEM", f"  -> Kept merged record for reg {reg}. Reason: merged longest fields.")

                slog.info("SEARCH_DEDUP", f"Deduplication finished. Before merge: {len(collected)}, After merge: {len(merged)}, Duplicates removed: {duplicates_removed}")
                if duplicate_regs:
                    slog.info("SEARCH_DEDUP", f"Duplicate regs: {duplicate_regs}")
                
                final_results = list(merged.values())
                slog.info("SEARCH_RESULT", f"final_result_count: {len(final_results)}")
                slog.debug("SEARCH_RESULT", f"Final regs: {[n.reg for n in final_results]}")
                slog.debug("SEARCH_RESULT", f"Final titles: {[n.title for n in final_results]}")
                slog.debug("SEARCH_RESULT", f"Final URLs: {[n.href for n in final_results]}")

                duration = time.time() - start_time
                slog.info("SEARCH_SUMMARY", f"query='{query}' | raw_found_total={len(collected)} | unique_after_dedup={len(final_results)} | duplicates_removed={duplicates_removed} | returned_to_api={len(final_results)} | duration_seconds={duration:.2f}")

                return final_results

        except Exception as global_err:
            slog.error("SEARCH_START", f"Playwright Global Error: {global_err}")
            raise global_err
