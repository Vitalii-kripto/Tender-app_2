import os
import re
import sqlite3
import time
import random
import traceback
import asyncio
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Optional, Set, Dict
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

def ensure_dir(p: str):
    if p:
        os.makedirs(p, exist_ok=True)

# Use a specific logger for this service
logger = logging.getLogger("EIS_Service")
logger.setLevel(logging.INFO)

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
    block = soup.select_one("div.blockFilesTabDocs")
    if not block:
        return []

    items: List[tuple[str, str]] = []
    for a in block.select("a[href]"):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        if href.startswith("/"):
            href = urljoin(BASE, href)

        if "filestore" in href and "download" in href:
            title = (a.get("title") or "").strip()
            text = (a.get_text() or "").strip()
            suggested = safe_filename(title or text)
            items.append((href, suggested))

    out, seen = [], set()
    for u, t in items:
        if u not in seen:
            seen.add(u)
            out.append((u, t))
    return out

def get_http_client():
    global RF_CLIENT_PROXY, RF_CLIENT_DIRECT
    if USE_PROXY:
        if RF_CLIENT_PROXY is None:
            from backend.services.auto_ssh import RfProxyHttpClient
            RF_CLIENT_PROXY = RfProxyHttpClient(RF_CFG)
        return RF_CLIENT_PROXY
    else:
        if RF_CLIENT_DIRECT is None:
            RF_CLIENT_DIRECT = requests.Session()
        return RF_CLIENT_DIRECT

def download_file_with_real_name(file_url: str, reg_dir: str, suggested_title: str) -> str:
    client = get_http_client()
    # RfProxyHttpClient handles tunnel ensure and warmup internally in .get()
    r = client.get(file_url, timeout=120, stream=True)
    r.raise_for_status()

    cd = r.headers.get("Content-Disposition", "")
    ct = r.headers.get("Content-Type", "")

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

    with open(out_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=1024 * 128):
            if chunk:
                f.write(chunk)

    return out_path

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
    def process_tenders(self, notices: List[Notice]) -> List[Dict]:
        if sys.platform == 'win32':
            try:
                asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
            except Exception:
                pass

        if USE_PROXY:
            try:
                client = get_http_client()
                logger.info("Warming up proxy session...")
                client.warmup()
            except Exception as e:
                logger.error(f"Proxy warmup failed: {e}")
                raise e

        results = []

        try:
            with sync_playwright() as p:
                try:
                    logger.info("Launching Chromium for processing...")
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

                    for n in notices:
                        log(f"--- Processing {n.reg} ---")
                        if n.reg.startswith("223-"):
                            notice_id = n.reg.replace("223-", "")
                            d_url = f"{BASE}/epz/order/notice/notice223/documents.html?noticeInfoId={notice_id}"
                        else:
                            d_url = f"{BASE}/epz/order/notice/{n.ntype}/view/documents.html?regNumber={n.reg}"
                        
                        try:
                            self.goto_with_human_delays(page, d_url, wait="domcontentloaded", timeout=60000, retries=2)
                            html = page.content()
                        except PwTimeoutError as e:
                            log_exception(f"Timeout fetching docs page {d_url}", e)
                            log_skip(f"SKIP:documents_timeout for {n.reg}")
                            results.append({"reg": n.reg, "status": "skip", "reason": "documents_timeout", "docs_url": d_url, "files": []})
                            continue
                        except Exception as e:
                            log_exception(f"Failed to fetch docs page {d_url}", e)
                            results.append({"reg": n.reg, "status": "error", "reason": f"fetch_docs_page:{e}", "docs_url": d_url, "files": []})
                            continue

                        items = parse_docs_block(html)
                        if not items:
                            log(f"No files found on docs page for {n.reg}")
                            log_skip(f"SKIP:no_files_found for {n.reg}")
                            mark_seen(n.reg)
                            results.append({"reg": n.reg, "status": "skip", "reason": "no_files_found", "docs_url": d_url, "files": []})
                            continue

                        reg_dir = os.path.join(DOCUMENTS_ROOT, n.reg)
                        ensure_dir(reg_dir)

                        downloaded_files = []
                        for file_url, suggested_title in items:
                            try:
                                log(f"  Downloading {file_url}")
                                out_path = download_file_with_real_name(file_url, reg_dir, suggested_title)
                                log(f"  -> Saved to {out_path}")
                                downloaded_files.append(out_path)
                                time.sleep(random.uniform(0.5, 1.5))
                            except Exception as e:
                                log_exception(f"  Failed to download {file_url}", e)

                        if downloaded_files:
                            mark_seen(n.reg)
                            results.append({"reg": n.reg, "status": "selected", "docs_url": d_url, "files": downloaded_files})
                        else:
                            log_skip(f"SKIP:all_downloads_failed for {n.reg}")
                            results.append({"reg": n.reg, "status": "skip", "reason": "all_downloads_failed", "docs_url": d_url, "files": []})

                    ensure_dir(os.path.dirname(STATE_PATH))
                    context.storage_state(path=STATE_PATH)
                    logger.info(f"[state] saved: {STATE_PATH}")

                except Exception as nav_err:
                    logger.error(f"Navigation/Page Error: {nav_err}")
                finally:
                    browser.close()
                    logger.info("Browser closed.")

        except Exception as global_err:
            logger.error(f"Playwright Global Error: {global_err}", exc_info=True)
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

        if "/plan.html" in path and "/printform/" not in path:
            return 3

        # printForm/common-info оставляем только как запасной fallback
        if "/printform/" in path and "/common-info" in path:
            return 50

        # Остальные технические ссылки — самый низкий приоритет
        if self._is_technical_notice_href(href):
            return 1000

        return 100

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
        found_by_key: Dict[str, Notice] = {}
        
        raw_items_on_page = 0
        added_to_collected = 0
        skipped_on_page = 0

        links = soup.select(NOTICE_LINK_SELECTOR)
        for a in links:
            raw_items_on_page += 1
            href = (a.get("href") or "").strip()
            if not href:
                slog.info("SEARCH_ITEM", f"page={page_number} | item={raw_items_on_page} | status=skipped | reason=empty_href")
                skipped_on_page += 1
                continue

            key = self._extract_notice_key_from_href(href)
            if not key:
                slog.info("SEARCH_ITEM", f"page={page_number} | item={raw_items_on_page} | status=skipped | reason=parse_error (no reg match in href) | href={href}")
                skipped_on_page += 1
                continue

            key_name, key_value = key
            full_href = urljoin(BASE, href)
            ntype = self._extract_notice_type_from_href(full_href)

            card = self._find_result_card(a)

            title = self._normalize_text(a.get_text(" ", strip=True))
            if not title:
                title = self._normalize_text(a.get("title") or "")

            if card is not None:
                card_title = ""
                try:
                    preferred_links = []
                    for link in card.select("a[href]"):
                        link_href = (link.get("href") or "").strip()
                        if not link_href:
                            continue
                        if not self._extract_notice_key_from_href(link_href):
                            continue
                        preferred_links.append(link)

                    preferred_links.sort(
                        key=lambda link: self._href_rank(urljoin(BASE, (link.get("href") or "").strip()))
                    )

                    for link in preferred_links:
                        link_text = self._normalize_text(link.get_text(" ", strip=True))
                        if link_text and len(link_text) > len(card_title):
                            card_title = link_text
                except Exception:
                    card_title = ""

                if len(card_title) > len(title):
                    title = card_title

            object_info = self._extract_field_by_label(card, ["Объект закупки"]) if card else ""
            initial_price = self._extract_initial_price(card) if card else ""
            application_deadline = self._extract_application_deadline(card) if card else ""

            current = found_by_key.get(key_value)

            if current is None:
                slog.info("SEARCH_ITEM", f"page={page_number} | item={raw_items_on_page} | key={key_name}:{key_value} | title='{title[:50]}...' | price='{initial_price}' | href={full_href} | status=added")
                current = Notice(
                    reg=key_value,
                    ntype=ntype,
                    keyword=keyword,
                    search_url=search_url,
                    title=title,
                    href=full_href,
                    object_info=object_info,
                    initial_price=initial_price,
                    application_deadline=application_deadline,
                    seen=is_seen(key_value),
                    docs_url=full_href if "/documents" in full_href.lower() else "",
                )
                current.page_number = page_number
                found_by_key[key_value] = current
                added_to_collected += 1
            else:
                slog.info("SEARCH_ITEM", f"page={page_number} | item={raw_items_on_page} | key={key_name}:{key_value} | status=merged | reason=duplicate_on_same_page")
                skipped_on_page += 1
                if title and len(title) > len(current.title):
                    current.title = title

                if object_info and len(object_info) > len(current.object_info):
                    current.object_info = object_info

                if initial_price and len(initial_price) > len(current.initial_price):
                    current.initial_price = initial_price

                if application_deadline and len(application_deadline) > len(current.application_deadline):
                    current.application_deadline = application_deadline

                current.href = self._choose_better_href(current.href, full_href)

                if "/documents" in full_href.lower() and (
                    not current.docs_url or self._href_rank(full_href) < self._href_rank(current.docs_url)
                ):
                    current.docs_url = full_href

                if not current.ntype and ntype:
                    current.ntype = ntype

        slog.info("SEARCH_PAGE_SUMMARY", f"page={page_number} | raw_items_on_page={raw_items_on_page} | added_to_collected={added_to_collected} | skipped_on_page={skipped_on_page}")
        return list(found_by_key.values())

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
