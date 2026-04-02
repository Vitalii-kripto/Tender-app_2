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
NOTICE_HREF_RE = re.compile(r"/epz/order/notice/([^/]+)/view/[^?]+\.html\?[^#]*regNumber=(\d+)")
NOTICE_LINK_SELECTOR = "a[href*='/epz/order/notice/']"

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

    def _extract_notices_from_results(self, html: str, keyword: str, search_url: str) -> List[Notice]:
        soup = BeautifulSoup(html, "html.parser")
        found_by_reg: Dict[str, Notice] = {}

        for a in soup.select(NOTICE_LINK_SELECTOR):
            href = (a.get("href") or "").strip()
            match = NOTICE_HREF_RE.search(href)
            if not match:
                continue

            ntype = match.group(1)
            reg = match.group(2)
            full_href = urljoin(BASE, href)

            title = self._normalize_text(a.get_text(" ", strip=True))
            if not title:
                title = self._normalize_text(a.get("title") or "")

            card = None
            for parent in a.parents:
                try:
                    parent_text = parent.get_text(" ", strip=True)
                except Exception:
                    continue
                if (
                    "Объект закупки" in parent_text
                    or "Начальная цена" in parent_text
                    or "Начальная (максимальная) цена" in parent_text
                    or "Окончание подачи заявок" in parent_text
                ):
                    card = parent
                    break

            object_info = self._extract_field_by_label(card, ["Объект закупки"]) if card else ""
            initial_price = self._extract_initial_price(card) if card else ""
            application_deadline = self._extract_application_deadline(card) if card else ""

            if reg not in found_by_reg:
                found_by_reg[reg] = Notice(
                    reg=reg,
                    ntype=ntype,
                    keyword=keyword,
                    search_url=search_url,
                    title=title,
                    href=full_href,
                    object_info=object_info,
                    initial_price=initial_price,
                    application_deadline=application_deadline,
                )
            else:
                current = found_by_reg[reg]
                if len(title) > len(current.title):
                    current.title = title
                if object_info and len(object_info) > len(current.object_info):
                    current.object_info = object_info
                if initial_price and len(initial_price) > len(current.initial_price):
                    current.initial_price = initial_price
                if application_deadline and len(application_deadline) > len(current.application_deadline):
                    current.application_deadline = application_deadline
                if not current.href:
                    current.href = full_href

        return list(found_by_reg.values())

    def _has_notice_results(self, page) -> bool:
        try:
            return page.locator(NOTICE_LINK_SELECTOR).count() > 0
        except Exception:
            return False

    def _has_no_results_banner(self, page) -> bool:
        try:
            html = page.content().lower()
        except Exception:
            return False
        return any(p in html for p in NO_RESULTS_PATTERNS)

    def _wait_results_or_empty(self, page, timeout_ms: int = 15000) -> bool:
        deadline = time.time() + timeout_ms / 1000.0
        while time.time() < deadline:
            if self._has_notice_results(page):
                return True
            if self._has_no_results_banner(page):
                return False
            page.wait_for_timeout(400)
        return False

    def _get_first_notice_href(self, page) -> str:
        try:
            return page.eval_on_selector(NOTICE_LINK_SELECTOR, "el => el.getAttribute('href') || ''") or ""
        except Exception:
            return ""

    def _ensure_fresh_search_results(self, page) -> bool:
        initial_has_results = self._wait_results_or_empty(page, timeout_ms=15000)
        if not initial_has_results:
            logger.info("На странице результатов карточек нет")
            return False

        page.wait_for_timeout(300)
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

        page.wait_for_timeout(500)

        try:
            page.wait_for_function(
                """(sel, before) => {
                    const a = document.querySelector(sel);
                    if (!a) return false;
                    const now = a.getAttribute('href') || '';
                    return now && now !== before;
                }""",
                arg=(NOTICE_LINK_SELECTOR, before_href),
                timeout=8000,
            )
        except Exception:
            pass

        page.wait_for_timeout(700)
        has_results = self._wait_results_or_empty(page, timeout_ms=12000)
        logger.info(f"После 'Применить': has_results={has_results}")
        return has_results

    def goto_with_human_delays(self, page, url: str, wait: str = "domcontentloaded", timeout: int = 60000, op_counter: Optional[int] = None, retries: int = 2):
        last_exc = None
        for attempt in range(1, retries + 1):
            try:
                human_sleep(1.2, 3.2)
                logger.info(f"GOTO -> {url} (attempt {attempt}/{retries})")
                page.goto(url, wait_until=wait, timeout=timeout)
                human_sleep(0.8, 2.2)
                if op_counter is not None:
                    long_pause_every(25, op_counter)
                return
            except PwTimeoutError as e:
                last_exc = e
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

    def search_tenders(self, query: str, fz44: bool = True, fz223: bool = True, only_application_stage: bool = True, publish_days_back: int = 30):
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
        logger.info(f"Searching EIS via Playwright for: {query}")
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
                            logger.info("Search cancelled by user.")
                            break
                        for pn in range(1, self.MAX_PAGES + 1):
                            if self._cancel_flag:
                                logger.info("Search cancelled by user.")
                                break
                            url = self.build_search_url(kw, pn, fz44, fz223, only_application_stage, publish_days_back)
                            logger.info(f"[SEARCH] kw='{kw}' page={pn}")
                            logger.info(f"[SEARCH] url: {url}")

                            try:
                                self.goto_with_human_delays(page, url, op_counter=op_counter, retries=2)
                                op_counter += 1

                                has_results = self._ensure_fresh_search_results(page)
                                if not has_results:
                                    logger.info(f"[SEARCH] no results on page {pn} for kw='{kw}' -> stop pages for this keyword")
                                    break

                            except PwTimeoutError as e:
                                logger.error(f"[SEARCH] timeout kw='{kw}' page={pn}: {e}")
                                break
                            except Exception as e:
                                logger.error(f"[SEARCH] error kw='{kw}' page={pn}: {e}")
                                break

                            items = self._extract_notices_from_results(page.content(), kw, url)
                            logger.info(f"[SEARCH] found notices: {len(items)}")
                            
                            if not items:
                                break

                            collected.extend(items)

                    ensure_dir(os.path.dirname(STATE_PATH))
                    context.storage_state(path=STATE_PATH)
                    logger.info(f"[state] saved: {STATE_PATH}")

                except Exception as nav_err:
                    logger.error(f"Navigation/Page Error: {nav_err}")
                finally:
                    browser.close()
                    logger.info("Browser closed.")

                # Filter and merge
                merged: Dict[str, Notice] = {}
                for n in collected:
                    n.seen = is_seen(n.reg)
                    if n.reg not in merged:
                        merged[n.reg] = n
                    else:
                        current = merged[n.reg]
                        if len(n.title) > len(current.title):
                            current.title = n.title
                        if n.object_info and len(n.object_info) > len(current.object_info):
                            current.object_info = n.object_info
                
                return list(merged.values())

        except Exception as global_err:
            logger.error(f"Playwright Global Error: {global_err}", exc_info=True)
            raise global_err
