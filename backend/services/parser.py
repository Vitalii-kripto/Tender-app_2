import asyncio
import logging
import re
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse, urljoin, urlunparse, parse_qsl, urlencode

import aiohttp
from bs4 import BeautifulSoup
from langchain_community.document_loaders.recursive_url_loader import RecursiveUrlLoader
from sqlalchemy.orm import Session

logger = logging.getLogger("GidroizolParser")
logger.setLevel(logging.INFO)

ASYNC_CONCURRENCY = 10
ASYNC_TIMEOUT = 30
ASYNC_RETRIES = 2


class GidroizolParser:
    BASE_URL = "https://gidroizol.ru"
    DOMAIN = "gidroizol.ru"

    MOSCOW_CITY_ID = "1"
    CITY_PARAM = "city"

    START_URLS = [
        "https://gidroizol.ru/9",
        "https://gidroizol.ru/18",
        "https://gidroizol.ru/149",  # ИЗОПЛАСТ
    ]

    DROP_QUERY_PREFIXES = ("utm_",)
    DROP_QUERY_KEYS = {"gclid", "fbclid", "yclid"}

    PRICE_RE = re.compile(r"(\d+[.,]\d+|\d+)\s*р\./", re.IGNORECASE)

    # Заголовки, при встрече которых парсинг описания останавливается
    STOP_DESC_HEADINGS = {
        "с этим товаром покупают",
        "аналоги",
        "отзывы",
        "сертификаты",
        "написать сообщение",
        "запросить сертификат",
        "характеристики", # часто характеристики идут после описания
    }

    def __init__(self):
        self.visited_urls: Set[str] = set()
        self.product_urls: Set[str] = set()
        self.product_data: List[Dict] = []

        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0 Safari/537.36"
            ),
            "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Cookie": f"{self.CITY_PARAM}={self.MOSCOW_CITY_ID}",
        }

    # ----------------------------
    # URL helpers (Москва-only)
    # ----------------------------
    def _is_asset(self, url: str) -> bool:
        return url.lower().endswith(
            (".jpg", ".jpeg", ".png", ".svg", ".ico", ".webp", ".css", ".js", ".pdf", ".zip")
        )

    def _is_same_domain(self, url: str) -> bool:
        netloc = (urlparse(url).netloc or "").replace("www.", "")
        return netloc == "" or netloc.endswith(self.DOMAIN)

    def _drop_tracking_qs(self, pairs: List[Tuple[str, str]]) -> List[Tuple[str, str]]:
        kept: List[Tuple[str, str]] = []
        for k, v in pairs:
            lk = k.lower()
            if lk in self.DROP_QUERY_KEYS:
                continue
            if any(lk.startswith(pref) for pref in self.DROP_QUERY_PREFIXES):
                continue
            kept.append((k, v))
        return kept

    def _ensure_moscow_city(self, url: str) -> str:
        url = (url or "").strip()
        if not url:
            return url

        p = urlparse(url)
        scheme = p.scheme or "https"
        netloc = (p.netloc or self.DOMAIN).replace("www.", "")
        path = (p.path or "").rstrip("/")

        qs = self._drop_tracking_qs(list(parse_qsl(p.query, keep_blank_values=True)))
        qs = [(k, v) for (k, v) in qs if k.lower() != self.CITY_PARAM]
        qs.append((self.CITY_PARAM, self.MOSCOW_CITY_ID))
        query = urlencode(qs, doseq=True)

        return urlunparse((scheme, netloc, path, "", query, ""))

    def _get_city_from_url(self, url: str) -> Optional[str]:
        p = urlparse(url)
        for k, v in parse_qsl(p.query, keep_blank_values=True):
            if k.lower() == self.CITY_PARAM:
                return v
        return None

    def normalize_url(self, url: str) -> str:
        return self._ensure_moscow_city(url)

    def _is_allowed_url(self, url: str) -> bool:
        if not url:
            return False
        if not self._is_same_domain(url):
            return False
        if self._is_asset(url):
            return False

        city = self._get_city_from_url(url)
        if city is not None and city != self.MOSCOW_CITY_ID:
            return False

        return True

    # ----------------------------
    # Listing vs product detection
    # ----------------------------
    def _count_price_occurrences(self, soup: BeautifulSoup) -> int:
        txt = soup.get_text(" ", strip=True)
        return len(self.PRICE_RE.findall(txt))

    def _count_add_to_cart_buttons(self, soup: BeautifulSoup) -> int:
        count = 0
        for el in soup.find_all(["a", "button"], string=True):
            t = el.get_text(" ", strip=True).lower()
            if "в корзину" in t:
                count += 1
        return count

    def is_listing_page(self, soup: BeautifulSoup) -> bool:
        prices = self._count_price_occurrences(soup)
        carts = self._count_add_to_cart_buttons(soup)

        if carts >= 3:
            return True
        if prices >= 8:
            return True

        heading_links = 0
        for h in soup.select("h2 a[href], h3 a[href]"):
            href = h.get("href", "")
            if href and href.startswith("/"):
                heading_links += 1
        if heading_links >= 6:
            return True

        return False

    # ----------------------------
    # Parsing Logic
    # ----------------------------
    def _clean_price(self, s: str) -> float:
        if not s:
            return 0.0
        s = s.replace("\xa0", " ").replace("&nbsp;", " ")
        s = re.sub(r"[^\d.,]", "", s.replace(" ", ""))
        s = s.replace(",", ".")
        try:
            return float(s)
        except Exception:
            return 0.0

    def parse_price(self, soup: BeautifulSoup) -> float:
        text = soup.get_text(" ", strip=True)

        m = re.search(r"(\d+[.,]\d+|\d+)\s*р\./[^\s]*\s*РОЗН", text, re.IGNORECASE)
        if m:
            return self._clean_price(m.group(1))

        m = re.search(r"(\d+[.,]\d+|\d+)\s*р\./", text, re.IGNORECASE)
        if m:
            return self._clean_price(m.group(1))

        for sel in [".price_val", ".price", ".product-price", ".detail-price", "[itemprop='price']"]:
            el = soup.select_one(sel)
            if el:
                val = self._clean_price(el.get_text(" ", strip=True))
                if val > 0:
                    return val

        return 0.0

    def parse_specs(self, soup: BeautifulSoup) -> Dict[str, str]:
        specs: Dict[str, str] = {}
        row_selectors = [
            "div.table_dop-info .table-row",
            "table.specs tr",
            "table tr",
            ".product-features li",
        ]

        rows = []
        for rs in row_selectors:
            rows = soup.select(rs)
            if rows:
                break

        for row in rows:
            tds = row.select("td")
            if len(tds) >= 2:
                k = tds[0].get_text(" ", strip=True).replace(":", "")
                v = tds[1].get_text(" ", strip=True)
                if k and v:
                    specs[k] = v
                continue

            cells = row.select("div.table-cell")
            if len(cells) >= 2:
                k = cells[0].get_text(" ", strip=True).replace(":", "")
                v = cells[1].get_text(" ", strip=True)
                if k and v:
                    specs[k] = v
                continue

            txt = row.get_text(" ", strip=True)
            if ":" in txt:
                k, v = txt.split(":", 1)
                k = k.strip().replace(":", "")
                v = v.strip()
                if k and v:
                    specs[k] = v

        return specs

    def parse_description(self, soup: BeautifulSoup) -> str:
        """
        Извлекает описание товара, комбинируя несколько стратегий.
        """
        description_text = ""

        # Стратегия 1: Поиск по itemprop="description"
        desc_el = soup.select_one('[itemprop="description"]')
        if desc_el:
            description_text = desc_el.get_text(" ", strip=True)

        # Стратегия 2: Поиск заголовка "Описание" и взятие текста после него
        if not description_text or len(description_text) < 50:
            header_node = None
            # Ищем заголовок, содержащий "Описание"
            for tag in soup.find_all(['h2', 'h3', 'h4', 'div'], string=re.compile(r"Описание", re.IGNORECASE)):
                if tag.name == 'div' and not tag.get('class'): # игнорируем пустые div
                    continue
                header_node = tag
                break
            
            if header_node:
                content_chunks = []
                for sibling in header_node.next_siblings:
                    if sibling.name in ['h2', 'h3', 'h4', 'section', 'footer']:
                        # Проверяем, не начался ли новый смысловой блок
                        stop_text = sibling.get_text(" ", strip=True).lower()
                        if any(stop in stop_text for stop in self.STOP_DESC_HEADINGS):
                            break
                        # Если просто заголовок, но не из стоп-листа, считаем его частью описания? 
                        # Обычно да, но лучше быть осторожным.
                    
                    if sibling.name in ['p', 'div', 'ul', 'ol', 'span']:
                        text = sibling.get_text(" ", strip=True)
                        if len(text) > 10: # Фильтр совсем коротких обрывков
                            content_chunks.append(text)
                
                if content_chunks:
                    description_text = "\n\n".join(content_chunks)

        # Стратегия 3: Стандартные классы
        if not description_text or len(description_text) < 50:
            for sel in [".product-description", ".detail-text", ".desc", "#tab-description"]:
                el = soup.select_one(sel)
                if el:
                    description_text = el.get_text(" ", strip=True)
                    break

        # Очистка
        if description_text:
            # Убираем лишние пробелы
            description_text = re.sub(r'\s+', ' ', description_text).strip()
            # Убираем заголовки, если они попали в текст
            description_text = description_text.replace("Описание товара", "").replace("Описание", "")
            return description_text[:3000] # Лимит символов для БД

        return ""

    # ----------------------------
    # Product page detection
    # ----------------------------
    def is_product_page(self, soup: BeautifulSoup, url: str = "") -> bool:
        if self.is_listing_page(soup):
            return False

        if not soup.select_one("h1"):
            return False

        price = self.parse_price(soup)
        if price <= 0:
            return False

        text = soup.get_text(" ", strip=True).lower()
        specs = self.parse_specs(soup)
        carts = self._count_add_to_cart_buttons(soup)

        if "все характеристики" in text or len(specs) >= 3: # Снизил порог характеристик
            return True

        if carts <= 2 and ("розн" in text or "опт" in text or "спец" in text or "в корзину" in text or "заказать" in text):
            return True

        return False

    # ----------------------------
    # Parsing Entry Point
    # ----------------------------
    def parse_product_page(self, url: str, html: str) -> Optional[Dict]:
        try:
            if not self._is_allowed_url(url):
                return None

            soup = BeautifulSoup(html, "html.parser")

            if not self.is_product_page(soup, url):
                return None

            title = None
            for sel in ["section#tovar h1", "h1", "div.product-title h1", "div.product-title"]:
                el = soup.select_one(sel)
                if el and el.get_text(strip=True):
                    title = el.get_text(strip=True)
                    break
            if not title:
                return None

            category_path = self.extract_category_path(soup)
            price = self.parse_price(soup)
            specs = self.parse_specs(soup)
            description = self.parse_description(soup)

            product_url = self.normalize_url(url)

            return {
                "url": product_url,
                "title": title,
                "category": category_path,
                "price": price,
                "description": description,
                "specs": specs,
                "material_type": "Рулонный" if "рулон" in category_path.lower() else "Гидроизоляция",
                "type": "product",
                "city_id": self.MOSCOW_CITY_ID,
                "city_name": "Москва",
            }
        except Exception as e:
            logger.error(f"parse_product_page error {url}: {e}")
            return None

    def extract_category_path(self, soup: BeautifulSoup) -> str:
        breadcrumb_selectors = [
            'ul.col-12[itemtype*="BreadcrumbList"] a span[itemprop="name"]',
            'section#breadcrumbs ul li a',
            'div.breadcrumbs a',
            'nav.breadcrumb a',
            'ul.breadcrumb a',
            '.breadcrumb a',
        ]

        crumbs: List[str] = []
        for sel in breadcrumb_selectors:
            items = soup.select(sel)
            if items:
                crumbs = [x.get_text(strip=True) for x in items if x.get_text(strip=True)]
                break

        drop = {"главная", "home", "каталог", "все категории"}
        cats = [c for c in crumbs if c and c.strip().lower() not in drop]

        h1 = soup.select_one("h1")
        if h1:
            h1t = h1.get_text(strip=True)
            if cats and cats[-1] == h1t:
                cats = cats[:-1]

        return " / ".join(cats[-3:]) if cats else "Каталог"

    # ----------------------------
    # Crawler Logic
    # ----------------------------
    def _extract_links(self, soup: BeautifulSoup, base_url: str) -> Set[str]:
        out: Set[str] = set()
        for a in soup.select("a[href]"):
            href = a.get("href")
            if not href:
                continue
            href = href.strip()
            if href.startswith("#") or href.lower().startswith("javascript:"):
                continue

            full = urljoin(base_url, href)
            if not self._is_allowed_url(full):
                continue

            out.add(self.normalize_url(full))
        return out

    async def _fetch_html(self, session: aiohttp.ClientSession, url: str) -> Optional[str]:
        url = self.normalize_url(url)
        for attempt in range(ASYNC_RETRIES + 1):
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=ASYNC_TIMEOUT)) as resp:
                    if resp.status != 200:
                        return None
                    return await resp.text(errors="ignore")
            except Exception:
                if attempt >= ASYNC_RETRIES:
                    return None
                await asyncio.sleep(0.6)
        return None

    async def _process_urls_async(self, initial_urls: Set[str]):
        sem = asyncio.Semaphore(ASYNC_CONCURRENCY)
        async with aiohttp.ClientSession(headers=self.headers) as session:
            queue: Set[str] = set(self.normalize_url(u) for u in initial_urls if self._is_allowed_url(u))

            while queue:
                batch = list(queue)[: ASYNC_CONCURRENCY * 3]
                queue.difference_update(batch)

                async def handle(u: str):
                    async with sem:
                        nu = self.normalize_url(u)
                        if nu in self.visited_urls:
                            return
                        self.visited_urls.add(nu)

                        html = await self._fetch_html(session, nu)
                        if not html:
                            return

                        soup = BeautifulSoup(html, "html.parser")

                        data = self.parse_product_page(nu, html)
                        if data:
                            purl = data["url"]
                            if purl not in self.product_urls:
                                self.product_urls.add(purl)
                                self.product_data.append(data)
                                logger.info(f"✅ PRODUCT: {data['title']} | {data['price']}rub")
                            return

                        links = self._extract_links(soup, nu)
                        for l in links:
                            nl = self.normalize_url(l)
                            if nl not in self.visited_urls:
                                queue.add(nl)

                await asyncio.gather(*(handle(u) for u in batch))

    def extractor(self, html: str) -> str:
        return html

    async def crawl_pages(self, start_url: str, max_depth: int = 7):
        start_url = self.normalize_url(start_url)
        logger.info(f"Starting crawl (MSK) from: {start_url}")

        loader = RecursiveUrlLoader(
            url=start_url,
            max_depth=max_depth,
            extractor=self.extractor,
            prevent_outside=True,
            timeout=20,
            use_async=False,
            exclude_dirs=["contacts", "about", "news", "blog", "articles", "login", "auth"],
            headers=self.headers,
        )

        documents = await asyncio.to_thread(loader.load)
        logger.info(f"RecursiveUrlLoader finished. Loaded {len(documents)} documents from {start_url}")

        next_urls: Set[str] = set()

        for doc in documents:
            raw_url = doc.metadata.get("source", "")
            if not raw_url:
                continue

            url = self.normalize_url(raw_url)
            if not self._is_allowed_url(url):
                continue

            if url in self.visited_urls:
                continue
            self.visited_urls.add(url)

            data = self.parse_product_page(url, doc.page_content)
            if data:
                purl = data["url"]
                if purl not in self.product_urls:
                    self.product_urls.add(purl)
                    self.product_data.append(data)

            soup = BeautifulSoup(doc.page_content, "html.parser")
            for l in self._extract_links(soup, url):
                nl = self.normalize_url(l)
                if nl not in self.visited_urls:
                    next_urls.add(nl)

        await self._process_urls_async(next_urls)
        logger.info(f"Finished crawl (MSK) from {start_url}. Total products: {len(self.product_data)}")

    # ----------------------------
    # DB upsert
    # ----------------------------
    def _upsert_products_to_db(self, db: Session) -> int:
        try:
            from backend.models import ProductModel  # type: ignore
        except Exception:
            from ..models import ProductModel  # type: ignore

        created = 0
        for p in self.product_data:
            url = p.get("url")
            if not url:
                continue

            existing = db.query(ProductModel).filter(ProductModel.url == url).first()
            if existing:
                existing.title = p.get("title", existing.title)
                existing.category = p.get("category", getattr(existing, "category", "Каталог"))
                existing.material_type = p.get("material_type", getattr(existing, "material_type", ""))
                existing.price = p.get("price", getattr(existing, "price", 0))
                existing.specs = p.get("specs", getattr(existing, "specs", {}))
                
                # Явное обновление описания
                new_desc = p.get("description", "")
                if new_desc:
                    existing.description = new_desc
                    
            else:
                obj = ProductModel(
                    title=p.get("title", "Не указано"),
                    category=p.get("category", "Каталог"),
                    material_type=p.get("material_type", ""),
                    price=p.get("price", 0),
                    specs=p.get("specs", {}),
                    url=url,
                    description=p.get("description", ""),
                )
                db.add(obj)
                created += 1

        db.commit()
        return created

    # ----------------------------
    # Public entry
    # ----------------------------
    async def parse_and_save(self, db: Session) -> int:
        self.visited_urls.clear()
        self.product_urls.clear()
        self.product_data.clear()

        for u in self.START_URLS:
            await self.crawl_pages(u, max_depth=7)

        created = self._upsert_products_to_db(db)
        logger.info(f"DB upsert done (MSK). New created: {created}. Total parsed: {len(self.product_data)}")
        return created
