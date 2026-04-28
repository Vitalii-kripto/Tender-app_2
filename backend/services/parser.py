import asyncio
import hashlib
import html
import json
import logging
import re
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import urljoin, urlparse, urlunparse

import aiohttp
from bs4 import BeautifulSoup
from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger("GidroizolParser")

FETCH_CONCURRENCY = 16
FETCH_TIMEOUT = 25
FETCH_RETRIES = 2
PARSER_VERSION = "gidroizol-sitemap-v2"


class GidroizolParser:
    BASE_URL = "https://gidroizol.ru"
    DOMAIN = "gidroizol.ru"
    SITEMAP_URL = f"{BASE_URL}/sitemap.xml"
    MOSCOW_CITY_ID = 1

    STOP_DESC_HEADINGS = {
        "с этим товаром покупают",
        "аналоги",
        "отзывы",
        "сертификаты",
        "характеристики",
        "написать сообщение",
        "запросить сертификат",
    }

    IGNORE_PATH_PARTS = {
        "o-nas",
        "proektiruem-i-stroim",
        "search",
        "cart",
        "basket",
        "login",
        "auth",
        "manager",
        "connectors",
        "assets",
        "core",
        "privacy",
        "agreement",
        "policy",
    }

    FAMILY_ALIASES = {
        "изопласт": "ИЗОПЛАСТ",
        "изоэласт": "ИЗОЭЛАСТ",
        "техноэласт": "ТЕХНОЭЛАСТ",
        "унифлекс": "УНИФЛЕКС",
        "эластобит": "ЭЛАСТОБИТ",
        "эластоизол": "ЭЛАСТОИЗОЛ",
        "стеклоэласт": "СТЕКЛОЭЛАСТ",
        "стеклоизол": "СТЕКЛОИЗОЛ",
        "стеклофлекс": "СТЕКЛОФЛЕКС",
        "стеклокром": "СТЕКЛОКРОМ",
        "гидроизол": "ГИДРОИЗОЛ",
        "гидростеклоизол": "ГИДРОСТЕКЛОИЗОЛ",
        "линокром": "ЛИНОКРОМ",
        "рубероид": "РУБЕРОИД",
        "рубитэкс": "РУБИТЭКС",
        "филизол": "ФИЛИЗОЛ",
        "мостослой": "МОСТОСЛОЙ",
        "брит": "BRIT",
        "тирослой": "ТИРОСЛОЙ",
        "тэксослой": "ТЭКСОСЛОЙ",
        "кинефлекс": "КИНЕФЛЕКС",
        "виллатекс": "ВИЛЛАТЕКС",
        "икопал": "ICOPAL",
        "синтан": "СИНТАН",
        "дорнит": "ДОРНИТ",
        "пфг": "ПФГ",
        "изостуд": "ИЗОСТУД",
        "дрениз": "ДРЕНИЗ",
        "plastguard": "PLASTGUARD",
        "planter": "PLANTER",
        "пенебар": "ПЕНЕБАР",
    }

    def __init__(self):
        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Cookie": f"city={self.MOSCOW_CITY_ID}",
        }
        self._run_lock = asyncio.Lock()

    # ----------------------------
    # URL helpers
    # ----------------------------
    def _is_asset(self, url: str) -> bool:
        return str(url or "").lower().endswith(
            (".jpg", ".jpeg", ".png", ".svg", ".ico", ".webp", ".css", ".js", ".pdf", ".zip", ".xml")
        )

    def _is_same_domain(self, url: str) -> bool:
        netloc = (urlparse(url).netloc or "").replace("www.", "").lower()
        return bool(netloc) and netloc.endswith(self.DOMAIN)

    def canonicalize_url(self, url: str) -> str:
        raw = str(url or "").strip()
        if not raw:
            return ""
        if raw.startswith("/"):
            raw = urljoin(self.BASE_URL, raw)

        parsed = urlparse(raw)
        scheme = "https"
        host = (parsed.netloc or self.DOMAIN).replace("www.", "").lower()
        path = re.sub(r"/{2,}", "/", parsed.path or "/").rstrip("/") or "/"

        return urlunparse((scheme, host, path, "", "", ""))

    def normalize_url(self, url: str) -> str:
        return self.canonicalize_url(url)

    def _is_allowed_candidate_url(self, url: str) -> bool:
        canonical = self.canonicalize_url(url)
        if not canonical:
            return False
        if not self._is_same_domain(canonical):
            return False
        if self._is_asset(canonical):
            return False

        path = (urlparse(canonical).path or "/").strip("/")
        if not path:
            return False

        lowered = path.lower()
        return not any(part in lowered for part in self.IGNORE_PATH_PARTS)

    def _extract_site_product_id(self, url: str) -> str:
        path = (urlparse(url).path or "/").strip("/")
        if not path:
            return ""
        return path.split("/")[-1]

    # ----------------------------
    # Text / value helpers
    # ----------------------------
    @staticmethod
    def _clean_text(value: str) -> str:
        return re.sub(r"\s+", " ", html.unescape(str(value or ""))).strip()

    @staticmethod
    def _normalize_text(value: str) -> str:
        normalized = str(value or "").lower().replace("ё", "е")
        normalized = re.sub(r"[\"'`]", " ", normalized)
        normalized = re.sub(r"[^0-9a-zа-я%+./,\- ]+", " ", normalized)
        normalized = re.sub(r"\s+", " ", normalized).strip()
        return normalized

    @staticmethod
    def _clean_price(value: str) -> Optional[float]:
        if not value:
            return None
        cleaned = str(value).replace("\xa0", " ").replace(" ", "").replace(",", ".")
        cleaned = re.sub(r"[^\d.]", "", cleaned)
        if not cleaned:
            return None
        try:
            return float(cleaned)
        except Exception:
            return None

    @staticmethod
    def _safe_float(value: Any) -> Optional[float]:
        if value is None:
            return None
        try:
            return float(value)
        except Exception:
            return None

    @staticmethod
    def _normalize_unit(unit: str) -> str:
        cleaned = str(unit or "").strip().lower().replace("²", "2")
        cleaned = cleaned.rstrip(".")
        return cleaned

    def _extract_numeric_measure(self, value: str) -> Optional[float]:
        if not value:
            return None
        match = re.search(r"-?\d+(?:[.,]\d+)?", str(value))
        if not match:
            return None
        return self._clean_price(match.group(0))

    def _extract_temperature(self, value: str) -> Optional[float]:
        if not value:
            return None
        match = re.search(r"(-?\d+(?:[.,]\d+)?)", str(value).replace("−", "-"))
        if not match:
            return None
        try:
            return float(match.group(1).replace(",", "."))
        except Exception:
            return None

    def _extract_grade_markers(self, text: str) -> List[str]:
        normalized = self._normalize_text(text)
        markers: List[str] = []

        def add_marker(value: str) -> None:
            cleaned = self._clean_text(value)
            if cleaned and cleaned not in markers:
                markers.append(cleaned)

        for left, right in re.findall(r"\b(\d{2,3})\s*(?:/|\s)\s*(\d{1,2})\b", normalized):
            add_marker(f"{left}/{right}")
            add_marker(f"{left} {right}")

        for raw in re.findall(r"\b(\d{2,4})\b", normalized):
            try:
                value = int(raw)
            except Exception:
                continue
            if 50 <= value <= 1200:
                add_marker(raw)

        weight_match = re.search(r"\b(\d{2,3})\s*кг\b", normalized)
        if weight_match:
            add_marker(f"{weight_match.group(1)}кг")

        return markers

    def _flatten_specs(self, specs: Dict[str, Any]) -> str:
        parts: List[str] = []
        for key, value in (specs or {}).items():
            key_text = self._clean_text(str(key))
            value_text = self._clean_text(str(value))
            if key_text and value_text:
                parts.append(f"{key_text}: {value_text}")
        return "; ".join(parts)

    def _spec_value_by_keys(self, specs: Dict[str, Any], patterns: Sequence[str]) -> str:
        for key, value in (specs or {}).items():
            key_low = self._normalize_text(str(key))
            if any(pattern in key_low for pattern in patterns):
                return self._clean_text(str(value))
        return ""

    # ----------------------------
    # Network
    # ----------------------------
    async def _fetch_text(self, session: aiohttp.ClientSession, url: str) -> Optional[str]:
        target = self.canonicalize_url(url)
        if not target:
            return None

        timeout = aiohttp.ClientTimeout(total=FETCH_TIMEOUT)
        for attempt in range(FETCH_RETRIES + 1):
            try:
                async with session.get(target, timeout=timeout) as response:
                    if response.status != 200:
                        return None
                    return await response.text(errors="ignore")
            except Exception:
                if attempt >= FETCH_RETRIES:
                    return None
                await asyncio.sleep(0.5 * (attempt + 1))
        return None

    async def _fetch_sitemap_urls(self, session: aiohttp.ClientSession) -> List[str]:
        xml_text = await self._fetch_text(session, self.SITEMAP_URL)
        if not xml_text:
            logger.warning("Sitemap fetch failed: %s", self.SITEMAP_URL)
            return []

        urls: List[str] = []
        seen: set[str] = set()
        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError as e:
            logger.error("Sitemap XML parse error: %s", e)
            return []

        for loc in root.findall(".//{*}loc"):
            canonical = self.canonicalize_url(loc.text or "")
            if not self._is_allowed_candidate_url(canonical):
                continue
            if canonical in seen:
                continue
            seen.add(canonical)
            urls.append(canonical)

        logger.info("Sitemap loaded: %s candidate URLs", len(urls))
        return urls

    # ----------------------------
    # Page classification
    # ----------------------------
    def _count_add_to_cart_buttons(self, soup: BeautifulSoup) -> int:
        count = 0
        for el in soup.find_all(["a", "button"]):
            text_value = self._normalize_text(el.get_text(" ", strip=True))
            if "в корзину" in text_value:
                count += 1
        return count

    def _count_listing_cards(self, soup: BeautifulSoup) -> int:
        selectors = [
            ".list__item-wrapper",
            ".list__item.ms2_product",
            ".table__list .list__item",
            "form.ms2_form",
        ]
        max_count = 0
        for selector in selectors:
            max_count = max(max_count, len(soup.select(selector)))
        return max_count

    def is_listing_page(self, soup: BeautifulSoup) -> bool:
        listing_cards = self._count_listing_cards(soup)
        if listing_cards >= 4:
            return True

        heading_links = 0
        for link in soup.select("h2 a[href], h3 a[href], .left__heading a[href]"):
            href = self.canonicalize_url(link.get("href", ""))
            if href and self._is_allowed_candidate_url(href):
                heading_links += 1
        if heading_links >= 6:
            return True

        if self._count_add_to_cart_buttons(soup) >= 3 and listing_cards >= 2:
            return True
        return False

    def _extract_price_details(self, soup: BeautifulSoup) -> Dict[str, Any]:
        selectors = [
            ".dop-price",
            ".price-block-wrap",
            ".item__price",
            ".price__text",
        ]
        texts: List[str] = []
        for selector in selectors:
            for el in soup.select(selector):
                text_value = self._clean_text(el.get_text(" ", strip=True))
                if text_value and text_value not in texts:
                    texts.append(text_value)

        if not texts:
            text_value = self._clean_text(soup.get_text(" ", strip=True))
            if text_value:
                texts.append(text_value)

        retail = wholesale = special = None
        unit = ""
        availability = "unknown"
        combined = " || ".join(texts)

        if "под заказ" in self._normalize_text(combined):
            availability = "on_request"

        labeled_patterns = [
            ("retail", r"(\d+(?:[.,]\d+)?)\s*р\./\s*([a-zа-я0-9²./-]+)\s*розн"),
            ("wholesale", r"(\d+(?:[.,]\d+)?)\s*р\./\s*([a-zа-я0-9²./-]+)\s*опт"),
            ("special", r"(\d+(?:[.,]\d+)?)\s*р\./\s*([a-zа-я0-9²./-]+)\s*спец"),
        ]
        for label, pattern in labeled_patterns:
            match = re.search(pattern, combined, flags=re.IGNORECASE)
            if not match:
                continue
            value = self._clean_price(match.group(1))
            unit = unit or self._normalize_unit(match.group(2))
            if label == "retail":
                retail = value
            elif label == "wholesale":
                wholesale = value
            else:
                special = value

        if retail is None:
            generic = re.search(r"(\d+(?:[.,]\d+)?)\s*р\./\s*([a-zа-я0-9²./-]+)", combined, flags=re.IGNORECASE)
            if generic:
                retail = self._clean_price(generic.group(1))
                unit = unit or self._normalize_unit(generic.group(2))

        if retail is None:
            for selector in [".price_val", ".price", ".product-price", ".detail-price", "[itemprop='price']"]:
                for el in soup.select(selector):
                    value = self._clean_price(el.get_text(" ", strip=True))
                    if value is not None:
                        retail = value
                        break
                if retail is not None:
                    break

        if retail is not None and availability == "unknown":
            availability = "in_stock"

        return {
            "retail": retail,
            "wholesale": wholesale,
            "special": special,
            "unit": unit or None,
            "availability": availability,
        }

    def parse_specs(self, soup: BeautifulSoup) -> Dict[str, str]:
        specs: Dict[str, str] = {}
        row_selectors = [
            "div.table_dop-info .table-row",
            "table.specs tr",
            "table tr",
            ".product-features li",
        ]

        rows = []
        for selector in row_selectors:
            rows = soup.select(selector)
            if rows:
                break

        for row in rows:
            tds = row.select("td")
            if len(tds) >= 2:
                key = self._clean_text(tds[0].get_text(" ", strip=True)).rstrip(":")
                value = self._clean_text(tds[1].get_text(" ", strip=True))
                if key and value:
                    specs[key] = value
                continue

            cells = row.select("div.table-cell")
            if len(cells) >= 2:
                key = self._clean_text(cells[0].get_text(" ", strip=True)).rstrip(":")
                value = self._clean_text(cells[1].get_text(" ", strip=True))
                if key and value:
                    specs[key] = value
                continue

            text_value = self._clean_text(row.get_text(" ", strip=True))
            if ":" in text_value:
                key, value = text_value.split(":", 1)
                key = self._clean_text(key).rstrip(":")
                value = self._clean_text(value)
                if key and value:
                    specs[key] = value

        return specs

    def _clean_description_block(self, text_value: str, title: str = "") -> str:
        cleaned = self._clean_text(text_value)
        if not cleaned:
            return ""
        cleaned = re.sub(r"^\s*описание\s*", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\s*все описание\s*$", "", cleaned, flags=re.IGNORECASE)
        if title:
            normalized_title = self._normalize_text(title)
            normalized_cleaned = self._normalize_text(cleaned)
            if normalized_cleaned.startswith(normalized_title):
                cleaned = cleaned[len(title):].strip(" .:-")
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
        return cleaned[:3000]

    def parse_description(self, soup: BeautifulSoup, title: str = "") -> str:
        selectors = [
            ".content_box.ver2 .visible-content",
            ".content_box.ver2",
            ".visible-content",
            '[itemprop="description"]',
            ".product-description",
            ".detail-text",
            ".desc",
            "#tab-description",
        ]
        for selector in selectors:
            for el in soup.select(selector):
                text_value = self._clean_description_block(el.get_text(" ", strip=True), title=title)
                if len(text_value) >= 40:
                    return text_value

        header_node = None
        for tag in soup.find_all(["h2", "h3", "h4", "div"], string=re.compile(r"Описание", re.IGNORECASE)):
            header_node = tag
            break

        if header_node:
            chunks: List[str] = []
            for sibling in header_node.next_siblings:
                if getattr(sibling, "name", None) in ["h2", "h3", "h4", "section", "footer"]:
                    stop_text = self._normalize_text(getattr(sibling, "get_text", lambda *args, **kwargs: "")(" ", strip=True))
                    if any(marker in stop_text for marker in self.STOP_DESC_HEADINGS):
                        break

                if getattr(sibling, "name", None) in ["p", "div", "ul", "ol", "span"]:
                    text_value = self._clean_description_block(sibling.get_text(" ", strip=True), title=title)
                    if len(text_value) >= 25:
                        chunks.append(text_value)
            if chunks:
                return self._clean_description_block(" ".join(chunks), title=title)

        meta_description = self.parse_meta_description(soup, title=title)
        if meta_description:
            return meta_description
        return ""

    def parse_meta_description(self, soup: BeautifulSoup, title: str = "") -> str:
        meta = soup.find("meta", attrs={"name": "description"})
        content = meta.get("content", "") if meta else ""
        return self._clean_description_block(content, title=title)

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
        for selector in breadcrumb_selectors:
            items = soup.select(selector)
            if items:
                crumbs = [self._clean_text(item.get_text(strip=True)) for item in items if item.get_text(strip=True)]
                break

        drop = {"главная", "home", "каталог", "все категории"}
        categories = [item for item in crumbs if self._normalize_text(item) not in drop]

        h1 = soup.select_one("h1")
        if h1:
            h1_text = self._clean_text(h1.get_text(strip=True))
            if categories and categories[-1] == h1_text:
                categories = categories[:-1]

        return " / ".join(categories[-3:]) if categories else "Каталог"

    def _extract_title(self, soup: BeautifulSoup) -> str:
        for selector in ["section#tovar h1", "h1", "div.product-title h1", "div.product-title"]:
            el = soup.select_one(selector)
            if el:
                title = self._clean_text(el.get_text(" ", strip=True))
                if title:
                    return title
        return ""

    def is_product_page(self, soup: BeautifulSoup, url: str = "") -> bool:
        title = self._extract_title(soup)
        if not title:
            return False
        if self.is_listing_page(soup):
            return False

        specs = self.parse_specs(soup)
        price_details = self._extract_price_details(soup)
        price_value = price_details.get("retail")
        breadcrumb_count = len(soup.select("section#breadcrumbs ul li a, .breadcrumb a"))
        description = self.parse_description(soup, title=title)

        if len(specs) >= 3 and price_value is not None:
            return True
        if price_value is not None and breadcrumb_count >= 2:
            return True
        if len(description) >= 80 and len(specs) >= 2:
            return True
        return False

    # ----------------------------
    # Product normalization
    # ----------------------------
    def _build_normalized_category(self, category: str, category_leaf: str, material_group: str) -> str:
        parts = [self._normalize_text(category), self._normalize_text(category_leaf), self._normalize_text(material_group)]
        return " | ".join(part for part in parts if part)

    def _infer_product_family(self, title: str, category_leaf: str) -> str:
        base = f"{category_leaf} {title}".lower()
        for alias, canonical in self.FAMILY_ALIASES.items():
            if alias in base:
                return canonical
        cleaned_leaf = self._clean_text(category_leaf)
        if cleaned_leaf and len(cleaned_leaf) >= 4:
            return cleaned_leaf
        return ""

    def _infer_material_group(self, title: str, category: str) -> str:
        text_value = self._normalize_text(f"{title} {category}")
        if any(token in text_value for token in ["мастик"]):
            return "мастика"
        if "праймер" in text_value:
            return "праймер"
        if "гермет" in text_value:
            return "герметик"
        if any(token in text_value for token in ["гидрошпонк", "пенебар", "гидропроклад"]):
            return "гидрошпонка"
        if any(token in text_value for token in ["геомембран", "изостуд", "дрениз", "plastguard", "planter"]):
            return "геомембрана"
        if any(token in text_value for token in ["геотекст", "дорнит", "пфг", "иглопробив", "фильерн"]):
            return "геотекстиль"
        if "мембран" in text_value:
            return "мембрана"
        if any(token in text_value for token in ["утеплител", "минват", "пенополист", "пеноплэкс", "isover", "ursa"]):
            return "утеплитель"
        if any(token in text_value for token in ["битум", "бн 90/10"]):
            return "битум"
        if any(token in text_value for token in ["контроллер", "вентиляц", "оборудован", "горелк", "баллон", "редуктор"]):
            return "оборудование"
        if any(
            token in text_value
            for token in [
                "рулон",
                "тпп",
                "ткп",
                "хпп",
                "хкп",
                "эпп",
                "экп",
                "эмп",
                "изопласт",
                "техноэласт",
                "унифлекс",
                "стеклоэласт",
                "стеклоизол",
                "линокром",
                "рубероид",
                "мостослой",
                "эластобит",
                "эластоизол",
            ]
        ):
            return "рулонная гидроизоляция"
        return "гидроизоляция"

    def _infer_material_type(self, material_group: str) -> str:
        mapping = {
            "рулонная гидроизоляция": "Рулонный",
            "мастика": "Мастика",
            "праймер": "Праймер",
            "герметик": "Герметик",
            "гидрошпонка": "Гидрошпонка",
            "геотекстиль": "Геотекстиль",
            "геомембрана": "Геомембрана",
            "мембрана": "Мембрана",
            "утеплитель": "Утеплитель",
            "битум": "Битум",
            "оборудование": "Оборудование",
            "гидроизоляция": "Гидроизоляция",
        }
        return mapping.get(material_group, "Гидроизоляция")

    def _infer_material_subgroup(self, title: str, specs: Dict[str, Any]) -> str:
        text_value = self._normalize_text(f"{title} {self._flatten_specs(specs)}")
        for marker in ["тпп", "ткп", "хпп", "хкп", "эпп", "экп", "эмп"]:
            if re.search(rf"\b{marker}\b", text_value):
                return marker.upper()
        if "битумно полимер" in text_value or "сбс" in text_value:
            return "битумно-полимерный"
        if "битум" in text_value:
            return "битумный"
        return ""

    def _infer_application_scope(self, title: str, category: str, description: str) -> str:
        text_value = self._normalize_text(f"{title} {category} {description}")
        if "мост" in text_value:
            return "мосты"
        if "фундамент" in text_value:
            return "фундамент"
        if "кровл" in text_value:
            return "кровля"
        if "шв" in text_value:
            return "швы"
        if "гидроизоляц" in text_value:
            return "гидроизоляция"
        return ""

    def _infer_base_material(self, title: str, specs: Dict[str, Any]) -> str:
        base = self._spec_value_by_keys(specs, ["основа", "армирующая основа"])
        if base:
            normalized = self._normalize_text(base)
            if "полиэстер" in normalized:
                return "полиэстер"
            if "стеклоткан" in normalized:
                return "стеклоткань"
            if "стеклохолст" in normalized:
                return "стеклохолст"
            return base

        title_low = self._normalize_text(title)
        if "п/э" in title_low:
            return "полиэстер"
        if "с/т" in title_low:
            return "стеклоткань"
        if "х/ст" in title_low:
            return "стеклохолст"
        return ""

    def _infer_binder_type(self, title: str, specs: Dict[str, Any], description: str) -> str:
        text_value = self._normalize_text(f"{title} {description} {self._flatten_specs(specs)}")
        if "сбс" in text_value:
            return "СБС"
        if "полимер" in text_value:
            return "полимер"
        if "битум" in text_value:
            return "битум"
        return ""

    def _infer_surface(self, specs: Dict[str, Any], surface_kind: str) -> str:
        value = self._spec_value_by_keys(specs, ["покрытие верхнее/нижнее", "покрытие"])
        if not value:
            return ""
        if "/" in value:
            top, bottom = [self._clean_text(part) for part in value.split("/", 1)]
            return top if surface_kind == "top" else bottom
        return value if surface_kind == "top" else value

    def _infer_color(self, title: str, specs: Dict[str, Any]) -> str:
        from_specs = self._spec_value_by_keys(specs, ["цвет"])
        if from_specs:
            return from_specs
        title_low = self._normalize_text(title)
        for color in ["серый", "зеленый", "красный", "черный", "коричневый", "белый"]:
            if color in title_low:
                return color
        return ""

    def _infer_standard_code(self, title: str, specs: Dict[str, Any], description: str) -> str:
        text_value = f"{title} {description} {self._flatten_specs(specs)}"
        match = re.search(r"\b(?:ГОСТ|ТУ)\s*[\d.\-/]+\b", text_value, flags=re.IGNORECASE)
        return self._clean_text(match.group(0)) if match else ""

    def _calculate_quality_score(
        self,
        title: str,
        category: str,
        description: str,
        meta_description: str,
        specs: Dict[str, Any],
        price_details: Dict[str, Any],
    ) -> int:
        score = 0
        if title:
            score += 20
        if category:
            score += 10
        if price_details.get("retail") is not None:
            score += 10
        if price_details.get("unit"):
            score += 5
        specs_count = len(specs or {})
        score += min(specs_count * 4, 30)
        if len(description or "") >= 80:
            score += 15
        elif description:
            score += 8
        if meta_description:
            score += 5
        return min(score, 100)

    def _build_family_specific_aliases(
        self,
        title: str,
        product_family: str,
        category_leaf: str,
    ) -> List[Tuple[str, str]]:
        source = self._normalize_text(f"{category_leaf} {product_family} {title}")
        grade_markers = self._extract_grade_markers(source)
        aliases: List[Tuple[str, str]] = []

        def add_alias(value: str, alias_type: str = "family_synonym") -> None:
            cleaned = self._clean_text(value)
            normalized = self._normalize_text(cleaned)
            if cleaned and normalized:
                aliases.append((cleaned, alias_type))

        if any(token in source for token in ["пфг", "полиэфирн", "фильерн"]):
            add_alias("Геотекстиль ПФГ")
            add_alias("Геотекстиль полиэфирный")
            for marker in grade_markers:
                if re.fullmatch(r"\d{2,4}", marker):
                    add_alias(f"ПФГ {marker}")
                    add_alias(f"Геотекстиль ПФГ {marker}")
                    add_alias(f"Геотекстиль полиэфирный {marker}")

        if "дорнит" in source:
            add_alias("Геотекстиль Дорнит")
            add_alias("Геотекстиль иглопробивной")
            for marker in grade_markers:
                if re.fullmatch(r"\d{2,4}", marker):
                    add_alias(f"ДОРНИТ {marker}")
                    add_alias(f"Геотекстиль Дорнит {marker}")

        geomembrane_brands = {
            "изостуд": "ИЗОСТУД",
            "дрениз": "ДРЕНИЗ",
            "plastguard": "PLASTGUARD",
            "planter": "PLANTER",
        }
        matched_geomembrane_brand = ""
        for token, canonical in geomembrane_brands.items():
            if token in source:
                matched_geomembrane_brand = canonical
                break
        if matched_geomembrane_brand:
            add_alias(f"Геомембрана {matched_geomembrane_brand}")
            add_alias(f"Профилированная мембрана {matched_geomembrane_brand}")

        if "битум" in source:
            add_alias("Битум")
            for marker in grade_markers:
                if re.fullmatch(r"\d{2,3}/\d{1,2}", marker):
                    add_alias(f"Битум {marker}")
                elif re.fullmatch(r"\d{2,3} \d{1,2}", marker):
                    add_alias(f"Битум {marker}")
                elif re.fullmatch(r"\d{2,3}кг", marker) and "брикет" in source:
                    add_alias(f"Битум брикет {marker}")
                    add_alias(f"Брикет {marker}")

        if any(token in source for token in ["гидрошпонк", "пенебар", "гидропроклад"]):
            add_alias("Гидрошпонка")
            add_alias("Гидропрокладка")

        return aliases

    def _build_aliases(self, title: str, product_family: str, category_leaf: str) -> List[Tuple[str, str]]:
        aliases: List[Tuple[str, str]] = []
        for alias, alias_type in [
            (title, "source_title"),
            (product_family, "family"),
            (category_leaf, "category_leaf"),
        ]:
            cleaned = self._clean_text(alias)
            normalized = self._normalize_text(cleaned)
            if cleaned and normalized:
                aliases.append((cleaned, alias_type))

        if product_family and title and self._normalize_text(product_family) not in self._normalize_text(title):
            combined = f"{product_family} {title}"
            aliases.append((combined, "family_title"))

        aliases.extend(self._build_family_specific_aliases(title, product_family, category_leaf))

        deduped: List[Tuple[str, str]] = []
        seen: set[str] = set()
        for alias, alias_type in aliases:
            normalized = self._normalize_text(alias)
            if normalized in seen:
                continue
            seen.add(normalized)
            deduped.append((alias, alias_type))
        return deduped

    def _build_content_hash(
        self,
        title: str,
        category: str,
        price_details: Dict[str, Any],
        description: str,
        specs: Dict[str, Any],
    ) -> str:
        payload = json.dumps(
            {
                "title": title,
                "category": category,
                "retail": price_details.get("retail"),
                "wholesale": price_details.get("wholesale"),
                "special": price_details.get("special"),
                "description": description,
                "specs": specs,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
        return hashlib.md5(payload.encode("utf-8")).hexdigest()

    # ----------------------------
    # Parsing entry point
    # ----------------------------
    def parse_product_page(self, url: str, html_text: str) -> Optional[Dict[str, Any]]:
        canonical_url = self.canonicalize_url(url)
        if not self._is_allowed_candidate_url(canonical_url):
            return None

        soup = BeautifulSoup(html_text, "html.parser")
        if not self.is_product_page(soup, canonical_url):
            return None

        title = self._extract_title(soup)
        if not title:
            return None

        category = self.extract_category_path(soup)
        category_leaf = category.split(" / ")[-1] if category else ""
        price_details = self._extract_price_details(soup)
        specs = self.parse_specs(soup)
        description = self.parse_description(soup, title=title)
        meta_description = self.parse_meta_description(soup, title=title)

        material_group = self._infer_material_group(title, category)
        quality_score = self._calculate_quality_score(
            title=title,
            category=category,
            description=description,
            meta_description=meta_description,
            specs=specs,
            price_details=price_details,
        )

        return {
            "vendor": "gidroizol",
            "site_product_id": self._extract_site_product_id(canonical_url),
            "url": canonical_url,
            "source_url": canonical_url,
            "city_id": self.MOSCOW_CITY_ID,
            "title": title,
            "category": category,
            "category_leaf": category_leaf,
            "normalized_category": self._build_normalized_category(category, category_leaf, material_group),
            "searchable_for_analogs": quality_score >= 20,
            "material_type": self._infer_material_type(material_group),
            "price": price_details.get("retail"),
            "price_wholesale": price_details.get("wholesale"),
            "price_special": price_details.get("special"),
            "price_currency": "RUB",
            "price_unit": price_details.get("unit"),
            "availability_status": price_details.get("availability") or "unknown",
            "specs": specs,
            "specs_text": self._flatten_specs(specs),
            "description": description,
            "meta_description": meta_description,
            "quality_score": quality_score,
            "content_hash": self._build_content_hash(title, category, price_details, description, specs),
            "parse_version": PARSER_VERSION,
            "is_active": True,
        }

    # ----------------------------
    # Async sitemap parsing
    # ----------------------------
    async def _parse_catalog(self) -> List[Dict[str, Any]]:
        products_by_url: Dict[str, Dict[str, Any]] = {}
        visited = 0
        parsed_products = 0

        connector = aiohttp.TCPConnector(limit=FETCH_CONCURRENCY, ssl=False)
        async with aiohttp.ClientSession(headers=self.headers, connector=connector) as session:
            sitemap_urls = await self._fetch_sitemap_urls(session)
            if not sitemap_urls:
                return []

            semaphore = asyncio.Semaphore(FETCH_CONCURRENCY)

            async def handle(url: str) -> None:
                nonlocal visited, parsed_products
                async with semaphore:
                    visited += 1
                    html_text = await self._fetch_text(session, url)
                    if not html_text:
                        return

                    product = self.parse_product_page(url, html_text)
                    if not product:
                        return

                    products_by_url[product["url"]] = product
                    parsed_products += 1

                    if parsed_products % 100 == 0:
                        logger.info(
                            "Parser progress: visited=%s/%s parsed_products=%s",
                            visited,
                            len(sitemap_urls),
                            parsed_products,
                        )

            await asyncio.gather(*(handle(url) for url in sitemap_urls))

        products = list(products_by_url.values())
        logger.info("Catalog parsing completed: %s product pages", len(products))
        return products

    # ----------------------------
    # Analog index rebuild
    # ----------------------------
    def _build_analog_index_record(self, product: Any) -> Dict[str, Any]:
        specs = product.specs if isinstance(product.specs, dict) else {}
        if product.specs and not specs:
            try:
                specs = json.loads(product.specs) if isinstance(product.specs, str) else {}
            except Exception:
                specs = {}

        title = self._clean_text(getattr(product, "title", ""))
        category = self._clean_text(getattr(product, "category", ""))
        category_leaf = self._clean_text(getattr(product, "category_leaf", ""))
        description = self._clean_text(getattr(product, "description", ""))
        meta_description = self._clean_text(getattr(product, "meta_description", ""))
        material_type = self._clean_text(getattr(product, "material_type", ""))

        product_family = self._infer_product_family(title, category_leaf)
        material_group = self._infer_material_group(title, category)
        material_subgroup = self._infer_material_subgroup(title, specs)
        application_scope = self._infer_application_scope(title, category, description)
        base_material = self._infer_base_material(title, specs)
        binder_type = self._infer_binder_type(title, specs, description)
        top_surface = self._infer_surface(specs, "top")
        bottom_surface = self._infer_surface(specs, "bottom")

        extracted_attrs = {
            "price_unit": getattr(product, "price_unit", None),
            "availability_status": getattr(product, "availability_status", None),
            "quality_score": getattr(product, "quality_score", None),
        }

        normalized_title = self._normalize_text(title)
        search_parts = [
            normalized_title,
            self._normalize_text(category),
            self._normalize_text(material_type),
            self._normalize_text(product_family),
            self._normalize_text(material_group),
            self._normalize_text(material_subgroup),
            self._normalize_text(base_material),
            self._normalize_text(description),
            self._normalize_text(meta_description),
            self._normalize_text(getattr(product, "specs_text", self._flatten_specs(specs))),
        ]
        search_text = " ".join(part for part in search_parts if part)

        exact_model_key = " | ".join(
            part for part in [normalized_title, self._normalize_text(product_family), self._normalize_text(base_material)] if part
        )
        analog_group_key = " | ".join(
            part
            for part in [
                self._normalize_text(material_group),
                self._normalize_text(product_family),
                self._normalize_text(base_material),
                self._normalize_text(material_subgroup),
            ]
            if part
        )

        return {
            "product_id": product.id,
            "normalized_title": normalized_title or self._normalize_text(title),
            "brand": product_family or None,
            "series": category_leaf or None,
            "product_family": product_family or None,
            "material_group": material_group or "гидроизоляция",
            "material_subgroup": material_subgroup or None,
            "application_scope": application_scope or None,
            "base_material": base_material or None,
            "binder_type": binder_type or None,
            "top_surface": top_surface or None,
            "bottom_surface": bottom_surface or None,
            "thickness_mm": self._extract_numeric_measure(self._spec_value_by_keys(specs, ["толщин"])),
            "mass_kg_m2": self._extract_numeric_measure(self._spec_value_by_keys(specs, ["масса 1м2", "масса 1 м2", "масса 1м²"])),
            "density_kg_m3": self._extract_numeric_measure(self._spec_value_by_keys(specs, ["плотност"])),
            "roll_length_m": self._extract_numeric_measure(self._spec_value_by_keys(specs, ["длина"])),
            "roll_width_m": self._extract_numeric_measure(self._spec_value_by_keys(specs, ["ширина"])),
            "roll_area_m2": self._extract_numeric_measure(self._spec_value_by_keys(specs, ["площадь рулона", "площадь"])),
            "package_weight_kg": self._extract_numeric_measure(self._spec_value_by_keys(specs, ["вес", "масса упаковки"])),
            "flexibility_temp_c": self._extract_temperature(self._spec_value_by_keys(specs, ["гибкость"])),
            "heat_resistance_c": self._extract_temperature(self._spec_value_by_keys(specs, ["теплостойк"])),
            "color": self._infer_color(title, specs) or None,
            "standard_code": self._infer_standard_code(title, specs, description) or None,
            "extracted_attrs_json": extracted_attrs,
            "search_text": search_text or normalized_title,
            "analog_group_key": analog_group_key or None,
            "exact_model_key": exact_model_key or None,
            "updated_at": datetime.utcnow(),
        }

    def _rebuild_analog_tables(self, db: Session) -> None:
        try:
            from backend.models import ProductAliasModel, ProductAnalogIndexModel, ProductModel  # type: ignore
        except Exception:
            from ..models import ProductAliasModel, ProductAnalogIndexModel, ProductModel  # type: ignore

        products = db.query(ProductModel).all()
        analog_rows: List[Dict[str, Any]] = []
        alias_rows: List[Dict[str, Any]] = []
        fts_rows: List[Dict[str, Any]] = []

        for product in products:
            analog_row = self._build_analog_index_record(product)
            analog_rows.append(analog_row)

            aliases = self._build_aliases(
                product.title or "",
                analog_row.get("product_family") or "",
                product.category_leaf or "",
            )
            alias_texts: List[str] = []
            for alias, alias_type in aliases:
                normalized_alias = self._normalize_text(alias)
                alias_rows.append(
                    {
                        "product_id": product.id,
                        "alias": alias,
                        "alias_normalized": normalized_alias,
                        "alias_type": alias_type,
                    }
                )
                alias_texts.append(normalized_alias)

            fts_rows.append(
                {
                    "product_id": product.id,
                    "normalized_title": analog_row["normalized_title"],
                    "search_text": analog_row["search_text"],
                    "specs_text": self._normalize_text(getattr(product, "specs_text", "") or ""),
                    "description": self._normalize_text(getattr(product, "description", "") or ""),
                    "aliases": " ".join(alias_texts),
                }
            )

        db.execute(text("DELETE FROM product_analog_index"))
        db.execute(text("DELETE FROM product_aliases"))
        try:
            db.execute(text("DELETE FROM product_search_fts"))
        except Exception:
            logger.debug("FTS delete skipped: product_search_fts is not available")

        if analog_rows:
            db.bulk_insert_mappings(ProductAnalogIndexModel, analog_rows)
        if alias_rows:
            db.bulk_insert_mappings(ProductAliasModel, alias_rows)
        if fts_rows:
            try:
                db.execute(
                    text(
                        """
                        INSERT INTO product_search_fts
                            (product_id, normalized_title, search_text, specs_text, description, aliases)
                        VALUES
                            (:product_id, :normalized_title, :search_text, :specs_text, :description, :aliases)
                        """
                    ),
                    fts_rows,
                )
            except Exception as e:
                logger.warning("FTS insert skipped: %s", e)

        db.commit()
        logger.info(
            "Analog index rebuilt: products=%s analog_rows=%s alias_rows=%s",
            len(products),
            len(analog_rows),
            len(alias_rows),
        )

    # ----------------------------
    # DB upsert
    # ----------------------------
    def _upsert_products_to_db(self, db: Session, products: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
        try:
            from backend.models import ProductModel  # type: ignore
        except Exception:
            from ..models import ProductModel  # type: ignore

        existing_rows = db.query(ProductModel).all()
        existing_by_url = {
            self.canonicalize_url(row.url or ""): row
            for row in existing_rows
            if self.canonicalize_url(row.url or "")
        }

        now = datetime.utcnow()
        inserts: List[Dict[str, Any]] = []
        updates: List[Dict[str, Any]] = []
        seen_existing_urls: set[str] = set()
        missing_url = 0

        for product in products:
            canonical_url = self.canonicalize_url(product.get("url", ""))
            if not canonical_url:
                missing_url += 1
                continue

            existing = existing_by_url.get(canonical_url)
            payload = {
                "vendor": product.get("vendor", "gidroizol"),
                "site_product_id": product.get("site_product_id"),
                "source_url": product.get("source_url") or canonical_url,
                "city_id": product.get("city_id", self.MOSCOW_CITY_ID),
                "title": product.get("title", "Не указано"),
                "category": product.get("category", "Каталог"),
                "category_leaf": product.get("category_leaf"),
                "normalized_category": product.get("normalized_category"),
                "searchable_for_analogs": bool(product.get("searchable_for_analogs", True)),
                "material_type": product.get("material_type", "Гидроизоляция"),
                "price": product.get("price"),
                "price_wholesale": product.get("price_wholesale"),
                "price_special": product.get("price_special"),
                "price_currency": product.get("price_currency", "RUB"),
                "price_unit": product.get("price_unit"),
                "availability_status": product.get("availability_status", "unknown"),
                "specs": product.get("specs", {}),
                "specs_text": product.get("specs_text") or self._flatten_specs(product.get("specs", {})),
                "url": canonical_url,
                "description": product.get("description", ""),
                "meta_description": product.get("meta_description", ""),
                "quality_score": product.get("quality_score", 0),
                "content_hash": product.get("content_hash"),
                "parse_version": product.get("parse_version", PARSER_VERSION),
                "last_seen_at": now,
                "scraped_at": now,
                "updated_at": now,
                "is_active": True,
            }

            if existing:
                seen_existing_urls.add(canonical_url)
                payload["id"] = existing.id
                if not payload["description"] and existing.description:
                    payload["description"] = existing.description
                if not payload["meta_description"] and getattr(existing, "meta_description", None):
                    payload["meta_description"] = existing.meta_description
                if not payload["specs"] and existing.specs:
                    payload["specs"] = existing.specs
                    payload["specs_text"] = getattr(existing, "specs_text", "") or self._flatten_specs(existing.specs or {})
                updates.append(payload)
            else:
                payload["first_seen_at"] = now
                inserts.append(payload)

        if inserts:
            db.bulk_insert_mappings(ProductModel, inserts)
        if updates:
            db.bulk_update_mappings(ProductModel, updates)
        db.commit()
        not_seen_rows = [
            row
            for canonical_url, row in existing_by_url.items()
            if canonical_url not in seen_existing_urls
        ]
        not_seen_sample = [
            self._clean_text(getattr(row, "title", "")) or f"id={getattr(row, 'id', '?')}"
            for row in not_seen_rows[:5]
        ]
        stats = {
            "parsed_input": len(products),
            "inserted": len(inserts),
            "updated": len(updates),
            "affected": len(inserts) + len(updates),
            "missing_url": missing_url,
            "existing_before": len(existing_by_url),
            "matched_existing": len(seen_existing_urls),
            "not_seen_existing": len(not_seen_rows),
            "not_seen_existing_sample": not_seen_sample,
        }
        logger.info(
            "DB upsert completed | parsed_input=%s | inserted=%s | updated=%s | missing_url=%s | "
            "existing_before=%s | not_seen_existing=%s",
            stats["parsed_input"],
            stats["inserted"],
            stats["updated"],
            stats["missing_url"],
            stats["existing_before"],
            stats["not_seen_existing"],
        )
        if stats["inserted"] == 0 and stats["updated"] > 0:
            logger.info(
                "No new products inserted: all parsed URLs already existed in DB and were updated."
            )
        if stats["not_seen_existing"]:
            logger.info(
                "Existing products not seen in current sitemap run: %s | sample=%s",
                stats["not_seen_existing"],
                ", ".join(not_seen_sample) or "-",
            )
        return stats

    # ----------------------------
    # Public entry
    # ----------------------------
    async def parse_and_save(self, db: Session) -> Dict[str, Any]:
        async with self._run_lock:
            logger.info("Starting gidroizol sitemap parse | version=%s", PARSER_VERSION)
            products = await self._parse_catalog()
            if not products:
                logger.warning("Catalog parse returned no products")
                return {
                    "parsed": 0,
                    "inserted": 0,
                    "updated": 0,
                    "affected": 0,
                    "missing_url": 0,
                    "existing_before": 0,
                    "matched_existing": 0,
                    "not_seen_existing": 0,
                    "not_seen_existing_sample": [],
                }

            upsert_stats = self._upsert_products_to_db(db, products)
            self._rebuild_analog_tables(db)
            logger.info(
                "Catalog parse completed | parsed=%s | inserted=%s | updated=%s | missing_url=%s | "
                "existing_before=%s | not_seen_existing=%s | version=%s",
                len(products),
                upsert_stats["inserted"],
                upsert_stats["updated"],
                upsert_stats["missing_url"],
                upsert_stats["existing_before"],
                upsert_stats["not_seen_existing"],
                PARSER_VERSION,
            )
            return {
                "parsed": len(products),
                **upsert_stats,
            }
