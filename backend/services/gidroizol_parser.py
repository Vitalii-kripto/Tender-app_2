"""
Парсер каталога gidroizol.ru для наполнения локальной БД материалами.
Используется для подбора аналогов в тендерах.
"""

import re
import time
import logging
import requests
from bs4 import BeautifulSoup
from typing import Optional

logger = logging.getLogger("LegalAI")

# Базовый URL сайта
BASE_URL = "https://gidroizol.ru"

# Категории каталога (url_id: название)
CATALOG_CATEGORIES = {
    "137":  "Гидроизол рулонный",
    "138":  "Рубероид",
    "139":  "Пергамин",
    "140":  "Мастика битумная",
    "141":  "Праймер битумный",
    "142":  "Геотекстиль",
    "143":  "Стеклоткань / Стеклохолст",
    "144":  "Геомембрана",
    "145":  "Техноэласт",
    "146":  "Технониколь кровельные",
    "147":  "Утеплитель",
    "148":  "Кузбасслак",
    "149":  "Герметик",
    "150":  "Пленка полиэтиленовая",
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
}


class GidroizolParser:
    """
    Парсер каталога gidroizol.ru.
    Скачивает список товаров и их характеристики.
    """

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        logger.info("[GidroizolParser] Initialized.")

    def _get_page(self, url: str, timeout: int = 15) -> Optional[BeautifulSoup]:
        """Загружает страницу и возвращает BeautifulSoup объект."""
        try:
            resp = self.session.get(url, timeout=timeout)
            resp.raise_for_status()
            resp.encoding = "utf-8"
            return BeautifulSoup(resp.text, "html.parser")
        except Exception as e:
            logger.warning(f"[GidroizolParser] Failed to fetch {url}: {e}")
            return None

    def parse_category(self, category_id: str, category_name: str) -> list:
        """
        Парсит одну категорию каталога.
        Возвращает список словарей с данными о товарах.
        """
        url = f"{BASE_URL}/{category_id}"
        logger.info(f"[GidroizolParser] Parsing category '{category_name}': {url}")

        soup = self._get_page(url)
        if not soup:
            return []

        products = []

        # Ищем карточки товаров (адаптировано под структуру gidroizol.ru)
        # Структура: div.catalog-item или article или li с ссылкой на товар
        item_selectors = [
            "div.catalog-item",
            "div.product-item",
            "article.product",
            "div.item",
            "li.item",
        ]

        items = []
        for selector in item_selectors:
            items = soup.select(selector)
            if items:
                break

        # Если специфичные селекторы не нашли — берём все ссылки на товары
        if not items:
            product_links = soup.find_all(
                "a",
                href=re.compile(r"gidroizol\.ru/\d+")
            )
            for link in product_links:
                href = link.get("href", "")
                title_text = link.get_text(strip=True)
                if title_text and len(title_text) > 5:
                    product_url = href if href.startswith("http") else f"{BASE_URL}{href}"
                    products.append({
                        "title": title_text,
                        "category": category_name,
                        "material_type": category_name,
                        "price": None,
                        "url": product_url,
                        "description": "",
                        "specs": {},
                    })
            return products

        # Парсим найденные карточки
        for item in items:
            try:
                # Название
                title_el = (
                    item.find("h2") or
                    item.find("h3") or
                    item.find(class_=re.compile(r"title|name")) or
                    item.find("a")
                )
                title = title_el.get_text(strip=True) if title_el else ""
                if not title:
                    continue

                # Ссылка
                link_el = item.find("a", href=True)
                href = link_el["href"] if link_el else ""
                product_url = href if href.startswith("http") else f"{BASE_URL}{href}"

                # Цена
                price_el = item.find(class_=re.compile(r"price|cost|стоимость"))
                price_text = price_el.get_text(strip=True) if price_el else ""
                price = None
                if price_text:
                    price_digits = re.sub(r"[^\d.]", "", price_text.replace(",", "."))
                    try:
                        price = float(price_digits) if price_digits else None
                    except ValueError:
                        price = None

                # Описание
                desc_el = item.find(class_=re.compile(r"desc|text|intro"))
                description = desc_el.get_text(strip=True) if desc_el else ""

                products.append({
                    "title": title,
                    "category": category_name,
                    "material_type": category_name,
                    "price": price,
                    "url": product_url,
                    "description": description,
                    "specs": {},
                })
            except Exception as e:
                logger.debug(f"[GidroizolParser] Error parsing item: {e}")
                continue

        logger.info(
            f"[GidroizolParser] Category '{category_name}': found {len(products)} products"
        )
        return products

    def parse_product_details(self, product_url: str) -> dict:
        """
        Парсит страницу конкретного товара для получения характеристик.
        Возвращает словарь specs с техническими характеристиками.
        """
        soup = self._get_page(product_url)
        if not soup:
            return {}

        specs = {}

        # Ищем таблицу характеристик
        table = soup.find("table", class_=re.compile(r"spec|char|prop|param|table"))
        if not table:
            table = soup.find("table")

        if table:
            rows = table.find_all("tr")
            for row in rows:
                cells = row.find_all(["td", "th"])
                if len(cells) >= 2:
                    key = cells[0].get_text(strip=True)
                    value = cells[1].get_text(strip=True)
                    if key and value:
                        specs[key] = value

        # Ищем списки dl/dt/dd
        dl = soup.find("dl")
        if dl:
            dts = dl.find_all("dt")
            dds = dl.find_all("dd")
            for dt, dd in zip(dts, dds):
                key = dt.get_text(strip=True)
                value = dd.get_text(strip=True)
                if key and value:
                    specs[key] = value

        return specs

    def parse_full_catalog(self, max_categories: int = None) -> list:
        """
        Парсит весь каталог gidroizol.ru.
        Возвращает список всех найденных товаров с характеристиками.
        """
        all_products = []
        categories = list(CATALOG_CATEGORIES.items())
        if max_categories:
            categories = categories[:max_categories]

        for cat_id, cat_name in categories:
            products = self.parse_category(cat_id, cat_name)
            for p in products:
                # Подгружаем детали товара (не для всех — слишком долго)
                if p.get("url") and len(all_products) < 200:
                    time.sleep(0.5)  # вежливая задержка
                    specs = self.parse_product_details(p["url"])
                    p["specs"] = specs
            all_products.extend(products)
            time.sleep(1)  # задержка между категориями

        logger.info(
            f"[GidroizolParser] Full catalog parsed: {len(all_products)} products total"
        )
        return all_products
