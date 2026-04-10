"""
Сервис подбора аналогов материалов для тендерных закупок.
Ищет аналоги в локальной БД и через Gemini AI с Google Search.
"""

import json
import logging
import re
from typing import Optional
from sqlalchemy.orm import Session

logger = logging.getLogger("LegalAI")


class AnalogService:
    """
    Сервис подбора аналогов.
    Источники: локальная БД products, Gemini AI + Google Search.
    """

    def __init__(self, ai_service, db_session_factory):
        self.ai_service = ai_service
        self.db_session_factory = db_session_factory
        logger.info("[AnalogService] Initialized.")

    # ─────────────────────────────────────────────────────────────────────────
    # ПОИСК В ЛОКАЛЬНОЙ БД
    # ─────────────────────────────────────────────────────────────────────────

    def search_local_db(
        self,
        query: str,
        category: Optional[str] = None,
        limit: int = 10
    ) -> list:
        """
        Полнотекстовый поиск по локальной БД товаров.
        Ищет по полям: title, description, category, material_type.

        Параметры:
          query    : строка поиска (название материала)
          category : фильтр по категории (опционально)
          limit    : максимальное количество результатов

        Возвращает список словарей с данными о товарах.
        """
        from sqlalchemy import text
        logger.info(f"[AnalogService] Local DB search: '{query}' | category={category}")

        # Разбиваем запрос на слова для поиска
        keywords = [w.strip().lower() for w in re.split(r"\s+", query) if len(w.strip()) > 2]
        if not keywords:
            return []

        results = []
        try:
            with self.db_session_factory() as session:
                # Строим SQL запрос с LIKE по всем ключевым словам
                conditions = []
                params = {}
                for i, kw in enumerate(keywords[:5]):  # максимум 5 слов
                    param_name = f"kw{i}"
                    like_pattern = f"%{kw}%"
                    conditions.append(
                        f"(LOWER(title) LIKE :{param_name} OR "
                        f"LOWER(description) LIKE :{param_name} OR "
                        f"LOWER(category) LIKE :{param_name} OR "
                        f"LOWER(material_type) LIKE :{param_name})"
                    )
                    params[param_name] = like_pattern

                where_clause = " OR ".join(conditions)
                if category:
                    where_clause = f"({where_clause}) AND LOWER(category) = :cat"
                    params["cat"] = category.lower()

                sql = text(
                    f"SELECT id, title, category, material_type, price, "
                    f"specs, url, description "
                    f"FROM products WHERE {where_clause} LIMIT :lim"
                )
                params["lim"] = limit

                rows = session.execute(sql, params).fetchall()

                for row in rows:
                    specs = {}
                    if row.specs:
                        try:
                            specs = json.loads(row.specs) if isinstance(row.specs, str) else row.specs
                        except Exception:
                            specs = {}

                    results.append({
                        "id": row.id,
                        "title": row.title,
                        "category": row.category,
                        "material_type": row.material_type,
                        "price": row.price,
                        "specs": specs,
                        "url": row.url,
                        "description": row.description,
                        "source": "local_db",
                    })

        except Exception as e:
            logger.error(f"[AnalogService] Local DB search error: {e}")

        logger.info(f"[AnalogService] Local DB: found {len(results)} results for '{query}'")
        return results

    # ─────────────────────────────────────────────────────────────────────────
    # ПОИСК ЧЕРЕЗ GEMINI AI + GOOGLE SEARCH
    # ─────────────────────────────────────────────────────────────────────────

    async def search_ai(
        self,
        query: str,
        requirements: Optional[str] = None,
        max_results: int = 5
    ) -> list:
        """
        Поиск аналогов через Gemini AI с Google Search Grounding.

        Параметры:
          query        : название искомого материала
          requirements : технические требования из ТЗ тендера (опционально)
          max_results  : максимальное количество аналогов

        Возвращает список словарей с найденными аналогами.
        """
        logger.info(f"[AnalogService] AI search: '{query}' | requirements={bool(requirements)}")

        prompt = f"""Ты эксперт по гидроизоляционным и кровельным материалам.

Задача: найти аналоги материала "{query}" которые можно поставить в тендере.

{"Требования из ТЗ тендера:" + chr(10) + requirements if requirements else ""}

Найди {max_results} лучших аналогов от российских производителей/поставщиков.
Используй поиск в интернете для нахождения актуальных характеристик и цен.

Для каждого аналога укажи:
1. Полное название (марка, производитель)
2. Основные технические характеристики (основа, состав, толщина, вес, ГОСТ/ТУ)
3. Примерную цену за единицу (руб/рулон или руб/кг или руб/л)
4. Ссылку на сайт производителя или магазина
5. Вывод: подходит ли как аналог и почему

Верни ответ СТРОГО в формате JSON (и только JSON, без пояснений):
{{
  "analogs": [
    {{
      "title": "Техноэласт ЭПП 4,0 (Технониколь)",
      "manufacturer": "Технониколь",
      "material_type": "рулонная гидроизоляция",
      "specs": {{
        "Основа": "полиэстер",
        "Толщина": "4,0 мм",
        "Вес рулона": "40 кг",
        "ГОСТ": "ТУ 5774-005-00289973-2004",
        "Длина рулона": "10 м",
        "Ширина": "1 м"
      }},
      "price": 3200,
      "price_unit": "руб/рулон",
      "url": "https://www.tn.ru/catalogue/...",
      "match_reason": "Аналог по основным характеристикам: полиэстеровая основа, аналогичная толщина",
      "match_score": 95
    }}
  ]
}}"""

        try:
            # Вызов Gemini AI (с Google Search Grounding)
            response_text = await self.ai_service.generate_with_search(prompt)
            if not response_text:
                logger.warning("[AnalogService] AI returned empty response")
                return []

            # Извлекаем JSON из ответа
            json_match = re.search(r'\{[\s\S]*\}', response_text)
            if not json_match:
                logger.warning(f"[AnalogService] No JSON in AI response: {response_text[:200]}")
                return []

            data = json.loads(json_match.group())
            analogs = data.get("analogs", [])

            # Нормализуем и добавляем источник
            result = []
            for a in analogs[:max_results]:
                result.append({
                    "id": None,
                    "title": a.get("title", ""),
                    "manufacturer": a.get("manufacturer", ""),
                    "category": a.get("material_type", ""),
                    "material_type": a.get("material_type", ""),
                    "price": a.get("price"),
                    "price_unit": a.get("price_unit", ""),
                    "specs": a.get("specs", {}),
                    "url": a.get("url", ""),
                    "description": a.get("match_reason", ""),
                    "match_score": a.get("match_score", 0),
                    "source": "ai_search",
                })

            logger.info(
                f"[AnalogService] AI found {len(result)} analogs for '{query}'"
            )
            return result

        except json.JSONDecodeError as e:
            logger.error(f"[AnalogService] JSON parse error: {e}")
            return []
        except Exception as e:
            logger.error(f"[AnalogService] AI search error: {e}")
            return []

    # ─────────────────────────────────────────────────────────────────────────
    # КОМБИНИРОВАННЫЙ ПОИСК
    # ─────────────────────────────────────────────────────────────────────────

    async def search_analogs(
        self,
        query: str,
        requirements: Optional[str] = None,
        use_ai: bool = True,
        limit: int = 10
    ) -> dict:
        """
        Главный метод: комбинированный поиск аналогов.

        Алгоритм:
          1. Поиск в локальной БД
          2. Если результатов мало (< 3) — поиск через AI
          3. Объединяем, дедуплицируем, возвращаем

        Возвращает словарь:
          {
            "query": "...",
            "local_results": [...],
            "ai_results": [...],
            "total": N
          }
        """
        logger.info(f"[AnalogService] Combined search: '{query}'")

        # 1. Локальный поиск
        local_results = self.search_local_db(query, limit=limit)

        # 2. AI поиск если нужно
        ai_results = []
        if use_ai and len(local_results) < 3:
            ai_results = await self.search_ai(
                query=query,
                requirements=requirements,
                max_results=5
            )

        return {
            "query": query,
            "local_results": local_results,
            "ai_results": ai_results,
            "total": len(local_results) + len(ai_results),
        }

    # ─────────────────────────────────────────────────────────────────────────
    # СОХРАНЕНИЕ AI-РЕЗУЛЬТАТОВ В БД
    # ─────────────────────────────────────────────────────────────────────────

    def save_ai_result_to_db(self, product: dict) -> Optional[int]:
        """
        Сохраняет найденный AI аналог в локальную БД для будущего использования.
        Возвращает ID созданной записи или None при ошибке.
        """
        from sqlalchemy import text
        import json as json_lib
        try:
            with self.db_session_factory() as session:
                specs_json = json_lib.dumps(product.get("specs", {}), ensure_ascii=False)
                result = session.execute(
                    text(
                        "INSERT INTO products (title, category, material_type, "
                        "price, specs, url, description) "
                        "VALUES (:title, :cat, :mat, :price, :specs, :url, :desc)"
                    ),
                    {
                        "title": product.get("title", ""),
                        "cat": product.get("category", product.get("material_type", "")),
                        "mat": product.get("material_type", ""),
                        "price": product.get("price"),
                        "specs": specs_json,
                        "url": product.get("url", ""),
                        "desc": product.get("description", ""),
                    }
                )
                session.commit()
                logger.info(f"[AnalogService] Saved AI result to DB: {product.get('title')}")
                return result.lastrowid
        except Exception as e:
            logger.error(f"[AnalogService] Error saving to DB: {e}")
            return None
