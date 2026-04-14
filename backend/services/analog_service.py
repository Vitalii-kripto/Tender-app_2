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
        self._ai_blocked_until: float = 0.0  # timestamp до которого AI заблокирован
        logger.info("[AnalogService] Initialized.")

    def _normalize_text(self, text: str) -> str:
        text = (text or "").lower().replace("\xa0", " ")
        text = re.sub(r"[^a-zа-я0-9.,+\-]+", " ", text, flags=re.IGNORECASE)
        text = re.sub(r"\s+", " ", text).strip()
        return text

    def _tokenize(self, text: str) -> list[str]:
        stopwords = {
            "и", "или", "для", "по", "на", "с", "из", "под", "над", "в",
            "материал", "товар", "требуется", "аналог", "серый", "черный",
            "поставка", "закупка", "техническое", "задание", "описание",
            "объекта", "документация", "характеристики", "требования"
        }
        tokens = []
        for token in self._normalize_text(text).split():
            if len(token) < 2:
                continue
            if token in stopwords:
                continue
            tokens.append(token)
        return tokens

    def _flatten_specs(self, specs: dict) -> str:
        if not specs:
            return ""
        parts = []
        for key, value in specs.items():
            parts.append(f"{key}: {value}")
        return " ; ".join(parts)

    def _extract_marks(self, text: str) -> set[str]:
        text = self._normalize_text(text)
        return set(re.findall(r"\b(тпп|ткп|хпп|хкп|эпп|экп|эмп)\b", text, flags=re.IGNORECASE))

    def _extract_first_number_after_label(self, text: str, patterns: list[str]) -> Optional[float]:
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                raw = match.group(1).replace(",", ".")
                try:
                    return float(raw)
                except Exception:
                    pass
        return None

    def _extract_requirements_numeric(self, query: str, requirements: str = "") -> dict:
        source = f"{query}\n{requirements}".replace(",", ".")
        return {
            "thickness": self._extract_first_number_after_label(source, [
                r"толщин[аы]?[^\d]{0,15}(\d+(?:\.\d+)?)",
                r"\b(\d+(?:\.\d+)?)\s*мм\b",
            ]),
            "mass": self._extract_first_number_after_label(source, [
                r"масс[аы]?[^\d]{0,15}(\d+(?:\.\d+)?)",
                r"\b(\d+(?:\.\d+)?)\s*кг/?м2\b",
                r"\b(\d+(?:\.\d+)?)\s*кг/?м²\b",
            ]),
            "flex": self._extract_first_number_after_label(source, [
                r"гибкост[ьи][^\-\d]{0,15}(-?\d+(?:\.\d+)?)",
                r"\b(-?\d+(?:\.\d+)?)\s*°?c\b",
            ]),
        }

    def _score_product(self, product: dict, query: str, requirements: str = "") -> tuple[int, list[str]]:
        title = self._normalize_text(product.get("title", ""))
        category = self._normalize_text(product.get("category", ""))
        material_type = self._normalize_text(product.get("material_type", ""))
        description = self._normalize_text(product.get("description", ""))
        specs_text = self._normalize_text(self._flatten_specs(product.get("specs") or {}))

        haystack_title = f"{title} {category} {material_type}".strip()
        haystack_full = f"{title} {category} {material_type} {description} {specs_text}".strip()

        query_tokens = self._tokenize(query)
        requirements_tokens = self._tokenize(requirements)
        all_tokens = []
        for token in query_tokens + requirements_tokens:
            if token not in all_tokens:
                all_tokens.append(token)

        score = 0
        reasons = []

        normalized_query = self._normalize_text(query)
        if normalized_query and normalized_query in title:
            score += 60
            reasons.append("точное вхождение запроса в название")

        title_hits = [token for token in all_tokens if token in haystack_title]
        full_hits = [token for token in all_tokens if token in haystack_full]

        if title_hits:
            score += min(40, len(title_hits) * 10)
            reasons.append(f"совпадения в названии: {', '.join(title_hits[:5])}")

        extra_full_hits = [token for token in full_hits if token not in title_hits]
        if extra_full_hits:
            score += min(20, len(extra_full_hits) * 4)
            reasons.append(f"совпадения в описании/характеристиках: {', '.join(extra_full_hits[:5])}")

        query_marks = self._extract_marks(query)
        product_marks = self._extract_marks(haystack_full)
        if query_marks:
            if query_marks & product_marks:
                score += 25
                reasons.append(f"совпадает марка: {', '.join(sorted(query_marks & product_marks))}")
            else:
                score -= 20
                reasons.append("марка не совпадает")

        numeric_req = self._extract_requirements_numeric(query, requirements)
        numeric_product_source = f"{title}\n{specs_text}\n{description}"

        product_thickness = self._extract_first_number_after_label(numeric_product_source, [
            r"толщин[аы]?[^\d]{0,15}(\d+(?:\.\d+)?)",
            r"\b(\d+(?:\.\d+)?)\s*мм\b",
        ])
        product_mass = self._extract_first_number_after_label(numeric_product_source, [
            r"масс[аы]?[^\d]{0,15}(\d+(?:\.\d+)?)",
            r"\b(\d+(?:\.\d+)?)\s*кг/?м2\b",
            r"\b(\d+(?:\.\d+)?)\s*кг/?м²\b",
        ])
        product_flex = self._extract_first_number_after_label(numeric_product_source, [
            r"гибкост[ьи][^\-\d]{0,15}(-?\d+(?:\.\d+)?)",
            r"\b(-?\d+(?:\.\d+)?)\s*°?c\b",
        ])

        if numeric_req["thickness"] is not None and product_thickness is not None:
            delta = abs(numeric_req["thickness"] - product_thickness)
            if delta <= 0.35:
                score += 18
                reasons.append("близкая толщина")
            elif delta <= 0.7:
                score += 8
                reasons.append("похожа толщина")
            else:
                score -= 8
                reasons.append("толщина отличается")

        if numeric_req["mass"] is not None and product_mass is not None:
            delta = abs(numeric_req["mass"] - product_mass)
            if delta <= 0.5:
                score += 10
                reasons.append("близкая масса")
            elif delta <= 1.0:
                score += 5
                reasons.append("похожа масса")

        if numeric_req["flex"] is not None and product_flex is not None:
            delta = abs(numeric_req["flex"] - product_flex)
            if delta <= 5:
                score += 12
                reasons.append("близкая гибкость")
            elif delta <= 10:
                score += 5
                reasons.append("похожа гибкость")

        if score < 0:
            score = 0

        return score, reasons

    def _normalize_title_for_dedup(self, title: str) -> str:
        if not title:
            return ""
        return re.sub(r"\s+", " ", re.sub(r"[^a-zA-Zа-яА-Я0-9]+", " ", title.lower())).strip()

    # ─────────────────────────────────────────────────────────────────────────
    # ПОИСК В ЛОКАЛЬНОЙ БД
    # ─────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _clean_search_query(raw_query: str) -> str:
        """
        Очищает поисковый запрос от мусора из парсинга документов.

        Удаляет:
          - единицы измерения в скобках: (штука), (м2), (кг), (рул)
          - метки отсутствия данных: (не указано)*, / (не указано)*
          - служебные символы парсера: *, /
          - пайп-формат: "Ключ | Значение | источник.docx, стр.N |"
          - технические суффиксы: ГОСТ XXXXX, ТУ XXXXX
        """
        import re

        if not raw_query:
            return ""

        query = raw_query.strip()

        # ШАБЛОН 1: Пайп-формат "Поле | Значение | файл.docx, стр.N |"
        if "|" in query:
            parts = [p.strip() for p in query.split("|")]
            FIELD_NAMES = {
                "предмет закупки", "наименование", "описание",
                "объект закупки", "товар", "номенклатура",
                "позиция", "материал", "продукт", "изделие",
            }
            FILE_PATTERN = re.compile(
                r"\.(docx|pdf|xlsx?|doc)\b|стр\.?\s*\d+",
                re.IGNORECASE
            )
            good_parts = [
                p for p in parts
                if p
                and p.lower() not in FIELD_NAMES
                and not FILE_PATTERN.search(p)
                and len(p) > 3
            ]
            if good_parts:
                query = max(good_parts, key=len)

        # ШАБЛОН 2: Единицы измерения в скобках — (штука), (м2), (кг) и т.д.
        query = re.sub(
            r"\(\s*(штука|штук|шт|м2|м\.кв|кв\.м|м|рулон|рул|кг|г|л|литр|"
            r"упак|уп|комплект|компл|набор|пара|п\.м|пм|погонный метр)\s*\)",
            "",
            query,
            flags=re.IGNORECASE
        )

        # ШАБЛОН 3: Метки отсутствия данных
        query = re.sub(
            r"/?\s*\(\s*не\s+указано\s*\)\s*\*?",
            "",
            query,
            flags=re.IGNORECASE
        )
        query = re.sub(r"\(\s*не\s+указано\s*\)", "", query, flags=re.IGNORECASE)
        query = re.sub(r"\*", "", query)

        # ШАБЛОН 4: Слэш в конце или начале
        query = query.strip("/").strip()

        # ШАБЛОН 5: ГОСТ и ТУ с номерами
        query = re.sub(r"\s+ГОСТ\s+[\d\-]+", "", query, flags=re.IGNORECASE)
        query = re.sub(r"\s+ТУ\s+[\d\.\-]+", "", query, flags=re.IGNORECASE)

        # ШАБЛОН 6: Количество + единица в скобках "10 кг" "(20 л)"
        query = re.sub(
            r"\s*\(\s*\d+[\.,]?\d*\s*(кг|г|л|м2|м|шт|рул)\s*\)",
            "",
            query,
            flags=re.IGNORECASE
        )

        # Финальная очистка пробелов
        query = re.sub(r"\s{2,}", " ", query).strip().strip(",").strip("/").strip()

        import logging
        logger = logging.getLogger("LegalAI")
        logger.info(
            f"[AnalogService] Query cleaned: '{raw_query[:80]}' -> '{query}'"
        )
        return query if query else raw_query.strip()

    def search_local_db(
        self,
        query: str,
        category: str = None,
        limit: int = 10,
        requirements: str = None,
    ) -> list:
        """
        Поиск аналогов в локальной БД с предварительной очисткой запроса.
        Поддерживает поиск по нескольким ключевым словам одновременно.
        """
        import re
        from sqlalchemy import text
        import json as json_lib

        clean_query = self._clean_search_query(query)
        if not clean_query:
            logger.warning(f"[AnalogService] Empty query after cleaning: '{query}'")
            return []

        logger.info(
            f"[AnalogService] Local DB search: "
            f"raw='{query[:60]}' -> clean='{clean_query}' | category={category}"
        )

        raw_keywords = re.split(r"[\s,;/\(\)]+", clean_query)

        STOP_WORDS = {
            "не", "и", "или", "для", "на", "в", "с", "по", "из",
            "указано", "штука", "штук", "шт", "кг", "м2", "рул",
            "рулон", "упак", "литр", "единица", "ед", "марка",
            "тип", "вид", "класс", "материал", "товар", "продукт",
            "поставка", "закупка", "техническое", "задание", "описание",
            "объекта", "документация", "характеристики", "требования"
        }

        keywords = [
            kw.strip().lower()
            for kw in raw_keywords
            if len(kw.strip()) > 2
            and kw.strip().lower() not in STOP_WORDS
            and not kw.strip().isdigit()
            and kw.strip() not in ("*", "/", "|", "(", ")", "-")
        ][:5]

        if not keywords:
            logger.warning(
                f"[AnalogService] No valid keywords after filtering: "
                f"raw='{query}' clean='{clean_query}'"
            )
            return []

        logger.info(
            f"[AnalogService] Search keywords: {keywords} "
            f"(from clean query: '{clean_query}')"
        )

        results = []
        try:
            with self.db_session_factory() as session:
                conditions = []
                params = {}
                for i, kw in enumerate(keywords):
                    pname = f"kw{i}"
                    like = f"%{kw}%"
                    conditions.append(
                        f"(LOWER(title) LIKE :{pname} "
                        f"OR LOWER(description) LIKE :{pname} "
                        f"OR LOWER(category) LIKE :{pname})"
                    )
                    params[pname] = like

                where = " OR ".join(conditions)

                if category:
                    where = f"({where}) AND LOWER(category) LIKE :user_cat"
                    params["user_cat"] = f"%{category.lower()}%"

                params["lim"] = limit * 3

                sql = text(
                    f"SELECT id, title, category, material_type, price, "
                    f"specs, url, description "
                    f"FROM products WHERE {where} LIMIT :lim"
                )
                rows = session.execute(sql, params).fetchall()

                for row in rows:
                    specs = {}
                    if row.specs:
                        try:
                            specs = (
                                json_lib.loads(row.specs)
                                if isinstance(row.specs, str)
                                else row.specs
                            )
                        except Exception:
                            specs = {}

                    title_lower = (row.title or "").lower()
                    desc_lower = (row.description or "").lower()

                    score = sum(1 for kw in keywords if kw in title_lower) * 3
                    score += sum(1 for kw in keywords if kw in desc_lower)

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
                        "_score": score,
                    })

                results.sort(key=lambda x: x.get("_score", 0), reverse=True)
                for r in results:
                    r.pop("_score", None)
                results = results[:limit]

        except Exception as e:
            logger.error(f"[AnalogService] Local DB search error: {e}")

        logger.info(
            f"[AnalogService] Local DB: found {len(results)} results "
            f"for '{clean_query}'"
        )
        return results

    # ─────────────────────────────────────────────────────────────────────────
    # ПОИСК ЧЕРЕЗ GEMINI AI + GOOGLE SEARCH
    # ─────────────────────────────────────────────────────────────────────────

    async def search_ai(
        self,
        query: str,
        requirements: str = None,
        max_results: int = 5,
        local_db_products: list = None
    ) -> tuple[list, str]:
        import time
        # Проверяем флаг блокировки AI (устанавливается при 429)
        blocked_until = getattr(self, '_ai_blocked_until', 0)
        if time.time() < blocked_until:
            remaining = int(blocked_until - time.time()) // 60
            logger.info(
                f"[AnalogService] AI is blocked for {remaining} more min "
                f"(quota exhausted). Skipping AI search."
            )
            return [], "QUOTA_EXHAUSTED"

        logger.info(f"[AnalogService] AI search: '{query}' | requirements={bool(requirements)}")

        if not self.ai_service:
            logger.error(f"[AnalogService] AI service is not initialized")
            return [], "AI service is not initialized"

        # Очищаем запрос перед отправкой в AI
        clean_q = self._clean_search_query(query)

        # Формируем список товаров из локальной БД для AI
        local_items_text = ""
        if local_db_products:
            lines = []
            for p in local_db_products[:10]:
                specs_str = ", ".join(
                    f"{k}: {v}"
                    for k, v in (p.get("specs") or {}).items()
                    if k and v
                )[:200]
                lines.append(
                    f"- {p['title']} | {p.get('category','')} | "
                    f"Цена: {p.get('price','н/д')} руб | {specs_str}"
                )
            local_items_text = "\n".join(lines)

        prompt = f"""Ты эксперт по материально-техническому снабжению и подбору аналогов промышленной продукции для тендерных закупок.

ЗАДАЧА: Подобрать аналоги для товара/материала: "{clean_q}"

{"ТЕХНИЧЕСКИЕ ТРЕБОВАНИЯ ИЗ ТЗ ТЕНДЕРА:" + chr(10) + requirements[:800] if requirements else ""}

{"УЖЕ ЕСТЬ В НАШЕМ КАТАЛОГЕ:" + chr(10) + local_items_text if local_items_text else ""}

ИНСТРУКЦИЯ:
1. Если в нашем каталоге уже есть подходящий аналог — укажи его первым со score >= 85
2. Найди ещё {max_results} аналогов от других производителей
3. Для каждого аналога укажи конкретные технические характеристики
4. Проверяй соответствие требованиям ТЗ если они указаны
5. В поле match_reason объясни конкретно почему это аналог

ВАЖНО: Верни ТОЛЬКО JSON без пояснений и markdown-блоков.

Формат ответа:
{{
  "analogs": [
    {{
      "title": "Название аналога",
      "manufacturer": "Производитель",
      "material_type": "Тип/Категория товара",
      "specs": {{
        "Характеристика 1": "Значение 1",
        "Характеристика 2": "Значение 2"
      }},
      "price": 100,
      "price_unit": "руб/ед",
      "url": "ссылка на товар",
      "match_reason": "Почему это аналог",
      "match_score": 90,
      "in_local_db": true
    }}
  ]
}}"""

        try:
            response_text = self.ai_service.generate_with_search(prompt)
            if not response_text:
                logger.warning(
                    f"[AnalogService] AI returned empty response."
                )
                return [], "Empty response"
        except Exception as e:
            error_str = str(e)
            is_quota = "429" in error_str or "RESOURCE_EXHAUSTED" in error_str or "QUOTA_EXHAUSTED" in error_str
            is_unavailable = "недоступен" in error_str or "временно" in error_str or "SERVICE_UNAVAILABLE" in error_str

            if is_quota or is_unavailable:
                logger.warning(
                    f"[AnalogService] AI quota/unavailable: {error_str[:100]}. "
                    f"AI search skipped — returning local DB results only."
                )
                # Устанавливаем флаг блокировки AI на 1 час
                import time
                self._ai_blocked_until = time.time() + 3600
                logger.warning(
                    f"[AnalogService] AI blocked for 1 hour "
                    f"(until {time.strftime('%H:%M', time.localtime(self._ai_blocked_until))})"
                )
                return [], "QUOTA_EXHAUSTED" if is_quota else "SERVICE_UNAVAILABLE"
            else:
                logger.error(f"[AnalogService] AI search error: {e}")
                return [], str(e)

        try:
            json_match = re.search(r'\{[\s\S]*\}', response_text)
            if not json_match:
                logger.warning(f"[AnalogService] No JSON found in AI response: {response_text[:500]}")
                return [], "AI не вернул JSON"

            data = json.loads(json_match.group())
            analogs = data.get("analogs", [])

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

            logger.info(f"[AnalogService] AI found {len(result)} analogs for '{query}'")
            return result, ""

        except json.JSONDecodeError as e:
            logger.error(f"[AnalogService] JSON parse error: {e}", exc_info=True)
            return [], f"AI вернул невалидный JSON: {e}"
        except Exception as e:
            logger.error(f"[AnalogService] AI search error: {e}", exc_info=True)
            return [], str(e)

    # ─────────────────────────────────────────────────────────────────────────
    # КОМБИНИРОВАННЫЙ ПОИСК
    # ─────────────────────────────────────────────────────────────────────────

    async def search_analogs(
        self,
        query: str,
        requirements: str = None,
        use_ai: bool = True,
        limit: int = 10
    ) -> dict:
        """
        Главный метод: комбинированный поиск аналогов.

        Алгоритм:
          1. Очищаем запрос
          2. Ищем в локальной БД
          3. Если AI включён — передаём контекст из БД в AI промпт
          4. AI ищет дополнительные аналоги с учётом того что уже есть в БД
          5. Объединяем результаты, убираем дубли

        Возвращает:
          {
            "query": "очищенный запрос",
            "original_query": "исходный запрос",
            "local_results": [...],
            "ai_results": [...],
            "total": N,
            "ai_error": "..."
          }
        """
        # Очищаем запрос
        clean_query = self._clean_search_query(query)
        logger.info(
            f"[AnalogService] Combined search: "
            f"raw='{query[:60]}' → clean='{clean_query}' | "
            f"mode={'ai+db' if use_ai else 'db_only'} | limit={limit}"
        )

        # Поиск в локальной БД
        local_results = self.search_local_db(clean_query, limit=limit)

        # AI поиск с передачей контекста из БД
        ai_results = []
        ai_error = ""
        if use_ai:
            # Передаём в AI что уже есть в БД — чтобы он не дублировал
            ai_results, ai_error = await self.search_ai(
                query=clean_query,
                requirements=requirements,
                max_results=5,
                local_db_products=local_results,
            )

        # Убираем дубли между local и ai результатами
        seen_titles = {r["title"].lower() for r in local_results}
        ai_unique = []
        for r in ai_results:
            t = r.get("title", "").lower()
            # Проверяем нет ли похожего в local (по первым 15 символам)
            is_dup = any(
                t[:15] in existing or existing[:15] in t
                for existing in seen_titles
            )
            if not is_dup:
                ai_unique.append(r)
                seen_titles.add(t)

        return {
            "query": clean_query,
            "original_query": query,
            "local_results": local_results,
            "ai_results": ai_unique,
            "total": len(local_results) + len(ai_unique),
            "ai_error": ai_error
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
