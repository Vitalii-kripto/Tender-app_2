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
        self.last_ai_error = ""
        logger.info("[AnalogService] Initialized.")

    def _normalize_text(self, text: str) -> str:
        text = (text or "").lower().replace("\xa0", " ")
        text = re.sub(r"[^a-zа-я0-9.,+\-]+", " ", text, flags=re.IGNORECASE)
        text = re.sub(r"\s+", " ", text).strip()
        return text

    def _tokenize(self, text: str) -> list[str]:
        stopwords = {
            "и", "или", "для", "по", "на", "с", "из", "под", "над", "в",
            "материал", "товар", "требуется", "аналог", "серый", "черный"
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

    def search_local_db(
        self,
        query: str,
        category: Optional[str] = None,
        requirements: Optional[str] = None,
        limit: int = 10
    ) -> list:
        """
        Улучшенный подбор по локальной БД.
        Не просто LIKE по словам, а ранжирование по названию, марке и характеристикам.
        """
        from sqlalchemy import text

        logger.info(f"[AnalogService] Local DB search: '{query}' | category={category}")

        if not query.strip():
            return []

        results = []
        try:
            with self.db_session_factory() as session:
                rows = session.execute(
                    text(
                        "SELECT id, title, category, material_type, price, specs, url, description "
                        "FROM products"
                    )
                ).fetchall()

                candidates = []
                for row in rows:
                    specs = {}
                    if row.specs:
                        try:
                            specs = json.loads(row.specs) if isinstance(row.specs, str) else row.specs
                        except Exception:
                            specs = {}

                    product = {
                        "id": row.id,
                        "title": row.title,
                        "category": row.category,
                        "material_type": row.material_type,
                        "price": row.price,
                        "specs": specs,
                        "url": row.url,
                        "description": row.description,
                        "source": "local_db",
                    }

                    if category and str(product.get("category") or "").lower() != str(category).lower():
                        continue

                    score, reasons = self._score_product(product, query, requirements or "")
                    if score <= 0:
                        continue

                    product["match_score"] = min(score, 100)
                    product["description"] = (
                        f"Подбор по базе. Причины: {', '.join(reasons[:4])}"
                        if reasons else
                        "Подбор по базе по совпадению названия и характеристик."
                    )
                    candidates.append(product)

                candidates.sort(
                    key=lambda item: (
                        item.get("match_score", 0),
                        1 if item.get("title", "").lower() == query.lower() else 0,
                        float(item.get("price") or 0)
                    ),
                    reverse=True
                )

                seen = set()
                for item in candidates:
                    key = self._normalize_title_for_dedup(item.get("title", ""))
                    if key in seen:
                        continue
                    seen.add(key)
                    results.append(item)
                    if len(results) >= limit:
                        break

        except Exception as e:
            logger.error(f"[AnalogService] Local DB search error: {e}", exc_info=True)

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
        logger.info(f"[AnalogService] AI search: '{query}' | requirements={bool(requirements)}")
        self.last_ai_error = ""

        if not self.ai_service:
            self.last_ai_error = "AI service is not initialized"
            logger.error(f"[AnalogService] {self.last_ai_error}")
            return []

        prompt = f"""Ты эксперт по гидроизоляционным и кровельным материалам.

Задача: найти аналоги материала "{query}", которые реально продаются в РФ и могут быть предложены как аналог.

{"Требования из ТЗ тендера:\n" + requirements if requirements else ""}

Найди до {max_results} лучших аналогов.
Используй поиск в интернете для нахождения актуальных характеристик и цен.

Верни ответ строго в JSON и только в JSON:
{{
  "analogs": [
    {{
      "title": "Полное название",
      "manufacturer": "Производитель",
      "material_type": "Тип материала",
      "specs": {{
        "Основа": "значение",
        "Толщина": "значение",
        "Масса": "значение",
        "Гибкость": "значение"
      }},
      "price": 0,
      "price_unit": "руб/рулон",
      "url": "https://...",
      "match_reason": "Почему подходит",
      "match_score": 0
    }}
  ]
}}"""

        try:
            response_text = self.ai_service.generate_with_search(prompt)

            if not response_text:
                provider_error = getattr(self.ai_service, "last_error_message", "") or "Пустой ответ от AI"
                self.last_ai_error = provider_error
                logger.warning(f"[AnalogService] AI returned empty response. Reason: {provider_error}")
                return []

            json_match = re.search(r'\{[\s\S]*\}', response_text)
            if not json_match:
                self.last_ai_error = "AI не вернул JSON"
                logger.warning(f"[AnalogService] No JSON found in AI response: {response_text[:500]}")
                return []

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

            self.last_ai_error = ""
            logger.info(f"[AnalogService] AI found {len(result)} analogs for '{query}'")
            return result

        except json.JSONDecodeError as e:
            self.last_ai_error = f"AI вернул невалидный JSON: {e}"
            logger.error(f"[AnalogService] JSON parse error: {e}", exc_info=True)
            return []
        except Exception as e:
            self.last_ai_error = str(e)
            logger.error(f"[AnalogService] AI search error: {e}", exc_info=True)
            return []

    # ─────────────────────────────────────────────────────────────────────────
    # КОМБИНИРОВАННЫЙ ПОИСК
    # ─────────────────────────────────────────────────────────────────────────

    async def search_analogs(
        self,
        query: str,
        requirements: Optional[str] = None,
        mode: str = "both",
        limit: int = 10
    ) -> dict:
        normalized_mode = (mode or "both").strip().lower()
        if normalized_mode not in {"local", "ai", "both"}:
            normalized_mode = "both"

        logger.info(
            f"[AnalogService] Combined search: query='{query}' | mode={normalized_mode} | limit={limit}"
        )

        self.last_ai_error = ""
        local_results = []
        ai_results = []

        if normalized_mode in {"local", "both"}:
            local_results = self.search_local_db(
                query=query,
                requirements=requirements,
                limit=limit
            )

        if normalized_mode in {"ai", "both"}:
            ai_results = await self.search_ai(
                query=query,
                requirements=requirements,
                max_results=min(limit, 10)
            )

        if normalized_mode == "both" and local_results and ai_results:
            local_titles = {
                self._normalize_title_for_dedup(item.get("title", ""))
                for item in local_results
            }
            filtered_ai_results = []
            for item in ai_results:
                norm_title = self._normalize_title_for_dedup(item.get("title", ""))
                if norm_title and norm_title not in local_titles:
                    filtered_ai_results.append(item)
            ai_results = filtered_ai_results

        if normalized_mode == "local":
            ai_results = []

        if normalized_mode == "ai":
            local_results = []

        return {
            "query": query,
            "mode": normalized_mode,
            "local_results": local_results,
            "ai_results": ai_results,
            "total": len(local_results) + len(ai_results),
            "ai_error": self.last_ai_error or getattr(self.ai_service, "last_error_message", ""),
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
