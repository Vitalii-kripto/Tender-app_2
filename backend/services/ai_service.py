import os
import time
from typing import List, Dict, Any, Optional
from fastapi import HTTPException
from google import genai
from google.genai import types
from dotenv import load_dotenv
import json
import re
import logging
import hashlib
from backend.config import GEMINI_MODEL, GEMINI_FALLBACK_MODEL
from backend.logger import logger

# --- LOGGING SETUP ---
# Загружаем переменные окружения (.env)
env_loaded = load_dotenv()

env_debug_val = os.getenv("LEGAL_AI_DEBUG", "false")
DEBUG_MODE = env_debug_val.lower() == "true"

class AiService:
    """
    Сервис для работы с Google Gemini API.
    Выполняет анализ рисков, подбор аналогов и проверку соответствия.
    Используется модель, указанная в GEMINI_MODEL.
    """
    def __init__(self):
        self.api_key = os.getenv("API_KEY")
        self.model_name = GEMINI_MODEL
        self.fallback_model_name = GEMINI_FALLBACK_MODEL
        self.active_model = self.model_name
        self.last_error_code = ""
        self.last_error_message = ""
        
        if not self.api_key:
            logger.warning("API_KEY not found in environment variables. AI analysis will be unavailable.")
            self.client = None
        else:
            try:
                self.client = genai.Client(api_key=self.api_key)
                logger.info(f"Gemini Client initialized. Primary: {self.model_name}, Fallback: {self.fallback_model_name}")
            except Exception as e:
                logger.error(f"Failed to initialize Gemini Client: {e}")
                self.client = None

        # Rate limiter: минимальный интервал между запросами = 5 сек
        self._last_ai_call_time = 0.0
        self._ai_call_min_interval = 5.0  # секунд между запросами
        # Простой in-memory кэш результатов (ключ → (timestamp, result))
        self._search_cache: dict = {}
        self._cache_ttl = 3600  # кэш живёт 1 час

    def test_model_availability(self) -> str:
        """
        Тестовый запрос к моделям при старте для выбора рабочей модели.
        Возвращает имя выбранной модели или пустую строку, если ни одна не доступна.
        """
        logger.info(f"AI Startup Check: Primary model: {self.model_name}, Fallback model: {self.fallback_model_name}")
        
        if not self.client:
            logger.error("Startup check: Client not initialized (no API_KEY).")
            return ""

        test_prompt = "Hello, this is a startup connectivity test. Reply with 'OK'."
        
        # 1. Тест основной модели
        logger.info(f"Startup check: Testing primary model {self.model_name}...")
        try:
            response = self.client.models.generate_content(
                model=self.model_name,
                contents=test_prompt
            )
            if response and response.text:
                logger.info(f"Startup check: Primary model {self.model_name} is ONLINE. Selected as active.")
                self.active_model = self.model_name
                # Логируем статус fallback как пропущено, чтобы выполнить требование о наличии лога по каждой модели
                logger.info(f"Startup check: Fallback model {self.fallback_model_name} - SKIPPED (Primary is OK).")
                return self.active_model
        except Exception as e:
            logger.warning(f"Startup check: Primary model {self.model_name} is OFFLINE: {e}")

        # 2. Тест fallback модели
        logger.info(f"Startup check: Testing fallback model {self.fallback_model_name}...")
        try:
            response = self.client.models.generate_content(
                model=self.fallback_model_name,
                contents=test_prompt
            )
            if response and response.text:
                logger.info(f"Startup check: Fallback model {self.fallback_model_name} is ONLINE. Selected as active.")
                self.active_model = self.fallback_model_name
                return self.active_model
        except Exception as e:
            logger.error(f"Startup check: Fallback model {self.fallback_model_name} is also OFFLINE: {e}")

        logger.critical("Startup check: CRITICAL - No AI models are available! Analysis will be blocked.")
        self.active_model = ""
        return ""

    def _is_transient_error(self, error: Exception) -> bool:
        """Определяет, является ли ошибка временной (429, 503, 504, timeout)."""
        err_str = str(error).lower()
        # Коды ошибок и типичные сообщения
        transient_indicators = [
            "429", "too many requests", 
            "503", "service unavailable", 
            "504", "gateway timeout",
            "deadline exceeded", "timeout",
            "transport error", "connection reset", "socket error"
        ]
        return any(indicator in err_str for indicator in transient_indicators)

    def _call_ai_with_retry(self, method, **kwargs):
        """
        Унифицированный вызов ИИ с логированием, повторами и переключением на fallback.
        ВАЖНО:
        - при 429 на primary выполняется немедленный переход на fallback без ожидания;
        - если 429 уже на fallback, вызов завершается с ошибкой;
        - при успешном ответе с fallback она становится active_model для следующих вызовов.
        """
        if not self.client:
            raise Exception("Gemini Client not initialized")

        if not self.active_model:
            raise Exception("Внешний AI-сервис временно недоступен, анализ не завершен")

        max_retries = 4
        kwargs["model"] = kwargs.get("model") or self.active_model
        quota_switched = False

        for attempt in range(1, max_retries + 1):
            attempt_start = time.time()

            try:
                logger.info(f"AI Call Attempt {attempt}/{max_retries} | Model: {kwargs['model']}")
                response = method(**kwargs)

                duration = time.time() - attempt_start
                logger.info(f"AI Call Success | Attempt: {attempt} | Duration: {duration:.2f}s")

                if kwargs["model"] != self.active_model:
                    logger.warning(
                        f"Dynamic model switch: {self.active_model} -> {kwargs['model']} (Success after retry)"
                    )
                    self.active_model = kwargs["model"]

                return response

            except Exception as e:
                duration = time.time() - attempt_start
                error_text = str(e)
                lower_error = error_text.lower()

                is_transient = self._is_transient_error(e)
                is_quota = (
                    "429" in error_text
                    or "resource_exhausted" in lower_error
                    or "quota" in lower_error
                )

                error_type = "TRANSIENT" if is_transient else "FATAL"
                logger.warning(
                    f"AI Call Failed ({error_type}) | Attempt: {attempt} | "
                    f"Model: {kwargs['model']} | Duration: {duration:.2f}s | Error: {e}"
                )

                if is_quota:
                    if (
                        kwargs["model"] != self.fallback_model_name
                        and self.fallback_model_name
                    ):
                        logger.warning(
                            f"[AIService] Quota exhausted on primary model {kwargs['model']}. "
                            f"Immediate switch to fallback model: {self.fallback_model_name}"
                        )
                        kwargs["model"] = self.fallback_model_name
                        quota_switched = True
                        continue

                    logger.error(
                        f"[AIService] Quota exhausted on model {kwargs['model']}. "
                        f"Fallback already used or unavailable. Stopping AI call."
                    )
                    raise HTTPException(status_code=429, detail="QUOTA_EXHAUSTED")

                if not is_transient or attempt == max_retries:
                    if is_transient:
                        logger.error(f"AI Call: All {max_retries} attempts exhausted for transient errors.")
                    raise HTTPException(status_code=503, detail="SERVICE_UNAVAILABLE")

                if (
                    attempt == max_retries // 2
                    and kwargs["model"] == self.model_name
                    and self.fallback_model_name
                    and not quota_switched
                ):
                    logger.warning(
                        f"Attempt {attempt} failed. Switching to fallback model for remaining retries: "
                        f"{self.fallback_model_name}"
                    )
                    kwargs["model"] = self.fallback_model_name

                wait_time = [3, 5, 9][min(attempt, 2)]
                logger.info(f"Waiting {wait_time}s before next retry...")
                time.sleep(wait_time)

        return None

    def _normalize_requirement_text(self, text: str) -> str:
        text = (text or "").replace("\xa0", " ")
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    def _remove_source_artifacts(self, text: str) -> str:
        text = self._normalize_requirement_text(text)
        if not text:
            return ""

        # Убираем служебные метки файлов/страниц, если они попали в текст.
        text = re.sub(r"\[FILE:[^\]]+\]", " ", text, flags=re.IGNORECASE)
        text = re.sub(r"\[CRM_DESCRIPTION_FALLBACK\]", " ", text, flags=re.IGNORECASE)
        text = re.sub(r"===\s*ФАЙЛ:[^\n]+", " ", text, flags=re.IGNORECASE)
        text = re.sub(r"---\s*СТРАНИЦА/ЛИСТ:[^\n]+", " ", text, flags=re.IGNORECASE)
        text = re.sub(r"\.(docx?|pdf|xlsx?|xls)\b", " ", text, flags=re.IGNORECASE)
        text = re.sub(r"\bстр\.?\s*\d+\b", " ", text, flags=re.IGNORECASE)
        text = re.sub(r"\s+\|\s+", " | ", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()


    def _is_noise_line_for_requirements(self, line: str) -> bool:
        low = (line or "").lower().strip()
        if not low:
            return True

        # Generic patterns for products/specifications
        product_like = (
            any(term in low for term in [
                "гост", "ту ", "ост ", "характеристик", "параметр", "значение",
                "толщин", "масса", "вес ", "длина", "ширина", "высота", "диаметр",
                "марка", "модель", "артикул", "тип ", "состав", "свойства",
                "наименование", "материал", "товар", "количеств", "ед. изм", "ед изм"
            ])
            or bool(re.search(r"\b\d+(?:[.,]\d+)?\s*(мм|м2|м²|м|кг|л|шт|рулон|компл|уп|т|гр|см)\b", low))
        )

        if product_like:
            return False

        noise_terms = [
            "оплата", "штраф", "пеня", "неустойк", "расторжен", "ответственность",
            "реквизит", "участник закупки", "инструкция", "образцы форм", "критерии оценки",
            "нмцд", "нмцк", "обоснован", "нац режим", "реестр", "банковская гарантия",
            "обеспечение исполнения", "порядок расчетов", "срок оплаты", "извещение",
            "подача заявок", "комиссия", "жалоб", "протокол", "контактн"
        ]

        if any(term in low for term in noise_terms) and not product_like:
            return True

        if re.search(r"\.(docx?|pdf|xlsx?|xls)\b", low, flags=re.IGNORECASE):
            return True
        if re.search(r"\bстр\.?\s*\d+\b", low, flags=re.IGNORECASE):
            return True

        return False


    def _split_text_into_semantic_blocks(self, text: str) -> list[str]:
        text = self._remove_source_artifacts(text)
        if not text:
            return []

        raw_blocks = re.split(r"\n\s*\n+", text)
        blocks: list[str] = []

        for raw in raw_blocks:
            raw = raw.strip()
            if not raw:
                continue

            # Сохраняем таблицы и пайп-строки как отдельные блоки.
            if "|" in raw and raw.count("|") >= 2:
                blocks.append(raw)
                continue

            if len(raw) <= 5000:
                blocks.append(raw)
                continue

            # Если блок слишком большой, дробим по строкам.
            lines = [line.strip() for line in raw.splitlines() if line.strip()]
            chunk: list[str] = []
            current_len = 0

            for line in lines:
                if current_len + len(line) > 2500 and chunk:
                    blocks.append("\n".join(chunk).strip())
                    chunk = []
                    current_len = 0

                chunk.append(line)
                current_len += len(line) + 1

            if chunk:
                blocks.append("\n".join(chunk).strip())

        return blocks


    def _score_requirement_block(self, block: str) -> int:
        low = (block or "").lower().strip()
        if not low:
            return 0

        score = 0

        positive_terms = [
            "техническое задание", "описание объекта закупки", "спецификац",
            "ведомость материалов", "наименование товара", "характерист",
            "материал", "товар", "поставка", "основа", "толщин",
            "масса", "параметр", "значение", "требован", "гост", "ту ", "ост "
        ]
        negative_terms = [
            "оплата", "штраф", "пеня", "неустойк", "расторжен", "ответственность",
            "реквизит", "участник закупки", "инструкция", "заявка", "критерии оценки",
            "нмцд", "нмцк", "обоснован", "нац режим", "реестр", "банковская гарантия",
            "обеспечение исполнения", "порядок расчетов", "срок оплаты"
        ]

        if any(term in low for term in positive_terms):
            score += 4
        
        # Generic patterns for quantities and units
        if re.search(r"\b\d+(?:[.,]\d+)?\s*(мм|м2|м²|м|кг|л|шт|рулон|компл|уп|т|гр|см)\b", low, flags=re.IGNORECASE):
            score += 3
            
        # Check for table-like structure or list of characteristics
        if ":" in low and any(term in low for term in ["характерист", "параметр", "значение"]):
            score += 3

        score -= sum(1 for term in negative_terms if term in low) * 2

        if "|" in low and re.search(r"\.(docx?|pdf|xlsx?|xls)\b", low, flags=re.IGNORECASE):
            score -= 4

        if len(low) < 20:
            score -= 2

        return score


    def _select_requirement_relevant_blocks(self, text: str, max_chars: int = 55000) -> str:
        blocks = self._split_text_into_semantic_blocks(text)
        if not blocks:
            return self._remove_source_artifacts(text)[:max_chars]

        scored_blocks: list[tuple[int, str]] = []
        for block in blocks:
            score = self._score_requirement_block(block)
            if score >= 3:
                scored_blocks.append((score, block))

        if not scored_blocks:
            # Если автоматический фильтр не нашел хороших блоков,
            # возвращаем нормализованный текст, но без source-artifacts.
            return self._remove_source_artifacts(text)[:max_chars]

        scored_blocks.sort(key=lambda item: item[0], reverse=True)

        selected: list[str] = []
        total_len = 0
        for _, block in scored_blocks:
            if total_len + len(block) > max_chars:
                break
            selected.append(block)
            total_len += len(block)

        return "\n\n".join(selected).strip()


    def _split_lines_for_requirements(self, text: str) -> list[str]:
        text = self._remove_source_artifacts(text)
        lines: list[str] = []

        for raw_line in text.splitlines():
            line = raw_line.strip(" \t\r\n-–—•*")
            line = self._normalize_requirement_text(line)

            if not line:
                continue

            # Если строка табличная/pipe-строка, берем наиболее осмысленную ячейку,
            # а не всю строку целиком.
            if "|" in line:
                cells = [cell.strip(" \t\r\n-–—•*") for cell in line.split("|")]
                good_cells = []
                for cell in cells:
                    cell = self._normalize_requirement_text(cell)
                    if not cell:
                        continue
                    if self._is_noise_line_for_requirements(cell):
                        continue
                    if re.search(r"\.(docx?|pdf|xlsx?|xls)\b", cell, flags=re.IGNORECASE):
                        continue
                    if re.search(r"\bстр\.?\s*\d+\b", cell, flags=re.IGNORECASE):
                        continue
                    good_cells.append(cell)

                if good_cells:
                    line = max(good_cells, key=len)
                else:
                    continue

            if self._is_noise_line_for_requirements(line):
                continue

            if 3 <= len(line) <= 350:
                lines.append(line)

        return lines


    def _looks_like_specification_line(self, line: str) -> bool:
        low = (line or "").lower().strip()
        if not low:
            return False

        if self._is_noise_line_for_requirements(low):
            return False

        # Generic patterns for products and characteristics
        has_qty = bool(re.search(r"\b\d+(?:[.,]\d+)?\s*(мм|м2|м²|м|кг|л|шт|рулон|компл|уп|т|гр|см)\b", low, flags=re.IGNORECASE))
        has_tech = any(term in low for term in ["основа", "толщин", "масса", "характерист", "параметр", "значение", "гост", "ту "])
        has_product_indicator = any(term in low for term in ["наименование", "товар", "материал", "марка", "модель", "артикул"])

        # Line is likely a product if:
        # - it has a quantity and some technical term
        # - it has a product indicator and a quantity
        # - it looks like a table row with multiple values
        if has_qty and (has_tech or has_product_indicator):
            return True
        
        if "|" in low and has_qty:
            return True

        return False

    def _is_specification_line(self, line: str) -> bool:
        """
        Возвращает True только для строк, которые похожи на товарную/спецификационную позицию,
        а не на договорной, процедурный или служебный текст.
        """
        if not line:
            return False

        raw = str(line).strip()
        if len(raw) < 8:
            return False

        lower = raw.lower()

        negative_markers = [
            'штраф', 'пеня', 'неустойк', 'оплат', 'аванс', 'расчет', 'расчёт',
            'приемк', 'приёмк', 'расторжен', 'ответственност', 'обеспечени',
            'реквизит', 'подсудност', 'гарант', 'срок исполнен', 'срок поставки',
            'нацрежим', 'нац режим', 'реестр', 'нмцк', 'обоснован', 'извещени',
            'комисси', 'заявк', 'участник', 'контракт', 'договор', 'проект договора',
            'пик', 'стр.', '.doc', '.docx', '.pdf', '.xls', '.xlsx'
        ]
        if any(marker in lower for marker in negative_markers):
            return False

        # Generic positive markers for products/specifications
        generic_positive_markers = [
            'гост', 'ту ', 'ост ', 'характеристик', 'параметр', 'значение',
            'толщин', 'масса', 'вес ', 'длина', 'ширина', 'высота', 'диаметр',
            'марка', 'модель', 'артикул', 'тип ', 'состав', 'свойства',
            'наименование', 'материал', 'товар', 'количеств', 'ед. изм', 'ед изм'
        ]
        if any(marker in lower for marker in generic_positive_markers):
            return True

        # If it has a quantity and unit, it's likely a specification line
        if re.search(r"\b\d+(?:[.,]\d+)?\s*(мм|м2|м²|м|кг|л|шт|рулон|компл|уп|т|гр|см)\b", lower):
            return True

        if self._looks_like_specification_line(raw):
            return True

        return False

    def _normalize_search_query(self, text: str) -> str:
        if not text:
            return ''

        q = str(text)
        q = re.sub(r'^[\d\s\.)-]+', '', q)
        q = re.sub(r'\|', ' ', q)
        q = re.sub(r'\b(или эквивалент|эквивалент|аналог)\b', '', q, flags=re.IGNORECASE)
        q = re.sub(r'\b(стр\.?|страница)\s*\d+\b', '', q, flags=re.IGNORECASE)
        q = re.sub(r'\b\S+\.(doc|docx|pdf|xls|xlsx)\b', '', q, flags=re.IGNORECASE)
        q = re.sub(r'\s{2,}', ' ', q)
        return q.strip(' -;,.')

    def _rule_based_extract_requirement_positions(self, text: str) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        if not text:
            return items

        lines = [line.strip() for line in str(text).splitlines() if line.strip()]
        seen_keys = set()

        for line in lines:
            try:
                if not self._is_specification_line(line):
                    continue

                normalized = re.sub(r'^[\d\s\.)-]+', '', line).strip()
                normalized = re.sub(r'\s{2,}', ' ', normalized)
                if len(normalized) < 5:
                    continue

                quantity_match = re.search(
                    r'(\d+[\d\s.,]*)\s*(м2|м²|м3|м³|м|кг|т|л|шт|рул|упак|компл|комплект|ведро)',
                    normalized,
                    flags=re.IGNORECASE
                )

                quantity = None
                unit = None
                if quantity_match:
                    quantity = quantity_match.group(1).replace(' ', '').strip()
                    unit = quantity_match.group(2).strip()

                cleaned_name = normalized
                cleaned_name = re.sub(r'\|', ' ', cleaned_name)
                cleaned_name = re.sub(r'\s{2,}', ' ', cleaned_name).strip(' -;,.')

                if quantity_match:
                    start, end = quantity_match.span()
                    cleaned_name = (normalized[:start] + ' ' + normalized[end:]).strip(' -;,.')
                    cleaned_name = re.sub(r'\s{2,}', ' ', cleaned_name)

                if len(cleaned_name) < 3:
                    continue

                search_query = self._normalize_search_query(cleaned_name)
                if len(search_query) < 3:
                    continue

                dedupe_key = search_query.lower()
                if dedupe_key in seen_keys:
                    continue
                seen_keys.add(dedupe_key)

                items.append({
                    'position_name': cleaned_name,
                    'search_query': search_query,
                    'quantity': quantity,
                    'unit': unit,
                    'requirements': normalized,
                    'source': 'rule_based'
                })
            except Exception as inner_error:
                logger.warning(f"[AiService] Skip broken specification line: {inner_error}; line={line[:200]}")
                continue

        return items

    def _prepare_requirement_candidate_text(self, text: str) -> str:
        selected_text = self._select_requirement_relevant_blocks(text, max_chars=55000)
        lines = self._split_lines_for_requirements(selected_text)

        selected: list[str] = []
        for idx, line in enumerate(lines):
            if self._looks_like_specification_line(line):
                start = max(0, idx - 1)
                end = min(len(lines), idx + 2)
                for candidate in lines[start:end]:
                    if candidate not in selected:
                        selected.append(candidate)

        candidate_text = "\n".join(selected).strip()
        if candidate_text:
            return candidate_text[:60000]

        return selected_text[:60000]

    def _split_text_for_llm(self, text: str, chunk_size: int = 12000, overlap: int = 1200) -> list[str]:
        text = self._normalize_requirement_text(text)
        if len(text) <= chunk_size:
            return [text] if text else []

        chunks = []
        start = 0
        while start < len(text):
            end = min(len(text), start + chunk_size)
            chunk = text[start:end].strip()
            if chunk:
                chunks.append(chunk)
            if end >= len(text):
                break
            start = max(0, end - overlap)

        return chunks

    def _merge_requirement_positions(self, items: list[dict]) -> list[dict]:
        merged = {}

        def normalize_key(value: str) -> str:
            value = (value or "").lower()
            value = re.sub(r"[^a-zа-я0-9]+", " ", value, flags=re.IGNORECASE)
            value = re.sub(r"\s+", " ", value).strip()
            return value

        for item in items:
            position_name = str(item.get("position_name") or "").strip()
            if not position_name:
                continue

            search_query = str(item.get("search_query") or position_name).strip()
            key = normalize_key(search_query or position_name)
            if not key:
                continue

            characteristics = []
            for value in item.get("characteristics") or []:
                value = self._normalize_requirement_text(str(value))
                if value and value not in characteristics:
                    characteristics.append(value)

            if key not in merged:
                merged[key] = {
                    "position_name": position_name,
                    "quantity": str(item.get("quantity") or "").strip(),
                    "unit": str(item.get("unit") or "").strip(),
                    "characteristics": characteristics,
                    "notes": str(item.get("notes") or "").strip(),
                    "search_query": search_query or position_name,
                }
                continue

            current = merged[key]

            if not current["quantity"] and item.get("quantity"):
                current["quantity"] = str(item.get("quantity") or "").strip()

            if not current["unit"] and item.get("unit"):
                current["unit"] = str(item.get("unit") or "").strip()

            for value in characteristics:
                if value not in current["characteristics"]:
                    current["characteristics"].append(value)

            notes = str(item.get("notes") or "").strip()
            if notes and notes not in current["notes"]:
                current["notes"] = (current["notes"] + " " + notes).strip()

        return list(merged.values())

    def _parse_json_response(self, text: str):
        text = text.strip()
        if text.startswith("```json"):
            text = text.replace("```json", "", 1).replace("```", "", 1).strip()
        elif text.startswith("```"):
            text = text.replace("```", "", 1).replace("```", "", 1).strip()
        
        try:
            return json.loads(text)
        except json.JSONDecodeError as e:
            logger.error(f"JSON Decode Error: {e}")
            logger.error(f"Raw AI Response:\n{text}")
            raise HTTPException(status_code=500, detail=f"AI returned invalid JSON: {e}. Raw response: {text}")

    def generate_with_search(self, prompt: str) -> str:
        if not self.client:
            logger.error("generate_with_search called without initialized Gemini client.")
            raise HTTPException(status_code=503, detail="Gemini client is not initialized")

        logger.info("generate_with_search started.")

        # Проверяем кэш
        cache_key = hashlib.md5(prompt.encode("utf-8", errors="replace")).hexdigest()
        now = time.time()
        if cache_key in self._search_cache:
            cached_time, cached_result = self._search_cache[cache_key]
            if now - cached_time < self._cache_ttl:
                logger.info(
                    f"[AIService] Cache HIT for query hash {cache_key[:8]}. "
                    f"Age: {int(now - cached_time)}s"
                )
                return cached_result
            else:
                del self._search_cache[cache_key]

        # Rate limiting: ждём если последний запрос был недавно
        elapsed = now - self._last_ai_call_time
        if elapsed < self._ai_call_min_interval:
            wait = self._ai_call_min_interval - elapsed
            logger.info(f"[AIService] Rate limit: waiting {wait:.1f}s before API call")
            time.sleep(wait)

        self._last_ai_call_time = time.time()

        try:
            response = self._call_ai_with_retry(
                self.client.models.generate_content,
                contents=prompt,
                config=types.GenerateContentConfig(
                    tools=[types.Tool(google_search=types.GoogleSearch())]
                )
            )

            if not response:
                logger.warning("generate_with_search returned empty response object.")
                raise HTTPException(status_code=500, detail="Empty response object from Gemini")

            text = (response.text or "").strip()
            if not text:
                logger.warning("generate_with_search returned empty response text.")
                raise HTTPException(status_code=500, detail="Empty response text from Gemini")

            # Сохраняем в кэш
            self._search_cache[cache_key] = (time.time(), text)
            # Ограничиваем размер кэша
            if len(self._search_cache) > 50:
                oldest_key = min(self._search_cache, key=lambda k: self._search_cache[k][0])
                del self._search_cache[oldest_key]

            return text

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"generate_with_search failed: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=str(e))

    def find_product_equivalent(self, tender_specs: str, catalog: list):
        if not self.client:
            logger.error("Find product equivalent called without API Key.")
            return [{"id": "error", "match_reason": "API Key missing", "similarity_score": 0}]

        logger.info(f"Finding product equivalent. Specs: {tender_specs[:50]}... Catalog size: {len(catalog)}")
        # Превращаем каталог в легкий контекст
        catalog_context = json.dumps([{"id": p['id'], "title": p['title'], "specs": p['specs']} for p in catalog], ensure_ascii=False)

        prompt = f"""
        Роль: Технический эксперт по материально-техническому снабжению.
        Задача: Подобрать НАИЛУЧШИЙ аналог из каталога для запроса.
        
        ЗАПРОС (Товар/Характеристики): {tender_specs[:1000]}
        
        КАТАЛОГ ПОСТАВЩИКА: {catalog_context}
        
        ИНСТРУКЦИЯ:
        1. Сравни характеристики запроса с каталогом.
        2. Если точного совпадения нет, ищи ближайший аналог по ключевым техническим свойствам и параметрам.
        3. Если запрос слишком общий, предложи самый подходящий товар этой категории.
        
        Верни JSON (массив):
        [{{ "id": "id товара", "match_reason": "Объяснение: совпадает основа, толщина и т.д.", "similarity_score": 95 }}]
        """

        try:
            response = self._call_ai_with_retry(
                self.client.models.generate_content,
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json"
                )
            )
            return self._parse_json_response(response.text)
        except Exception as e:
            if isinstance(e, HTTPException):
                raise e
            logger.error(f"AI Error (find_product_equivalent): {e}", exc_info=True)
            return []

    def search_products_internet(self, query: str):
        """Поиск аналогов в интернете с использованием Google Search Grounding"""
        if not self.client:
            return "API Key missing"

        logger.info(f"Searching internet for product: {query}")
        # Промпт усилен для поиска АНАЛОГОВ и ЦЕН
        prompt = f"""
        ЗАДАЧА: Выполни поиск в Google и найди доступные в РФ товары/материалы по запросу: "{query}".
        
        ЕСЛИ ЗАПРОШЕН БРЕНД:
        - Найди этот товар.
        - Найди 1-2 прямых АНАЛОГА от других производителей, если они сопоставимы по качеству.
        
        ДЛЯ КАЖДОГО ТОВАРА УКАЖИ:
        1. **Полное название** (Бренд + Марка).
        2. *Характеристики*: Ключевые технические параметры (размеры, материал, эксплуатационные свойства).
        3. *Цена*: Найди актуальную розничную или оптовую цену (укажи дату или источник, если видно).
        4. *Статус*: Является ли это прямым аналогом запрошенного товара.
        
        Используй актуальные данные с сайтов: tstn.ru, gidroizol.ru, petrovich.ru, krovlya-opt.ru.
        
        Ответ верни в формате Markdown. Сделай акцент на сравнении цены и характеристик.
        """
        
        try:
            # Используем Google Search Tool
            response = self._call_ai_with_retry(
                self.client.models.generate_content,
                contents=prompt,
                config=types.GenerateContentConfig(
                    tools=[types.Tool(google_search=types.GoogleSearch())]
                )
            )
            return response.text
        except Exception as e:
            logger.error(f"Error searching internet: {e}", exc_info=True)
            return f"Error searching internet: {e}"

    def enrich_product_specs(self, product_name: str):
        """Ищет реальные характеристики товара в интернете по названию (для волшебной палочки)"""
        if not self.client:
            return "API Key missing"

        logger.info(f"Enriching specs for: {product_name}")
        prompt = f"""
        ЗАДАЧА: Найти официальный Технический Лист (TDS) или страницу товара в магазине для: "{product_name}".
        ОБЯЗАТЕЛЬНО ИСПОЛЬЗУЙ GOOGLE SEARCH. Мне нужны точные цифры, а не галлюцинации.
        
        Найди основные технические параметры, такие как:
        1. Габариты / Размеры / Толщина
        2. Вес / Плотность
        3. Материал / Состав / Основа
        4. Ключевые эксплуатационные характеристики (прочность, температурный режим и т.д.)
        5. Соответствие стандартам (ГОСТ, ТУ)

        СФОРМИРУЙ ОТВЕТ ОДНОЙ СТРОКОЙ:
        "Параметры: [характеристика 1], [характеристика 2], [характеристика 3], [характеристика 4]."
        
        Если данных нет в поиске, напиши: "Спецификация не найдена в интернете."
        """
        
        try:
            response = self._call_ai_with_retry(
                self.client.models.generate_content,
                contents=prompt,
                config=types.GenerateContentConfig(
                    tools=[types.Tool(google_search=types.GoogleSearch())]
                )
            )
            
            if not response.text:
                return "Характеристики не найдены в поисковой выдаче."
                
            # Очистка от лишнего форматирования, если модель решит добавить markdown
            text = response.text.strip().replace('**', '').replace('*', '')
            return text
        except Exception as e:
            logger.error(f"Error enriching specs: {e}", exc_info=True)
            return f"Ошибка поиска характеристик: {e}"

    def extract_products_from_text(self, text: str):
        """Извлекает список товаров и их характеристик из неструктурированного текста (КП, Смета)"""
        if not self.client:
            return []

        logger.info(f"Extracting products from text. Length: {len(text)}")
        prompt = f"""
        Роль: Парсер строительных смет и спецификаций.
        Задача: Извлеки из текста список товаров/материалов и их характеристики.
        Игнорируй работы (укладка, монтаж), только материалы.
        
        ТЕКСТ:
        {text[:20000]}
        
        ВЕРНИ JSON массив:
        [
          {{
            "name": "Название материала",
            "quantity": "количество (если есть)",
            "specs": "строка с характеристиками (толщина, вес, основа и т.д.)"
          }}
        ]
        """
        try:
            response = self._call_ai_with_retry(
                self.client.models.generate_content,
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json"
                )
            )
            return self._parse_json_response(response.text)
        except Exception as e:
            if isinstance(e, HTTPException):
                raise e
            logger.error(f"Extraction Error: {e}", exc_info=True)
            return []

    def extract_tender_requirement_positions(self, text: str):
        """
        Извлекает из ТЗ только поставляемые материальные позиции.
        Перед extraction сначала выделяет релевантные блоки и убирает шум.
        """
        text = self._normalize_requirement_text(text)
        if not text:
            return []

        candidate_text = self._prepare_requirement_candidate_text(text)
        if not candidate_text:
            return []

        logger.info(f"Extracting tender requirement positions. Length: {len(candidate_text)}")

        try:
            fallback_items = self._rule_based_extract_requirement_positions(candidate_text)
        except Exception as fallback_error:
            logger.error(
                f"[AiService] Rule-based extraction failed: {fallback_error}",
                exc_info=True
            )
            fallback_items = []

        if not self.client:
            logger.warning("AI client is unavailable. Returning rule-based requirement extraction.")
            return fallback_items

        chunks = self._split_text_for_llm(candidate_text, chunk_size=12000, overlap=1000)
        ai_items = []

        for chunk_index, chunk in enumerate(chunks[:6], start=1):
            prompt = f"""
            Роль: старший инженер-сметчик и эксперт по материально-техническому снабжению.

            Нужно извлечь только поставляемые материальные позиции из фрагмента ТЗ.
            Игнорируй работы, услуги, этапы, договорные условия, требования к участнику,
            НМЦД, расчеты, национальный режим, извещение и общие процедурные формулировки.
            Нельзя додумывать характеристики, которых нет в тексте.
            Если одна и та же позиция встречается несколько раз, возвращай одну нормализованную запись.
            Если строка содержит служебные ссылки на файл/страницу/источник — игнорируй такие хвосты
            и извлекай только чистое название товара и характеристики.

            Верни СТРОГО JSON-массив.
            Формат каждой записи:
            {{
              "position_name": "нормализованное название товара/материала",
              "quantity": "количество строкой",
              "unit": "единица измерения",
              "characteristics": ["список технических характеристик"],
              "notes": "важная оговорка",
              "search_query": "короткий поисковый запрос для поиска аналогов"
            }}

            ФРАГМЕНТ ТЗ:
            {chunk}
            """

            try:
                response = self._call_ai_with_retry(
                    self.client.models.generate_content,
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        response_mime_type="application/json"
                    )
                )
                part = self._parse_json_response(response.text)
                if isinstance(part, list):
                    ai_items.extend(part)
            except Exception as e:
                logger.warning(
                    f"Requirement extraction chunk {chunk_index} failed, "
                    f"rule-based fallback will be used for this fragment: {e}"
                )

        merged = self._merge_requirement_positions(fallback_items + ai_items)
        return merged

    def compare_requirements_vs_proposal(self, requirements_text: str, proposal_json_str: str):
        """
        ТРОЙНАЯ ПРОВЕРКА:
        1. ТЗ (Требования)
        2. Предложение (Заявленное)
        3. Интернет (Реальные ТТХ) - выполняется через Grounding
        """
        if not self.client:
            return {"score": 0, "summary": "API Key missing", "items": []}

        logger.info("Comparing requirements vs proposal.")
        prompt = f"""
        Роль: Строгий технадзор и аудитор.
        Задача: Проведи аудит предложения поставщика на соответствие ТЗ.
        
        ВАЖНО: Для каждого товара в предложении используй Google Search, чтобы найти его РЕАЛЬНЫЕ характеристики (TDS) и проверить, не обманывает ли поставщик.
        
        ТРЕБОВАНИЯ ЗАКАЗЧИКА (ТЗ):
        {requirements_text[:10000]}
        
        ПРЕДЛОЖЕНИЕ ПОСТАВЩИКА (ТОВАРЫ):
        {proposal_json_str}
        
        ИНСТРУКЦИЯ:
        1. Сопоставь товары из Предложения с пунктами ТЗ.
        2. ИСПОЛЬЗУЙ Google Search, чтобы найти реальные характеристики предложенных товаров.
        3. Сравни: Требование <-> Заявленное в КП <-> Реальное (из интернета).
        4. Если Заявленное совпадает с Реальным, но не подходит под ТЗ -> FAIL (Несоответствие).
        5. Если Заявленное подходит под ТЗ, но в Реальности характеристики хуже -> FAKE (Обман/Ошибка в КП).
        
        ВЕРНИ JSON:
        {{
            "score": 0-100,
            "summary": "Общий вывод.",
            "items": [
                {{
                    "requirement_name": "Требование ТЗ",
                    "proposal_name": "Товар в КП",
                    "real_specs_found": "Кратко что нашел в интернете",
                    "status": "OK" | "FAIL" | "FAKE" | "MISSING",
                    "comment": "Пояснение: например 'В КП написано -25С, но по факту у Бикроста 0С. Это обман.'"
                }}
            ]
        }}
        """
        try:
            response = self._call_ai_with_retry(
                self.client.models.generate_content,
                contents=prompt,
                config=types.GenerateContentConfig(
                    tools=[types.Tool(google_search=types.GoogleSearch())],
                    response_mime_type="application/json"
                )
            )
            return self._parse_json_response(response.text)
        except Exception as e:
            if isinstance(e, HTTPException):
                raise e
            logger.error(f"Comparison Error: {e}", exc_info=True)
            return {"score": 0, "summary": f"Error: {e}", "items": []}

    def check_compliance(self, title: str, description: str, filenames: list):
        if not self.client:
            return {"overallStatus": "failed", "summary": "API Key missing"}
            
        logger.info(f"Checking compliance for: {title}")
        prompt = f"""
        Role: Tender Compliance Officer (Russian FZ-44/223).
        Analyze if uploaded files match requirements for: "{title}".
        Files: {json.dumps(filenames, ensure_ascii=False)}
        
        Return JSON:
        {{ "missingDocuments": [], "checkedFiles": [{{ "fileName": "...", "status": "valid/invalid", "comments": [] }}], "overallStatus": "passed/failed/warning", "summary": "..." }}
        """

        try:
            response = self._call_ai_with_retry(
                self.client.models.generate_content,
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json"
                )
            )
            return self._parse_json_response(response.text)
        except Exception as e:
            if isinstance(e, HTTPException):
                raise e
            logger.error(f"Compliance Check Error: {e}", exc_info=True)
            return {"overallStatus": "failed", "summary": str(e)}

    def extract_tender_details(self, text: str):
        """Извлекает структурированные данные о тендере из сырого текста (OCR)"""
        if not self.client:
            return {}

        logger.info(f"Extracting details from text. Length: {len(text)}")
        prompt = f"""
        Ты - ассистент по закупкам. Извлеки данные из текста документации.
        
        ТЕКСТ:
        {text[:15000]}
        
        Извлеки:
        1. Название закупки (title) - коротко и ясно.
        2. НМЦК (initial_price) - числом (float).
        3. Дата окончания подачи (deadline).
        4. Номер закупки (eis_number) - если есть, формат 11-19 цифр.
        5. Описание/ТЗ (description) - краткая выжимка (что нужно).
        
        ВЕРНИ JSON:
        {{
            "title": "...",
            "initial_price": 1000.00,
            "deadline": "dd.mm.yyyy",
            "eis_number": "...",
            "description": "..."
        }}
        """

        try:
            response = self._call_ai_with_retry(
                self.client.models.generate_content,
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json"
                )
            )
            return self._parse_json_response(response.text)
        except Exception as e:
            if isinstance(e, HTTPException):
                raise e
            logger.error(f"Extraction Error: {e}", exc_info=True)
            return {}

