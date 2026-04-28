import os
import time
from typing import List, Dict, Any, Optional
from fastapi import HTTPException
from google import genai
from google.genai import types
from dotenv import load_dotenv
from backend.services.tz_extractor import (
    extract_tz_from_text, 
    extract_relevant_text, 
    MODEL_AGNOSTIC_PROMPT,
    RequirementItem,
    ExtractionResult
)
import json
import re
import logging
import hashlib
from backend.config import (
    GEMINI_MODEL,
    GEMINI_MODEL_BACKUP,
    GEMINI_MODEL_GROUND,
    GEMINI_MODEL_GROUND_BACKUP,
)
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
    Использует отдельные профили моделей:
    - обычные AI-вызовы без Google Search grounding;
    - grounded AI-вызовы c Google Search.
    """
    def __init__(self):
        self.api_key = (os.getenv("API_KEY") or "").strip()
        self.api_key_secondary = (os.getenv("API_KEY_2") or "").strip()
        self.api_key_tertiary = (os.getenv("API_KEY_3") or "").strip()
        self.model_name = GEMINI_MODEL
        self.fallback_model_name = GEMINI_MODEL_BACKUP
        self.ground_model_name = GEMINI_MODEL_GROUND
        self.ground_fallback_model_name = GEMINI_MODEL_GROUND_BACKUP
        self.active_model = self.model_name
        self.active_ground_model = self.ground_model_name
        self.last_error_code = ""
        self.last_error_message = ""
        self.primary_client_name = "primary"
        self.secondary_client_name = "secondary"
        self.tertiary_client_name = "tertiary"
        self._client_labels = {
            self.primary_client_name: "API_KEY",
            self.secondary_client_name: "API_KEY_2",
            self.tertiary_client_name: "API_KEY_3",
        }
        self._clients: dict[str, Any] = {}

        primary_client = self._build_gemini_client(self.api_key, self.primary_client_name)
        seen_api_keys: set[str] = set()
        if self.api_key:
            seen_api_keys.add(self.api_key)

        secondary_client = None
        if self.api_key_secondary and self.api_key_secondary in seen_api_keys:
            logger.warning("API_KEY_2 duplicates an already configured Gemini key. Secondary Gemini client ignored.")
        else:
            secondary_client = self._build_gemini_client(
                self.api_key_secondary,
                self.secondary_client_name,
            )
            if self.api_key_secondary:
                seen_api_keys.add(self.api_key_secondary)

        tertiary_client = None
        if self.api_key_tertiary and self.api_key_tertiary in seen_api_keys:
            logger.warning("API_KEY_3 duplicates an already configured Gemini key. Tertiary Gemini client ignored.")
        else:
            tertiary_client = self._build_gemini_client(
                self.api_key_tertiary,
                self.tertiary_client_name,
            )

        if primary_client:
            self._clients[self.primary_client_name] = primary_client
        if secondary_client:
            self._clients[self.secondary_client_name] = secondary_client
        if tertiary_client:
            self._clients[self.tertiary_client_name] = tertiary_client

        self.preferred_client_name = self._get_available_client_names()[0] if self._get_available_client_names() else ""
        self.active_client_name = self.preferred_client_name
        self.client = self._get_client(self.active_client_name)

        if self._clients:
            logger.info(
                "Gemini Client initialized. Available keys: %s | Main models: %s -> %s | Grounded models: %s -> %s",
                ", ".join(self._client_labels.get(name, name) for name in self._get_available_client_names()),
                self.model_name,
                self.fallback_model_name,
                self.ground_model_name,
                self.ground_fallback_model_name,
            )
        else:
            logger.warning(
                "No Gemini API keys initialized successfully (API_KEY/API_KEY_2/API_KEY_3). AI analysis will be unavailable."
            )

        # Rate limiter: минимальный интервал между запросами = 5 сек
        self._last_ai_call_time = 0.0
        self._ai_call_min_interval = 5.0  # секунд между запросами
        # Простой in-memory кэш результатов (ключ → (timestamp, result))
        self._search_cache: dict = {}
        self._cache_ttl = 3600  # кэш живёт 1 час

    def has_available_clients(self) -> bool:
        return bool(self._get_available_client_names())

    def _build_gemini_client(self, api_key: str, client_name: str):
        if not api_key:
            logger.warning("%s not found in environment variables.", self._client_labels.get(client_name, client_name))
            return None

        try:
            return genai.Client(api_key=api_key)
        except Exception as e:
            logger.error(
                "Failed to initialize Gemini client for %s: %s",
                self._client_labels.get(client_name, client_name),
                e,
            )
            return None

    def _get_clients(self) -> dict[str, Any]:
        clients = getattr(self, "_clients", None)
        if isinstance(clients, dict) and clients:
            return {name: client for name, client in clients.items() if client}

        fallback_client = getattr(self, "client", None)
        if fallback_client:
            primary_client_name = getattr(self, "primary_client_name", "primary")
            return {primary_client_name: fallback_client}
        return {}

    def _get_available_client_names(self) -> list[str]:
        clients = self._get_clients()
        order = [
            getattr(self, "primary_client_name", "primary"),
            getattr(self, "secondary_client_name", "secondary"),
            getattr(self, "tertiary_client_name", "tertiary"),
        ]
        return [name for name in order if clients.get(name)]

    def _get_client(self, client_name: str):
        return self._get_clients().get(client_name)

    def _get_client_label(self, client_name: str) -> str:
        client_labels = getattr(self, "_client_labels", {}) or {}
        return client_labels.get(client_name, client_name)

    def _get_start_client_name(self) -> str:
        available = self._get_available_client_names()
        if not available:
            return ""
        active_client_name = getattr(self, "active_client_name", "")
        if active_client_name in available:
            return active_client_name
        preferred_client_name = getattr(self, "preferred_client_name", "")
        if preferred_client_name in available:
            return preferred_client_name
        return available[0]

    def _get_model_profile(self, grounded: bool = False) -> tuple[str, str, str]:
        if grounded:
            primary_model = getattr(self, "ground_model_name", "") or getattr(self, "model_name", "")
            backup_model = getattr(self, "ground_fallback_model_name", "") or getattr(self, "fallback_model_name", "")
            active_model = getattr(self, "active_ground_model", "") or primary_model
            return primary_model, backup_model, active_model

        primary_model = getattr(self, "model_name", "")
        backup_model = getattr(self, "fallback_model_name", "")
        active_model = getattr(self, "active_model", "") or primary_model
        return primary_model, backup_model, active_model

    def _build_retry_candidates(
        self,
        requested_model: str,
        start_client_name: str,
        backup_model: str = "",
    ) -> list[tuple[str, str]]:
        available_clients = self._get_available_client_names()
        if not available_clients:
            return []

        if start_client_name not in available_clients:
            start_client_name = available_clients[0]

        alternate_client_names = [
            name for name in available_clients if name != start_client_name
        ]
        fallback_model = backup_model if backup_model and backup_model != requested_model else ""

        raw_candidates = [
            (start_client_name, requested_model),
        ]
        for alternate_client_name in alternate_client_names:
            raw_candidates.append((alternate_client_name, requested_model))
        # Limit transient/service-unavailable failover to one fallback-model route.
        # This avoids burning both projects on both models for a single logical call.
        if fallback_model:
            raw_candidates.append(
                ((alternate_client_names[-1] if alternate_client_names else start_client_name), fallback_model)
            )

        candidates: list[tuple[str, str]] = []
        seen: set[tuple[str, str]] = set()
        for candidate in raw_candidates:
            if candidate[0] and candidate[1] and candidate not in seen:
                seen.add(candidate)
                candidates.append(candidate)
        return candidates

    @staticmethod
    def _find_next_quota_retry_candidate_index(
        retry_candidates: list[tuple[str, str]],
        current_index: int,
        requested_model: str,
        current_client_name: str,
    ) -> int:
        for idx in range(current_index + 1, len(retry_candidates)):
            next_client_name, next_model = retry_candidates[idx]
            if next_client_name != current_client_name and next_model == requested_model:
                return idx
        return -1

    def _resolve_client_method(self, method, client):
        if client and hasattr(client, "models") and hasattr(client.models, "generate_content"):
            return client.models.generate_content
        return method

    def _set_public_client(self, client_name: str) -> None:
        self.client = self._get_client(client_name)

    def test_model_availability(self) -> str:
        """
        Тестовый запрос к моделям при старте для выбора рабочей модели.
        Возвращает имя выбранной модели или пустую строку, если ни одна не доступна.
        """
        logger.info(
            "AI Startup Check: Main models: %s -> %s, Grounded models: %s -> %s, Keys: %s",
            self.model_name,
            self.fallback_model_name,
            self.ground_model_name,
            self.ground_fallback_model_name,
            ", ".join(self._get_client_label(name) for name in self._get_available_client_names()) or "none",
        )

        if not self._get_available_client_names():
            logger.error("Startup check: No Gemini clients initialized (no API_KEY / API_KEY_2 / API_KEY_3).")
            return ""

        test_prompt = "Hello, this is a startup connectivity test. Reply with 'OK'."

        startup_candidates: list[tuple[str, str]] = []
        for client_name in self._get_available_client_names():
            startup_candidates.append((client_name, self.model_name))
            if self.fallback_model_name and self.fallback_model_name != self.model_name:
                startup_candidates.append((client_name, self.fallback_model_name))

        for client_name, model_name in startup_candidates:
            client = self._get_client(client_name)
            if not client:
                continue

            logger.info(
                "Startup check: Testing %s with model %s...",
                self._get_client_label(client_name),
                model_name,
            )
            try:
                response = client.models.generate_content(
                    model=model_name,
                    contents=test_prompt,
                )
                if response and response.text:
                    logger.info(
                        "Startup check: %s with model %s is ONLINE. Selected as active.",
                        self._get_client_label(client_name),
                        model_name,
                    )
                    self.active_model = model_name
                    self.active_client_name = client_name
                    self._set_public_client(client_name)
                    return self.active_model
            except Exception as e:
                error_summary = self._summarize_ai_error(e)
                logger.warning(
                    "Startup check: %s with model %s is OFFLINE: %s",
                    self._get_client_label(client_name),
                    model_name,
                    error_summary,
                )
                if DEBUG_MODE:
                    logger.debug(
                        "Startup check full error | client=%s | model=%s | error=%s",
                        self._get_client_label(client_name),
                        model_name,
                        e,
                    )

        logger.critical("Startup check: WARNING - No AI models are available right now. Backend will start in degraded mode, AI operations will fail until models come online.")

        self.active_model = self.model_name
        self.active_client_name = self._get_start_client_name()
        self._set_public_client(self.active_client_name)
        return self.active_model

    def _is_transient_error(self, error: Exception) -> bool:
        """Определяет, является ли ошибка временной (429, 503, 504, timeout)."""
        err_str = str(error).lower()
        # Коды ошибок и типичные сообщения
        transient_indicators = [
            "429", "too many requests", 
            "503", "service unavailable", 
            "504", "gateway timeout",
            "deadline exceeded", "timeout",
            "transport error", "connection reset", "socket error",
            "sslerror", "ssl eof", "unexpected_eof_while_reading",
            "unexpected eof while reading", "eof occurred in violation of protocol",
            "httpsconnectionpool", "max retries exceeded", "read timed out",
            "remote disconnected", "connection aborted", "connection broken",
            "temporarily unavailable",
        ]
        return any(indicator in err_str for indicator in transient_indicators)

    def _summarize_ai_error(self, error: Exception) -> str:
        """Возвращает компактное описание ошибки AI без длинного JSON payload."""
        raw_text = " ".join(str(error).split())
        lower_text = raw_text.lower()

        code_match = re.search(r"\b(400|429|500|503|504)\b", raw_text)
        retry_match = re.search(
            r"retry(?:\s+in|\s*delay)?[^0-9]*([0-9]+(?:\.[0-9]+)?s)",
            lower_text,
        )

        self.last_error_code = code_match.group(1) if code_match else ""

        if (
            "resource_exhausted" in lower_text
            or "quota" in lower_text
            or self.last_error_code == "429"
        ):
            summary = "429 quota exhausted"
            if retry_match:
                summary += f", retry in {retry_match.group(1)}"
            self.last_error_message = summary
            return summary

        if self.last_error_code in {"503", "504"} or "service unavailable" in lower_text:
            summary = f"{self.last_error_code or '503'} service unavailable"
            if retry_match:
                summary += f", retry in {retry_match.group(1)}"
            self.last_error_message = summary
            return summary

        if "timeout" in lower_text or "deadline exceeded" in lower_text:
            self.last_error_message = "request timeout"
            return self.last_error_message

        if (
            "sslerror" in lower_text
            or "httpsconnectionpool" in lower_text
            or "max retries exceeded" in lower_text
            or "unexpected eof while reading" in lower_text
            or "eof occurred in violation of protocol" in lower_text
            or "read timed out" in lower_text
        ):
            self.last_error_message = "transport error"
            return self.last_error_message

        if self.last_error_code == "400" or "invalid_argument" in lower_text:
            self.last_error_message = "400 invalid argument"
            return self.last_error_message

        trimmed = raw_text[:240]
        if len(raw_text) > len(trimmed):
            trimmed += "..."
        self.last_error_message = trimmed
        return trimmed

    def _call_ai_with_retry(self, method, grounded: bool = False, **kwargs):
        """
        Унифицированный вызов ИИ с логированием, повторами и переключением
        по ключам и моделям.
        ВАЖНО:
        - при 429/недоступности сначала пробуем резервные ключи API_KEY_2/API_KEY_3;
        - затем, если нужно, fallback-модель;
        - успешный ответ с резервного пути НЕ делает его постоянным приоритетом.
        """
        if not self._get_available_client_names():
            raise Exception("Gemini Client not initialized")

        primary_model, backup_model, active_profile_model = self._get_model_profile(grounded=grounded)
        profile_label = "grounded" if grounded else "default"
        if not active_profile_model:
            logger.warning(
                "[AIService] active model for %s profile was empty. Setting to primary model.",
                profile_label,
            )
            active_profile_model = primary_model
            if grounded:
                self.active_ground_model = primary_model
            else:
                self.active_model = primary_model

        max_retries = 4
        requested_model = kwargs.get("model") or active_profile_model
        kwargs["model"] = requested_model
        start_client_name = self._get_start_client_name()
        retry_candidates = self._build_retry_candidates(
            requested_model,
            start_client_name,
            backup_model=backup_model,
        )
        if not retry_candidates:
            raise Exception("Gemini Client not initialized")

        candidate_index = 0

        for attempt in range(1, max_retries + 1):
            attempt_start = time.time()
            current_client_name, current_model = retry_candidates[candidate_index]
            current_client = self._get_client(current_client_name)
            invoke_method = self._resolve_client_method(method, current_client)
            kwargs["model"] = current_model

            try:
                logger.info(
                    "AI Call Attempt %s/%s | Profile: %s | Key: %s | Model: %s",
                    attempt,
                    max_retries,
                    profile_label,
                    self._get_client_label(current_client_name),
                    current_model,
                )
                response = invoke_method(**kwargs)

                duration = time.time() - attempt_start
                logger.info(f"AI Call Success | Attempt: {attempt} | Duration: {duration:.2f}s")

                if current_client_name != start_client_name or current_model != requested_model:
                    logger.warning(
                        "Dynamic AI failover success: [%s] %s/%s -> %s/%s. Preference NOT permanently changed.",
                        profile_label,
                        self._get_client_label(start_client_name),
                        requested_model,
                        self._get_client_label(current_client_name),
                        current_model,
                    )

                return response

            except Exception as e:
                duration = time.time() - attempt_start
                error_text = str(e)
                lower_error = error_text.lower()
                error_summary = self._summarize_ai_error(e)

                is_transient = self._is_transient_error(e)
                is_quota = (
                    "429" in error_text
                    or "resource_exhausted" in lower_error
                    or "quota" in lower_error
                )

                error_type = "TRANSIENT" if is_transient else "FATAL"
                logger.warning(
                    "AI Call Failed (%s) | Attempt: %s | Profile: %s | Key: %s | Model: %s | Duration: %.2fs | Error: %s",
                    error_type,
                    attempt,
                    profile_label,
                    self._get_client_label(current_client_name),
                    current_model,
                    duration,
                    error_summary,
                )
                if DEBUG_MODE:
                    logger.debug(
                        "AI Call full error payload | Key: %s | Model: %s | Error: %s",
                        self._get_client_label(current_client_name),
                        current_model,
                        e,
                    )

                next_candidate_index = -1
                if is_quota:
                    # For quota errors we preserve capacity by switching only to the
                    # alternate project's key on the same model, without burning
                    # fallback-model attempts in the same logical request.
                    next_candidate_index = self._find_next_quota_retry_candidate_index(
                        retry_candidates,
                        candidate_index,
                        requested_model,
                        current_client_name,
                    )
                elif is_transient and candidate_index + 1 < len(retry_candidates):
                    next_candidate_index = candidate_index + 1

                if next_candidate_index >= 0:
                    next_client_name, next_model = retry_candidates[next_candidate_index]
                    reason = "quota exhausted" if is_quota else "service unavailable"
                    logger.warning(
                        "AI_KEY_FAILOVER | profile=%s | reason=%s | from_key=%s | from_model=%s | to_key=%s | to_model=%s",
                        profile_label,
                        reason,
                        self._get_client_label(current_client_name),
                        current_model,
                        self._get_client_label(next_client_name),
                        next_model,
                    )
                    logger.warning(
                        "[AIService] %s on %s/%s. Switching to %s/%s",
                        reason,
                        self._get_client_label(current_client_name),
                        current_model,
                        self._get_client_label(next_client_name),
                        next_model,
                    )
                    candidate_index = next_candidate_index
                    continue

                if is_quota:
                    logger.error(
                        "[AIService] Quota exhausted on %s/%s. No more key/model fallbacks available.",
                        self._get_client_label(current_client_name),
                        current_model,
                    )
                    raise HTTPException(
                        status_code=429,
                        detail=f"QUOTA_EXHAUSTED | {error_summary}",
                    )

                if not is_transient:
                    if "400" in error_text or "invalid_argument" in lower_error:
                        raise HTTPException(
                            status_code=400,
                            detail=f"INVALID_ARGUMENT | {error_summary}",
                        )
                    raise HTTPException(
                        status_code=500,
                        detail=f"AI_FATAL_ERROR | {error_summary}",
                    )

                if attempt == max_retries:
                    logger.error(f"AI Call: All {max_retries} attempts exhausted for transient errors.")
                    raise HTTPException(
                        status_code=503,
                        detail=f"SERVICE_UNAVAILABLE | {error_summary}",
                    )

                wait_time = [3, 5, 9][min(attempt - 1, 2)]
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
        text = (text or "").replace("\ufeff", "").replace("\u200b", "").strip()
        if text.startswith("```json"):
            text = text.replace("```json", "", 1).replace("```", "", 1).strip()
        elif text.startswith("```"):
            text = text.replace("```", "", 1).replace("```", "", 1).strip()

        decoder = json.JSONDecoder()
        last_error = None

        try:
            return json.loads(text)
        except json.JSONDecodeError as exc:
            last_error = exc

        for idx, char in enumerate(text):
            if char not in "{[":
                continue
            try:
                parsed, _ = decoder.raw_decode(text[idx:])
                return parsed
            except json.JSONDecodeError as exc:
                last_error = exc
                continue

        logger.error(f"JSON Decode Error: {last_error}")
        logger.error(f"Raw AI Response:\n{text}")
        raise HTTPException(
            status_code=500,
            detail=f"AI returned invalid JSON: {last_error}. Raw response: {text}"
        )

    def _extract_grounding_sources(self, response) -> list[dict]:
        sources: list[dict] = []
        seen: set[str] = set()
        try:
            candidates = getattr(response, "candidates", None) or []
            if not candidates:
                return sources
            grounding_metadata = getattr(candidates[0], "grounding_metadata", None)
            grounding_chunks = getattr(grounding_metadata, "grounding_chunks", None) or []
            for chunk in grounding_chunks:
                web = getattr(chunk, "web", None)
                retrieved_context = getattr(chunk, "retrieved_context", None)
                title = ""
                uri = ""
                if web:
                    title = str(getattr(web, "title", "") or "").strip()
                    uri = str(getattr(web, "uri", "") or "").strip()
                elif retrieved_context:
                    title = str(getattr(retrieved_context, "title", "") or "").strip()
                    uri = str(getattr(retrieved_context, "uri", "") or "").strip()
                if not uri:
                    continue
                key = uri.strip().lower()
                if key in seen:
                    continue
                seen.add(key)
                sources.append({"title": title, "url": uri})
        except Exception as exc:
            logger.warning("Failed to extract grounding sources from Gemini response: %s", exc)
        return sources

    def generate_with_search(self, prompt: str, include_sources: bool = False):
        if not self.client:
            logger.error("generate_with_search called without initialized Gemini client.")
            raise HTTPException(status_code=503, detail="Gemini client is not initialized")

        logger.info("generate_with_search started with Google Search grounding.")

        # Проверяем кэш
        cache_version = "grounded_json_v4"
        _, _, grounded_active_model = self._get_model_profile(grounded=True)
        cache_key = hashlib.md5(
            f"{cache_version}|{grounded_active_model}|{prompt}".encode("utf-8", errors="replace")
        ).hexdigest()
        now = time.time()
        if cache_key in self._search_cache:
            cached_time, cached_result = self._search_cache[cache_key]
            if now - cached_time < self._cache_ttl:
                logger.info(
                    f"[AIService] Cache HIT for query hash {cache_key[:8]}. "
                    f"Age: {int(now - cached_time)}s"
                )
                if isinstance(cached_result, dict):
                    return cached_result if include_sources else cached_result.get("text", "")
                return cached_result if not include_sources else {"text": str(cached_result or ""), "sources": []}
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
            response = None
            text = ""
            empty_detail = ""
            for attempt in range(2):
                response = self._call_ai_with_retry(
                    self.client.models.generate_content,
                    grounded=True,
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        tools=[types.Tool(google_search=types.GoogleSearch())]
                    )
                )

                if not response:
                    empty_detail = "Empty response object from Gemini"
                    logger.warning("generate_with_search returned empty response object.")
                else:
                    text = (response.text or "").strip()
                    if text:
                        break
                    empty_detail = "Empty response text from Gemini"
                    logger.warning("generate_with_search returned empty response text.")

                if attempt == 0:
                    logger.warning("Retrying grounded request once because Gemini returned an empty payload.")
                    time.sleep(1.0)

            if not response:
                raise HTTPException(status_code=500, detail=empty_detail or "Empty response object from Gemini")
            if not text:
                raise HTTPException(status_code=500, detail=empty_detail or "Empty response text from Gemini")

            payload = {
                "text": text,
                "sources": self._extract_grounding_sources(response),
            }

            # Сохраняем в кэш
            self._search_cache[cache_key] = (time.time(), payload)
            # Ограничиваем размер кэша
            if len(self._search_cache) > 50:
                oldest_key = min(self._search_cache, key=lambda k: self._search_cache[k][0])
                del self._search_cache[oldest_key]

            return payload if include_sources else text

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

    def validate_analog_candidates(
        self,
        *,
        query: str,
        requirements: str,
        candidates: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Второй этап валидации кандидатов аналогов.
        На вход получает уже отфильтрованные товары и возвращает строгое AI-решение:
        APPROVED / REJECTED / UNCERTAIN по каждому кандидату.
        """
        if not self.client:
            logger.warning("validate_analog_candidates called without initialized Gemini client.")
            return {"results": [], "summary": "API Key missing"}

        if not candidates:
            return {"results": [], "summary": "No candidates provided"}

        logger.info(
            "AI validating analog candidates. Query len=%s | requirements len=%s | candidates=%s",
            len(query or ""),
            len(requirements or ""),
            len(candidates),
        )

        compact_candidates = []
        for candidate in candidates[:20]:
            compact_candidates.append({
                "candidate_id": str(candidate.get("candidate_id") or candidate.get("id") or ""),
                "title": str(candidate.get("title") or ""),
                "category": str(candidate.get("category") or ""),
                "material_type": str(candidate.get("material_type") or ""),
                "specs": candidate.get("specs") or {},
                "description": str(candidate.get("description") or ""),
                "source": str(candidate.get("source") or ""),
                "match_score": candidate.get("match_score"),
                "match_reason": str(candidate.get("match_reason") or ""),
            })

        prompt = f"""
        Роль: Строгий технический эксперт по подбору аналогов для тендерных закупок.

        ЗАДАЧА:
        Проведи ВТОРОЙ ЭТАП ВАЛИДАЦИИ найденных кандидатов.
        Нужно определить, какие кандидаты действительно можно показывать как аналоги для позиции ТЗ.

        ИСХОДНАЯ ПОЗИЦИЯ / ПОИСКОВЫЙ ЗАПРОС:
        {query[:1200]}

        ТРЕБОВАНИЯ ИЗ ТЗ:
        {(requirements or "")[:4000]}

        КАНДИДАТЫ ДЛЯ ПРОВЕРКИ:
        {json.dumps(compact_candidates, ensure_ascii=False)}

        ПРАВИЛА:
        1. APPROVED ставь только если кандидат действительно соответствует типу материала и не противоречит ТЗ.
        2. REJECTED ставь только если есть явное противоречие ТЗ: другой тип материала, другая марка, другая основа, другое назначение или подтверждено худшее/недопустимое числовое значение.
        3. Если часть параметров не указана, но ключевые признаки совпадают и явных противоречий нет, ставь UNCERTAIN, а не REJECTED.
        4. Отсутствие одного второстепенного параметра само по себе не является основанием для REJECTED.
        5. Не выдумывай характеристики, опирайся только на переданные данные.
        6. Кандидаты уже предварительно отфильтрованы локальной логикой. Не отбрасывай сильный кандидат только из-за неполных данных, если нет прямого конфликта с ТЗ.
        7. Будь строгим к противоречиям, но не к отсутствию части данных.
        8. Не делай вывод о массе, толщине или других числовых ТТХ только по коммерческой маркировке в названии вроде "ЭПП-4,0" или "ЭКП-4,5", если в явных specs этого нет.

        ВЕРНИ ТОЛЬКО JSON:
        {{
          "summary": "Краткий общий вывод",
          "results": [
            {{
              "candidate_id": "id кандидата",
              "title": "точное название кандидата из входного списка",
              "status": "APPROVED" | "REJECTED" | "UNCERTAIN",
              "validation_score": 0,
              "comment": "Краткое объяснение решения",
              "matched_parameters": ["параметр 1", "параметр 2"],
              "conflicting_parameters": ["параметр 3"],
              "missing_parameters": ["параметр 4"]
            }}
          ]
        }}

        ОБЯЗАТЕЛЬНО:
        - для каждого решения верни candidate_id ТОЧНО как во входных данных;
        - дополнительно продублируй title ТОЧНО как во входных данных.

        matched_parameters:
        - параметры, по которым кандидат явно соответствует ТЗ.

        conflicting_parameters:
        - параметры, по которым есть явное противоречие ТЗ.
        - сюда обязательно включай конкретные названия параметров, например:
          "массовая доля нелетучих веществ", "толщина", "основа", "температура размягчения".

        missing_parameters:
        - параметры, которые важны для проверки, но не подтверждены по данным кандидата.
        """

        try:
            response = self._call_ai_with_retry(
                self.client.models.generate_content,
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json"
                )
            )
            parsed = self._parse_json_response(response.text)
            if not isinstance(parsed, dict):
                return {"results": [], "summary": "AI returned unexpected payload"}
            return parsed
        except Exception as e:
            if isinstance(e, HTTPException):
                raise e
            logger.error(f"AI Error (validate_analog_candidates): {e}", exc_info=True)
            return {"results": [], "summary": str(e)}

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
                grounded=True,
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
                grounded=True,
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

    def extract_tender_requirement_positions(self, text: str) -> Dict[str, Any]:
        """
        Универсальное извлечение позиций ТЗ (rule-based + AI).
        Сначала выполняется rule-based анализ, затем AI дообработка по очищенному тексту.
        """
        logger.info(f"Extracting universal requirements. Raw text length: {len(text)}")
        
        # 1. Сначала используем универсальный rule-based экстрактор
        rb_result = extract_tz_from_text(text)
        
        # 2. Если AI клиент недоступен, возвращаем rule-based результат
        if not self.client:
            logger.warning("AI client is unavailable. Returning rule-based extraction result only.")
            return rb_result

        # 3. Выделяем очищенный релевантный текст для AI
        relevant_text, debug_info = extract_relevant_text(text)
        if not relevant_text.strip():
            return rb_result

        # 4. Выполняем AI извлечение по очищенному тексту
        ai_result = {"items": [], "general_requirements": [], "warnings": []}
        
        # Разбиваем на чанки если текст слишком длинный
        chunks = self._split_text_for_llm(relevant_text, chunk_size=15000, overlap=1000)
        
        for chunk in chunks[:4]: # Ограничиваем количество чанков для экономии токенов
            prompt = f"{MODEL_AGNOSTIC_PROMPT}\n\nФРАГМЕНТ ТЗ:\n{chunk}"
            
            try:
                response = self._call_ai_with_retry(
                    self.client.models.generate_content,
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        response_mime_type="application/json"
                    )
                )
                part = self._parse_json_response(response.text)
                if isinstance(part, dict):
                    ai_result["items"].extend(part.get("items", []))
                    ai_result["general_requirements"].extend(part.get("general_requirements", []))
                    ai_result["warnings"].extend(part.get("warnings", []))
            except Exception as e:
                logger.error(f"AI requirement extraction chunk failed: {e}")

        # 5. Смерживаем результаты
        # Собираем все айтемы
        combined_items = rb_result.get("items", []) + ai_result.get("items", [])
        
        # Нормализуем и мержим дубликаты
        # (в tz_extractor есть merge_similar_items, но он работает с RequirementItem)
        # Для простоты превратим все в словари и используем логику мерджа
        
        final_items = self._merge_requirement_positions_v2(combined_items)
        
        final_warnings = list(set(rb_result.get("warnings", []) + ai_result.get("warnings", [])))
        final_reqs = list(set(rb_result.get("general_requirements", []) + ai_result.get("general_requirements", [])))
        
        return {
            "items": final_items,
            "general_requirements": final_reqs,
            "warnings": final_warnings,
            "debug": {
                "rule_based": rb_result.get("debug"),
                "ai_chunks": len(chunks)
            }
        }

    def _merge_requirement_positions_v2(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Мержит похожие позиции, приоритизируя данные из более уверенных источников.
        """
        merged: Dict[str, Dict[str, Any]] = {}
        for item in items:
            name = str(item.get("normalized_name") or item.get("position_name") or "").strip()
            if not name:
                continue
            
            key = re.sub(r"\s+", " ", name.lower()).strip()
            if not key:
                continue
                
            if key not in merged:
                # Генерируем search_query если его нет
                if not item.get("search_query"):
                    # Здесь могла бы быть логика build_search_query, но мы надеемся на LLM/Extractor
                    item["search_query"] = name
                merged[key] = item
                continue
                
            base = merged[key]
            # Дополняем характеристики
            chars = list(set((base.get("characteristics") or []) + (item.get("characteristics") or [])))
            base["characteristics"] = chars
            
            # Обновляем количество если в базе его нет
            if not base.get("quantity") and item.get("quantity"):
                base["quantity"] = item.get("quantity")
            if not base.get("unit") and item.get("unit"):
                base["unit"] = item.get("unit")
            
            # Прихватываем примечания
            if not base.get("notes") and item.get("notes"):
                base["notes"] = item.get("notes")

        return list(merged.values())

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
                grounded=True,
                contents=prompt,
                config=types.GenerateContentConfig(
                    tools=[types.Tool(google_search=types.GoogleSearch())]
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
        6. Закон (law_type) - одно из: 44-ФЗ, 223-ФЗ, Коммерч.
        7. Наименование заказчика (customer_name).
        8. ИНН заказчика (customer_inn) - 10 или 12 цифр, если найден.
        9. Место нахождения заказчика (customer_location).
        10. Дата публикации/размещения (publish_date), если есть.

        ВЕРНИ JSON:
        {{
            "title": "...",
            "initial_price": 1000.00,
            "deadline": "dd.mm.yyyy",
            "eis_number": "...",
            "description": "...",
            "law_type": "44-ФЗ",
            "customer_name": "...",
            "customer_inn": "...",
            "customer_location": "...",
            "publish_date": "dd.mm.yyyy"
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

