import json
import re
from typing import Any, Dict, List, Optional

from backend.logger import logger, log_debug_event
from .legal_prompts import PROMPT_EXTRACT_FACTS


TOPIC_CONFIG = [
    {"id": "customer_info", "desc": "заказчик и контакты"},
    {"id": "subject", "desc": "предмет закупки"},
    {"id": "items_quantities", "desc": "позиции и количества"},
    {"id": "nmcc_prices", "desc": "НМЦК и цены"},
    {"id": "delivery_terms", "desc": "сроки поставки / исполнения"},
    {"id": "logistics", "desc": "логистика, приемка, разгрузка"},
    {"id": "payment", "desc": "условия оплаты"},
    {"id": "bid_docs", "desc": "документы заявки"},
    {"id": "delivery_docs", "desc": "документы при поставке"},
    {"id": "liability", "desc": "штрафы, пени, ответственность"},
    {"id": "equivalents", "desc": "эквиваленты / аналоги"},
    {"id": "restrictions", "desc": "национальный режим / ограничения"},
    {"id": "conflicts", "desc": "противоречия между документами"},
]

TOPIC_KEYWORDS = {
    "customer_info": ["заказчик", "инн", "кпп", "огрн", "адрес", "контакт", "телефон", "email", "почта", "реквизит"],
    "subject": ["предмет", "объект", "закупк", "товар", "работ", "услуг", "наименование", "описание"],
    "items_quantities": ["количеств", "объем", "единиц", "ед.", "штук", "спецификац", "таблиц"],
    "nmcc_prices": ["нмцк", "цена", "стоимость", "руб", "начальная", "максимальная", "смет"],
    "delivery_terms": ["срок", "поставк", "исполнени", "этап", "график", "период", "дней", "рабочих", "календарных"],
    "logistics": ["место", "доставк", "разгрузк", "приемк", "склад", "адрес", "транспорт"],
    "payment": ["оплат", "расчет", "аванс", "казначейск", "счет", "рабочих дней", "календарных дней"],
    "bid_docs": ["заявк", "участник", "документ", "декларац", "выписк", "свидетельств", "лиценз"],
    "delivery_docs": ["накладн", "акт", "упд", "счет-фактур", "сертификат", "паспорт", "при поставке"],
    "liability": ["штраф", "пен", "неустойк", "ответственност", "расторжени", "односторон"],
    "equivalents": ["эквивалент", "аналог", "товарный знак", "замен"],
    "restrictions": ["национальн", "режим", "запрет", "допуск", "пп рф", "постановлен", "реестр"],
    "conflicts": ["противореч", "разноглас", "несоответств", "разные значения"],
}

AGGREGATE_TOPICS = {
    "customer_info",
    "subject",
    "items_quantities",
    "delivery_terms",
    "logistics",
    "payment",
    "bid_docs",
    "delivery_docs",
    "liability",
    "restrictions",
    "conflicts",
}

SELECT_TOPICS = {
    "equivalents",
    "nmcc_prices",
}


class Fact:
    def __init__(
        self,
        topic: str,
        value: Any,
        source_file: str = "",
        source_fragment: str = "",
        source_page_or_sheet: str = "",
        confidence: str = "high",
        status: str = "ok",
        comment: str = "",
    ):
        self.topic = topic
        self.value = value
        self.source_file = source_file
        self.source_fragment = source_fragment
        self.source_page_or_sheet = source_page_or_sheet
        self.confidence = confidence
        self.status = status
        self.comment = comment

    def to_dict(self) -> Dict[str, Any]:
        return {
            "topic": self.topic,
            "value": self.value,
            "source_file": self.source_file,
            "source_fragment": self.source_fragment,
            "source_page_or_sheet": self.source_page_or_sheet,
            "confidence": self.confidence,
            "status": self.status,
            "comment": self.comment,
        }


class FactExtractionService:
    """
    Новый режим:
    - детерминированное извлечение кодом
    - 1 AI-вызов на все темы сразу
    - merge без ложных конфликтов между дополняющими сведениями
    """

    def __init__(self, ai_service):
        self.ai_service = ai_service
        self.chunk_cache: Dict[Any, List[Dict[str, Any]]] = {}

    def normalize_number(self, text: str) -> Optional[float]:
        if not text:
            return None
        clean_text = re.sub(r"\s+", "", str(text))
        match = re.search(r"(\d+[.,\d]*)", clean_text)
        if not match:
            return None
        num_str = match.group(1).replace(",", ".")
        try:
            return float(num_str)
        except ValueError:
            return None

    def extract_deterministic_facts(self, files_data: List[Dict[str, Any]]) -> List[Fact]:
        facts: List[Fact] = []

        for file in files_data:
            filename = file.get("filename", "Unknown")
            text = file.get("text", "") or ""
            status = file.get("status", "ok")

            if status != "ok":
                facts.append(
                    Fact(
                        topic="file_status",
                        value=status,
                        source_file=filename,
                        status=status,
                        comment=file.get("error_message", ""),
                    )
                )
                continue

            # НМЦК
            nmcc_matches = re.finditer(
                r"(нмцк|начальная\s*\(?максимальная\)?\s*цена\s*контракта)[\s:.-]*([\d\s.,]+)\s*(руб|₽)",
                text,
                re.IGNORECASE,
            )
            for match in nmcc_matches:
                raw_val = match.group(2)
                norm_val = self.normalize_number(raw_val)
                if norm_val is not None:
                    fragment = text[max(0, match.start() - 80): min(len(text), match.end() + 80)]
                    facts.append(
                        Fact(
                            topic="nmcc_prices",
                            value={"raw": raw_val.strip(), "normalized": norm_val, "type": "nmcc"},
                            source_file=filename,
                            source_fragment=fragment,
                            confidence="high",
                            status="ok",
                        )
                    )

            # Аванс
            advance_matches = re.finditer(
                r"(аванс|авансовый\s*платеж)[^\.]{0,120}?(\d+(?:[.,]\d+)?)\s*%",
                text,
                re.IGNORECASE,
            )
            for match in advance_matches:
                raw_val = match.group(2)
                norm_val = self.normalize_number(raw_val)
                if norm_val is not None:
                    fragment = text[max(0, match.start() - 80): min(len(text), match.end() + 80)]
                    facts.append(
                        Fact(
                            topic="payment",
                            value={
                                "raw": f"{raw_val.strip()}%",
                                "normalized": norm_val,
                                "type": "advance_percent",
                            },
                            source_file=filename,
                            source_fragment=fragment,
                            confidence="medium",
                            status="ok",
                        )
                    )

            # Пени / штрафы
            penalty_matches = re.finditer(
                r"(штраф|пеня|пени|неустойка)[^\.]{0,160}?(\d+(?:[.,]\d+)?)\s*%",
                text,
                re.IGNORECASE,
            )
            for match in penalty_matches:
                raw_val = match.group(2)
                norm_val = self.normalize_number(raw_val)
                if norm_val is not None:
                    fragment = text[max(0, match.start() - 80): min(len(text), match.end() + 80)]
                    facts.append(
                        Fact(
                            topic="liability",
                            value={
                                "raw": f"{raw_val.strip()}%",
                                "normalized": norm_val,
                                "type": "penalty_percent",
                            },
                            source_file=filename,
                            source_fragment=fragment,
                            confidence="medium",
                            status="ok",
                        )
                    )

            # Срок оплаты
            payment_days_matches = re.finditer(
                r"оплат[^\.]{0,180}?в\s*течение\s*(\d+)\s*(рабочих|календарных)?\s*дней",
                text,
                re.IGNORECASE,
            )
            for match in payment_days_matches:
                raw_val = match.group(1)
                day_type = match.group(2) or "дней"
                norm_val = self.normalize_number(raw_val)
                if norm_val is not None:
                    fragment = text[max(0, match.start() - 80): min(len(text), match.end() + 80)]
                    facts.append(
                        Fact(
                            topic="payment",
                            value={
                                "raw": f"{raw_val} {day_type}",
                                "normalized": norm_val,
                                "type": "payment_days",
                                "day_type": day_type.lower(),
                            },
                            source_file=filename,
                            source_fragment=fragment,
                            confidence="high",
                            status="ok",
                        )
                    )

            # Эквиваленты
            text_lower = text.lower()
            forbidden_phrases = [
                "эквивалент не допускается",
                "без эквивалента",
                "аналоги не допускаются",
                "поставка эквивалента не предусмотрена",
                "эквивалент не предусмотрен",
                "не допускается поставка эквивалента",
                "не подлежит замене на эквивалент",
                "поставка аналогов не допускается",
                "без аналогов",
            ]
            is_forbidden = any(phrase in text_lower for phrase in forbidden_phrases)
            has_equivalent = "эквивалент" in text_lower or "аналог" in text_lower

            if is_forbidden:
                facts.append(Fact(topic="equivalents", value="Запрещены", source_file=filename, confidence="high"))
            elif has_equivalent:
                facts.append(Fact(topic="equivalents", value="Разрешены", source_file=filename, confidence="medium"))

            # Грубое табличное извлечение для items_quantities
            pages = file.get("pages", []) or []
            for page in pages:
                tables = page.get("tables", []) or []
                for table_text in tables:
                    lines = [line.strip() for line in table_text.split("\n") if line.strip()]
                    if not lines:
                        continue
                    header = lines[0].lower()
                    if (
                        any(kw in header for kw in ["наименование", "товар", "услуг", "работ"])
                        and any(kw in header for kw in ["количеств", "объем", "кол-во"])
                    ):
                        data_lines = lines[1:6]
                        if data_lines:
                            facts.append(
                                Fact(
                                    topic="items_quantities",
                                    value={"raw": "\n".join(data_lines), "type": "table_extract"},
                                    source_file=filename,
                                    source_page_or_sheet=str(page.get("page_num", "")),
                                    confidence="medium",
                                    status="ok",
                                )
                            )

        return facts

    def _chunk_documents(self, files_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        chunks: List[Dict[str, Any]] = []
        chunk_id = 0

        for file in files_data:
            if file.get("status", "ok") != "ok":
                continue

            filename = file.get("filename", "Unknown")
            pages = file.get("pages", []) or []

            if pages:
                for page in pages:
                    page_num = page.get("page_num", "Unknown")
                    text = page.get("text", "") or ""
                    if not text.strip():
                        continue

                    paragraphs = [p.strip() for p in text.split("\n\n") if p.strip()]
                    current = ""
                    for p in paragraphs:
                        if len(current) + len(p) > 1400:
                            if current:
                                chunks.append(
                                    {
                                        "chunk_id": chunk_id,
                                        "source_file": filename,
                                        "source_page_or_sheet": str(page_num),
                                        "text": current.strip(),
                                    }
                                )
                                chunk_id += 1
                            current = p + "\n\n"
                        else:
                            current += p + "\n\n"

                    if current.strip():
                        chunks.append(
                            {
                                "chunk_id": chunk_id,
                                "source_file": filename,
                                "source_page_or_sheet": str(page_num),
                                "text": current.strip(),
                            }
                        )
                        chunk_id += 1
            else:
                text = file.get("text", "") or ""
                if text.strip():
                    chunks.append(
                        {
                            "chunk_id": chunk_id,
                            "source_file": filename,
                            "source_page_or_sheet": "Unknown",
                            "text": text[:1400],
                        }
                    )
                    chunk_id += 1

        return chunks

    def _score_chunk_for_topic(self, chunk_text: str, topic_id: str) -> int:
        text_lower = chunk_text.lower()
        keywords = TOPIC_KEYWORDS.get(topic_id, [])
        return sum(1 for kw in keywords if kw in text_lower)

    def _select_global_context_chunks(self, chunks: List[Dict[str, Any]], max_chunks: int = 18, per_topic: int = 2) -> List[Dict[str, Any]]:
        """
        Выбирает небольшой общий набор чанков для ОДНОГО общего AI-вызова.
        Это резко сокращает размер prompt и убирает fan-out.
        """
        selected: List[Dict[str, Any]] = []
        used_ids = set()

        for topic in TOPIC_CONFIG:
            topic_id = topic["id"]
            scored = []
            for chunk in chunks:
                score = self._score_chunk_for_topic(chunk["text"], topic_id)
                if score > 0:
                    scored.append((score, chunk))
            scored.sort(key=lambda x: x[0], reverse=True)

            for _, chunk in scored[:per_topic]:
                if chunk["chunk_id"] not in used_ids:
                    selected.append(chunk)
                    used_ids.add(chunk["chunk_id"])
                if len(selected) >= max_chunks:
                    break
            if len(selected) >= max_chunks:
                break

        if len(selected) < min(8, len(chunks)):
            for chunk in chunks:
                if chunk["chunk_id"] not in used_ids:
                    selected.append(chunk)
                    used_ids.add(chunk["chunk_id"])
                if len(selected) >= max_chunks:
                    break

        return selected[:max_chunks]

    def _build_structured_hints(self, facts: List[Fact]) -> Dict[str, Any]:
        """
        Короткие серверные подсказки для общего AI-вызова.
        """
        grouped: Dict[str, List[Dict[str, Any]]] = {}
        for fact in facts:
            if fact.status not in ("ok", "partial"):
                continue
            grouped.setdefault(fact.topic, []).append(
                {
                    "value": fact.value,
                    "source_file": fact.source_file,
                    "source_page_or_sheet": fact.source_page_or_sheet,
                    "confidence": fact.confidence,
                }
            )
        return grouped

    def extract_thematic_facts_ai(
        self,
        files_data: List[Dict[str, Any]],
        existing_facts: List[Fact],
        tender_id: str = "N/A",
        job_id: str = "N/A",
    ) -> List[Fact]:
        """
        НОВАЯ ЛОГИКА:
        один AI-вызов на все темы сразу.
        """
        for file in files_data:
            log_debug_event(
                {
                    "stage": "document_extraction",
                    "job_id": job_id,
                    "tender_id": tender_id,
                    "filename": file.get("filename", "Unknown"),
                    "status": file.get("status", "ok"),
                    "native_text_length": len(file.get("text", "") or ""),
                    "error_message": file.get("error_message", ""),
                }
            )

        files_key = tuple(sorted([f.get("filename", "") for f in files_data]))
        cache_key = (tender_id, files_key)

        if cache_key in self.chunk_cache:
            chunks = self.chunk_cache[cache_key]
            logger.info(f"Using cached chunks for tender {tender_id}")
        else:
            chunks = self._chunk_documents(files_data)
            self.chunk_cache[cache_key] = chunks

        selected_chunks = self._select_global_context_chunks(chunks)
        compact_context = ""
        for chunk in selected_chunks:
            compact_context += (
                f"\n--- CHUNK {chunk['chunk_id']} "
                f"(Файл: {chunk['source_file']}, Страница/Лист: {chunk['source_page_or_sheet']}) ---\n"
                f"{chunk['text']}\n"
            )

        structured_hints = json.dumps(self._build_structured_hints(existing_facts), ensure_ascii=False, indent=2)
        prompt = (
            PROMPT_EXTRACT_FACTS
            .replace("__STRUCTURED_DATA__", structured_hints)
            .replace("__TEXT__", compact_context)
        )

        all_facts: List[Fact] = list(existing_facts)

        try:
            from google.genai import types

            response = self.ai_service._call_ai_with_retry(
                self.ai_service.client.models.generate_content,
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                ),
            )

            raw_response = response.text if response else ""
            parsed = json.loads(raw_response) if raw_response else {}

            for topic in TOPIC_CONFIG:
                topic_id = topic["id"]
                node = parsed.get(topic_id, {}) or {}
                status = node.get("status", "not_found")
                summary = node.get("summary", "") or ""
                values = node.get("values", []) or []

                if values:
                    for item in values:
                        all_facts.append(
                            Fact(
                                topic=topic_id,
                                value=item.get("value"),
                                source_file=item.get("source_file", ""),
                                source_fragment=item.get("source_fragment", ""),
                                source_page_or_sheet=item.get("source_page_or_sheet", ""),
                                confidence=item.get("confidence", "medium"),
                                status=status,
                                comment=item.get("comment", "") or summary,
                            )
                        )
                else:
                    if status == "ok" and summary:
                        all_facts.append(
                            Fact(
                                topic=topic_id,
                                value=summary,
                                source_file="",
                                source_fragment=summary[:500],
                                source_page_or_sheet="",
                                confidence="medium",
                                status="ok",
                                comment="AI summary without explicit values",
                            )
                        )
                    else:
                        all_facts.append(
                            Fact(
                                topic=topic_id,
                                value=None,
                                status="not_found" if status != "conflict" else "conflict",
                                comment=summary,
                            )
                        )

            log_debug_event(
                {
                    "stage": "ai_single_fact_extraction",
                    "job_id": job_id,
                    "tender_id": tender_id,
                    "model_name": "gemini-single-pass",
                    "chunk_count": len(selected_chunks),
                    "input_fragments": [c["chunk_id"] for c in selected_chunks],
                    "source_files": list(sorted(set(c["source_file"] for c in selected_chunks))),
                    "total_text_size": len(compact_context),
                    "raw_model_response": raw_response,
                }
            )

        except Exception as e:
            logger.error(f"Single AI fact extraction failed for tender {tender_id}: {e}", exc_info=True)
            log_debug_event(
                {
                    "stage": "ai_single_fact_extraction_error",
                    "job_id": job_id,
                    "tender_id": tender_id,
                    "error": str(e),
                }
            )

        return all_facts

    def _fact_weight(self, fact: Fact) -> int:
        weight = 10
        if fact.confidence == "high":
            weight += 10
        elif fact.confidence == "medium":
            weight += 5
        elif fact.confidence == "low":
            weight += 1

        if fact.status == "partial":
            weight -= 3
        if fact.status == "ocr_failed":
            weight -= 7
        return weight

    def _normalize_string(self, value: Any) -> str:
        if value is None:
            return ""
        text = str(value).strip().lower()
        text = re.sub(r"\s+", " ", text)
        text = re.sub(r"[^\w\s\dа-яё]", "", text, flags=re.IGNORECASE)
        return text.strip()

    def _get_normalized_key(self, topic: str, value: Any) -> str:
        if value is None:
            return "none"

        if isinstance(value, dict):
            if "normalized" in value:
                vtype = value.get("type", "value")
                return f"dict_num:{topic}:{vtype}:{value.get('normalized')}"
            if "raw" in value:
                return f"dict_raw:{topic}:{self._normalize_string(value.get('raw'))}"

        if isinstance(value, list):
            return "list:" + "|".join(sorted(self._normalize_string(v) for v in value))

        return f"str:{topic}:{self._normalize_string(value)}"

    def _build_all_sources(self, facts: List[Fact]) -> List[Dict[str, Any]]:
        return [
            {
                "file": f.source_file,
                "page": f.source_page_or_sheet,
                "fragment": f.source_fragment,
                "value": f.value,
                "confidence": f.confidence,
                "status": f.status,
                "comment": f.comment,
            }
            for f in facts
        ]

    def _merge_select_topic(self, topic: str, facts: List[Fact]) -> Dict[str, Any]:
        groups: Dict[str, List[Fact]] = {}
        for fact in facts:
            key = self._get_normalized_key(topic, fact.value)
            groups.setdefault(key, []).append(fact)

        if len(groups) == 1:
            first = facts[0]
            return {
                "topic": topic,
                "merge_mode": "select",
                "final_value": first.value,
                "all_sources": self._build_all_sources(facts),
                "conflict_flag": False,
                "explanation": "Найдено одно согласованное значение.",
            }

        ranked = []
        for key, group_facts in groups.items():
            ranked.append(
                {
                    "key": key,
                    "facts": group_facts,
                    "weight": sum(self._fact_weight(f) for f in group_facts),
                    "value": group_facts[0].value,
                }
            )

        ranked.sort(key=lambda x: x["weight"], reverse=True)

        if len(ranked) >= 2 and ranked[0]["weight"] >= ranked[1]["weight"] * 2:
            return {
                "topic": topic,
                "merge_mode": "select",
                "final_value": ranked[0]["value"],
                "all_sources": self._build_all_sources(facts),
                "conflict_flag": False,
                "explanation": f"Найдено несколько вариантов, выбран наиболее надежный (вес {ranked[0]['weight']} против {ranked[1]['weight']}).",
            }

        variants = []
        for item in ranked[:5]:
            files = sorted(set(f.source_file for f in item["facts"] if f.source_file))
            variants.append({"value": item["value"], "files": files, "weight": item["weight"]})

        return {
            "topic": topic,
            "merge_mode": "select",
            "final_value": "conflict",
            "all_sources": self._build_all_sources(facts),
            "conflict_flag": True,
            "explanation": f"Обнаружены несовместимые значения: {variants}",
        }

    def _merge_aggregate_topic(self, topic: str, facts: List[Fact]) -> Dict[str, Any]:
        unique_items: List[Dict[str, Any]] = []
        seen = set()

        for fact in sorted(facts, key=self._fact_weight, reverse=True):
            key = self._get_normalized_key(topic, fact.value)
            if key in seen:
                continue
            seen.add(key)
            unique_items.append(
                {
                    "value": fact.value,
                    "source_file": fact.source_file,
                    "source_page_or_sheet": fact.source_page_or_sheet,
                    "confidence": fact.confidence,
                    "comment": fact.comment,
                }
            )

        explicit_conflict = any(f.status == "conflict" for f in facts)

        return {
            "topic": topic,
            "merge_mode": "aggregate",
            "final_value": unique_items,
            "all_sources": self._build_all_sources(facts),
            "conflict_flag": explicit_conflict,
            "explanation": "Собраны дополняющие сведения по теме." if unique_items else "Данные не найдены.",
        }

    def merge_facts(self, facts: List[Fact], tender_id: str = "N/A") -> Dict[str, Any]:
        merged: Dict[str, Any] = {}
        grouped: Dict[str, List[Fact]] = {}

        for fact in facts:
            grouped.setdefault(fact.topic, []).append(fact)

        for topic in [t["id"] for t in TOPIC_CONFIG] + ["file_status"]:
            topic_facts = grouped.get(topic, [])
            valid_facts = [
                f for f in topic_facts
                if f.status not in ("not_found", "error", "empty") and f.value not in (None, "", [], {})
            ]

            if not valid_facts:
                merged[topic] = {
                    "topic": topic,
                    "merge_mode": "none",
                    "final_value": "not_found",
                    "all_sources": [],
                    "conflict_flag": False,
                    "explanation": "Данные не найдены ни в одном источнике.",
                }
                log_debug_event(
                    {
                        "stage": "merge",
                        "tender_id": tender_id,
                        "topic": topic,
                        "merge_result": "not_found",
                        "conflict_flag": False,
                    }
                )
                continue

            if topic in SELECT_TOPICS:
                topic_result = self._merge_select_topic(topic, valid_facts)
            else:
                topic_result = self._merge_aggregate_topic(topic, valid_facts)

            merged[topic] = topic_result

            log_debug_event(
                {
                    "stage": "merge",
                    "tender_id": tender_id,
                    "topic": topic,
                    "raw_values": [str(f.value) for f in valid_facts],
                    "merge_result": str(topic_result.get("final_value")),
                    "conflict_flag": topic_result.get("conflict_flag", False),
                    "merge_mode": topic_result.get("merge_mode"),
                    "explanation": topic_result.get("explanation", ""),
                }
            )

        return merged
