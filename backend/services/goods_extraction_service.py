import json
import re
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

from backend.logger import logger
from .ai_service import AiService


PROMPT_UNIFIED_GOODS_EXTRACTION = """
Ты — эксперт по анализу тендерной документации на поставку товаров и строительных материалов.

ТВОЯ ЦЕЛЬ:
Из всей документации закупки выделить только данные о закупаемом товаре:
1) товарные позиции,
2) количества,
3) единицы измерения,
4) конкретные технические характеристики,
5) общие требования к качеству, упаковке, происхождению, сертификатам, гарантиям,
6) признаки допуска аналога / эквивалента,
7) ссылки на источник (файл, страница, лист, таблица).

ТЕБЕ ПЕРЕДАЮТ НЕ ОТДЕЛЬНОЕ ТЗ, А ВЕСЬ ПАКЕТ ДОКУМЕНТАЦИИ:
включая извещение, документацию, описание объекта закупки, проект договора, приложения, спецификации, excel-листы, сметы и прочее.

ОБЯЗАТЕЛЬНОЕ ПРАВИЛО:
Сначала классифицируй каждый фрагмент документа как один из типов:
- GOODS_SPEC — товарная спецификация / таблица поставки / описание объекта закупки / ТЗ;
- GOODS_GENERAL_REQUIREMENTS — общие требования к товару;
- CONTRACT_TERMS — условия договора, оплаты, ответственности, приемки, расторжения;
- PROCUREMENT_RULES — требования к участнику, состав заявки, критерии оценки;
- PRICE_JUSTIFICATION — НМЦК, расчеты, сметы, коммерческие предложения;
- OTHER — прочее.

ДАЛЬШЕ:
1. Используй для извлечения позиций только фрагменты типа GOODS_SPEC.
2. Используй для общих требований к товару только GOODS_GENERAL_REQUIREMENTS.
3. CONTRACT_TERMS, PROCUREMENT_RULES и PRICE_JUSTIFICATION НЕ должны превращаться в товарные позиции.
4. Если спецификация встречается и в ТЗ, и в приложении к договору, считай это дублем одной и той же позиции и не дублируй её.
5. Если характеристики даны отдельно от таблицы позиций, привяжи их ко всем позициям или к тем позициям, к которым они явно относятся.
6. Если товар задан точным наименованием/маркой — сохраняй точное наименование.
7. Если товар задан параметрически — собирай нормализованное название из типа товара и ключевых признаков.
8. Не подменяй отсутствующие данные догадками.
9. Не извлекай:
   - оплату,
   - штрафы,
   - пени,
   - сроки подписания,
   - банковские реквизиты,
   - требования к участнику,
   - формы заявок,
   - НМЦК,
   - национальный режим,
   - протоколы и служебные разделы,
   если они не содержат прямых требований к самому товару.
10. Если количество или единица измерения указаны только в дублирующем документе (например, в спецификации к договору), а характеристики — в ТЗ, объедини данные в одну позицию.

ФОРМАТ ОТВЕТА — СТРОГО JSON И НИЧЕГО КРОМЕ JSON:

{
  "positions": [
    {
      "position_id": 1,
      "position_name_raw": "...",
      "position_name_normalized": "...",
      "quantity": "...",
      "unit": "...",
      "characteristics": [
        {"name": "...", "value": "..."}
      ],
      "general_requirements_applied": true,
      "analog_allowed": true,
      "manufacturer_or_brand_required": false,
      "source_documents": [
        {
          "file": "...",
          "page_or_sheet": "...",
          "fragment_type": "GOODS_SPEC",
          "evidence": "короткая цитата"
        }
      ],
      "notes": "..."
    }
  ],
  "general_goods_requirements": [
    {
      "name": "...",
      "value": "...",
      "source_documents": [
        {
          "file": "...",
          "page_or_sheet": "...",
          "fragment_type": "GOODS_GENERAL_REQUIREMENTS",
          "evidence": "короткая цитата"
        }
      ]
    }
  ],
  "extraction_summary": {
    "positions_count": 0,
    "duplicates_merged": 0,
    "ignored_fragments_types": ["CONTRACT_TERMS", "PROCUREMENT_RULES", "PRICE_JUSTIFICATION"],
    "warnings": []
  }
}

ОСОБЫЕ ПРАВИЛА НОРМАЛИЗАЦИИ:
- Если в позиции есть марка типа ТКП, ТПП, ЭПП, ХПП и т.п. — сохраняй её в normalized name.
- Если есть "или эквивалент" / "аналог" — это не часть названия товара, а отдельный признак analog_allowed.
- Если общие требования относятся ко всем товарам, не копируй их в text blob, а вынеси в general_goods_requirements.
- Если фрагмент содержит только юр./процедурную информацию — не создавай из него позицию.
- Если видишь повтор позиции в проекте договора и в ТЗ — оставь одну запись, но сохрани оба источника.

ТЕПЕРЬ ПРОАНАЛИЗИРУЙ ПАКЕТ ДОКУМЕНТОВ НИЖЕ:

__DOCUMENTS__
""".strip()


SUPPORTED_ARCHIVES = {".7z", ".zip", ".rar"}


class GoodsExtractionService:
    """
    Извлечение товарных позиций из всей документации по аналогии с LegalAnalysisService.
    """

    def __init__(self, ai_service: Optional[AiService] = None):
        self.ai_service = ai_service or AiService()
        logger.info("GoodsExtractionService initialized (unified goods prompt mode).")

    def extract_goods_requirements(
        self,
        files_data: List[Dict[str, Any]],
        tender_id: str = "N/A",
        job_id: str = "N/A",
        callback: Optional[Callable[[str, int, str], None]] = None,
    ) -> Dict[str, Any]:
        documents_block, context_meta = self._build_goods_documents_block(files_data)

        logger.info(
            f"[GOODS_EXTRACTION_START] tender_id={tender_id} job_id={job_id} "
            f"files_in_packet={context_meta['documents_count']} "
            f"archives_skipped={len(context_meta['archives_skipped'])}"
        )

        if callback:
            callback("Подготовка контекста товарных документов", 20, "running")

        if not documents_block.strip():
            warnings = ["Не удалось подготовить goods documents block для извлечения ТЗ"]
            if context_meta["archives_skipped"]:
                warnings.append(
                    "В пакете обнаружены архивы, которые не были распакованы до извлечения: "
                    + ", ".join(item["filename"] for item in context_meta["archives_skipped"])
                )
            return self._empty_result(context_meta, warnings)

        prompt = PROMPT_UNIFIED_GOODS_EXTRACTION.replace("__DOCUMENTS__", documents_block)

        try:
            if callback:
                callback("Извлечение товарных позиций из всей документации", 55, "running")

            start_time = time.time()

            response = self.ai_service._call_ai_with_retry(
                self.ai_service.client.models.generate_content,
                contents=prompt,
            )

            raw_text = response.text if response else ""
            parsed = self._parse_json_response(raw_text)

            normalized = self._normalize_extraction_result(parsed, context_meta)
            duration = time.time() - start_time

            logger.info(
                f"[GOODS_EXTRACTION_DONE] tender_id={tender_id} "
                f"positions={len(normalized['positions'])} duration={duration:.2f}s"
            )
            return normalized

        except Exception as e:
            logger.error(f"Goods extraction error for tender {tender_id}: {e}", exc_info=True)
            return self._empty_result(
                context_meta,
                [f"Ошибка извлечения товарных данных: {str(e)}"]
            )

    def _empty_result(self, context_meta: Dict[str, Any], warnings: List[str]) -> Dict[str, Any]:
        return {
            "positions": [],
            "general_goods_requirements": [],
            "warnings": warnings,
            "debug": context_meta,
            "extraction_summary": {
                "positions_count": 0,
                "duplicates_merged": 0,
                "ignored_fragments_types": ["CONTRACT_TERMS", "PROCUREMENT_RULES", "PRICE_JUSTIFICATION"],
                "warnings": warnings,
            }
        }

    def _document_priority_for_goods(self, filename: str) -> Tuple[int, str]:
        name = (filename or "").lower()

        # Самые приоритетные документы для товаров
        if "описан" in name or "объект" in name or "тех" in name or "тз" in name:
            return (1, "goods_main")
        if "специфик" in name or "ведомост" in name or "номенклат" in name:
            return (2, "goods_spec")
        if name.endswith(".xls") or name.endswith(".xlsx"):
            return (3, "goods_excel")
        if "проект" in name or "контракт" in name or "договор" in name:
            return (4, "contract")
        if "извещ" in name:
            return (5, "notice")
        if "нмцк" in name or "обоснован" in name or "смет" in name:
            return (6, "price")
        return (10, "other")

    def _is_archive(self, filename: str) -> bool:
        ext = ""
        if "." in (filename or ""):
            ext = "." + filename.lower().rsplit(".", 1)[-1]
        return ext in SUPPORTED_ARCHIVES

    def _clean_text(self, text: str) -> str:
        if not text:
            return ""
        text = text.replace("\xa0", " ")
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    def _classify_text_fragment(self, text: str) -> str:
        low = (text or "").lower()

        goods_spec_markers = [
            "техническое задание", "описание объекта закупки", "спецификация",
            "наименование товара", "характеристики", "ведомость материалов",
            "товар", "материал", "ед. изм", "количество", "кол-во"
        ]
        goods_general_markers = [
            "товар должен быть новым", "гарантийный срок", "сертификат",
            "декларация соответствия", "упаковка", "маркировка",
            "общие требования к товарам", "общие требования к товару"
        ]
        contract_markers = [
            "оплата", "штраф", "пеня", "неустойка", "расторжение",
            "ответственность сторон", "порядок приемки", "приемка",
            "расчеты", "реквизиты", "порядок расчетов"
        ]
        procurement_markers = [
            "требования к участнику", "состав заявки", "критерии оценки",
            "инструкция", "форма заявки", "обеспечение заявки",
            "банковская гарантия", "участник закупки"
        ]
        price_markers = [
            "нмцк", "обоснование цены", "обоснование начальной",
            "смета", "коммерческое предложение", "расчет цены"
        ]

        if any(marker in low for marker in goods_general_markers):
            return "GOODS_GENERAL_REQUIREMENTS"
        if any(marker in low for marker in goods_spec_markers):
            return "GOODS_SPEC"
        if any(marker in low for marker in contract_markers):
            return "CONTRACT_TERMS"
        if any(marker in low for marker in procurement_markers):
            return "PROCUREMENT_RULES"
        if any(marker in low for marker in price_markers):
            return "PRICE_JUSTIFICATION"

        # fallback: если есть признак табличной товарной строки
        if "|" in low and re.search(r"\b(товар|материал|характерист|кол-во|количество|ед\.?\s*изм)\b", low):
            return "GOODS_SPEC"

        return "OTHER"

    def _render_goods_pages(self, file_data: Dict[str, Any]) -> str:
        filename = file_data.get("filename", "Unknown")
        status = file_data.get("status", "success")
        pages = file_data.get("pages", []) or []
        text = file_data.get("extracted_text", "") or ""
        priority, doc_hint = self._document_priority_for_goods(filename)

        header = (
            f"=== FILE: {filename} | DOC_CLASS_HINT: {doc_hint} | "
            f"PRIORITY: {priority} | STATUS: {status} | TEXT_LEN: {len(text)} ===\n"
        )

        if status != "success":
            err = file_data.get("error_message", "") or status
            return header + f"[FILE_WARNING] {err}\n"

        if not pages:
            cleaned = self._clean_text(text)
            frag_type = self._classify_text_fragment(cleaned)
            return header + f"--- PAGE_OR_SHEET: 1 | FRAGMENT_TYPE_HINT: {frag_type} ---\n[TEXT]\n{cleaned}\n"

        blocks: List[str] = [header]
        for page in pages:
            page_num = page.get("page_num", "")
            page_text = self._clean_text(page.get("text", "") or "")
            tables = page.get("tables", []) or []

            all_fragment_text = page_text
            if tables:
                all_fragment_text += "\n\n" + "\n\n".join(self._clean_text(t) for t in tables if t)
            fragment_type = self._classify_text_fragment(all_fragment_text)

            blocks.append(
                f"--- PAGE_OR_SHEET: {page_num} | FRAGMENT_TYPE_HINT: {fragment_type} ---"
            )

            if page_text:
                blocks.append("[TEXT]")
                blocks.append(page_text)

            if tables:
                blocks.append("[TABLES]")
                for idx, table_text in enumerate(tables, start=1):
                    table_clean = self._clean_text(table_text or "")
                    if table_clean:
                        blocks.append(f"[TABLE {idx}]")
                        blocks.append(table_clean)

        return "\n\n".join(blocks).strip() + "\n"

    def _build_goods_documents_block(
        self,
        files_data: List[Dict[str, Any]],
        max_total_chars: int = 180000,
    ) -> Tuple[str, Dict[str, Any]]:
        sorted_files = sorted(files_data, key=lambda f: self._document_priority_for_goods(f.get("filename", "")))

        included_files: List[str] = []
        skipped_files: List[Dict[str, Any]] = []
        archives_skipped: List[Dict[str, Any]] = []
        rendered_parts: List[str] = []
        total_chars = 0

        for file_data in sorted_files:
            filename = file_data.get("filename", "Unknown")
            status = file_data.get("status", "success")

            if self._is_archive(filename):
                archives_skipped.append({
                    "filename": filename,
                    "reason": "archive_not_supported_for_direct_analysis"
                })
                continue

            if status != "success":
                skipped_files.append({
                    "filename": filename,
                    "reason": f"status_{status}",
                    "error_message": file_data.get("error_message", "")
                })
                continue

            rendered = self._render_goods_pages(file_data)
            if not rendered.strip():
                skipped_files.append({"filename": filename, "reason": "empty_render"})
                continue

            if total_chars + len(rendered) > max_total_chars:
                skipped_files.append({"filename": filename, "reason": "max_total_chars_exceeded"})
                continue

            rendered_parts.append(rendered)
            included_files.append(filename)
            total_chars += len(rendered)

        block = "\n\n".join(rendered_parts).strip()
        meta = {
            "documents_count": len(included_files),
            "included_files": included_files,
            "skipped_files": skipped_files,
            "archives_skipped": archives_skipped,
            "total_chars": total_chars,
        }
        return block, meta

    def _parse_json_response(self, raw_text: str) -> Dict[str, Any]:
        text = (raw_text or "").strip()

        if text.startswith("```json"):
            text = text.replace("```json", "", 1).replace("```", "", 1).strip()
        elif text.startswith("```"):
            text = text.replace("```", "", 1).replace("```", "", 1).strip()

        match = re.search(r"\{[\s\S]*\}", text)
        if match:
            text = match.group(0).strip()

        return json.loads(text)

    def _normalize_string(self, value: Any) -> str:
        if value is None:
            return ""
        value = str(value).replace("\xa0", " ").strip()
        value = re.sub(r"\s{2,}", " ", value)
        return value

    def _normalize_quantity(self, value: Any) -> str:
        text = self._normalize_string(value)
        text = text.replace(" ", "")
        return text

    def _normalize_position_name(self, value: str) -> str:
        text = self._normalize_string(value)
        text = re.sub(r"^[0-9]+[.)]?\s*", "", text)
        text = re.sub(r"\bили\s+эквивалент\b", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\bэквивалент\b", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\s{2,}", " ", text)
        return text.strip(" ,;:-")

    def _normalize_characteristics(self, characteristics: Any) -> List[Dict[str, str]]:
        result: List[Dict[str, str]] = []

        if not isinstance(characteristics, list):
            return result

        seen = set()
        for item in characteristics:
            if not isinstance(item, dict):
                continue
            name = self._normalize_string(item.get("name"))
            value = self._normalize_string(item.get("value"))
            if not name and not value:
                continue
            key = (name.lower(), value.lower())
            if key in seen:
                continue
            seen.add(key)
            result.append({"name": name, "value": value})

        return result

    def _normalize_source_documents(self, sources: Any) -> List[Dict[str, str]]:
        result: List[Dict[str, str]] = []
        if not isinstance(sources, list):
            return result

        seen = set()
        for item in sources:
            if not isinstance(item, dict):
                continue
            file_name = self._normalize_string(item.get("file"))
            page_or_sheet = self._normalize_string(item.get("page_or_sheet"))
            fragment_type = self._normalize_string(item.get("fragment_type"))
            evidence = self._normalize_string(item.get("evidence"))

            if not file_name and not evidence:
                continue

            key = (file_name.lower(), page_or_sheet.lower(), fragment_type.lower(), evidence.lower())
            if key in seen:
                continue
            seen.add(key)

            result.append({
                "file": file_name,
                "page_or_sheet": page_or_sheet,
                "fragment_type": fragment_type,
                "evidence": evidence[:500]
            })

        return result

    def _position_key(self, item: Dict[str, Any]) -> str:
        name = self._normalize_position_name(
            item.get("position_name_normalized")
            or item.get("position_name_raw")
            or ""
        ).lower()
        name = re.sub(r"[^a-zа-я0-9]+", " ", name, flags=re.IGNORECASE)
        name = re.sub(r"\s{2,}", " ", name).strip()
        return name

    def _merge_positions(self, positions: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], int]:
        merged: Dict[str, Dict[str, Any]] = {}
        duplicates_merged = 0

        for item in positions:
            key = self._position_key(item)
            if not key:
                continue

            if key not in merged:
                merged[key] = item
                continue

            duplicates_merged += 1
            base = merged[key]

            if not base.get("quantity") and item.get("quantity"):
                base["quantity"] = item.get("quantity")
            if not base.get("unit") and item.get("unit"):
                base["unit"] = item.get("unit")

            existing_chars = {(c["name"].lower(), c["value"].lower()) for c in base.get("characteristics", [])}
            for char in item.get("characteristics", []):
                k = (char["name"].lower(), char["value"].lower())
                if k not in existing_chars:
                    base.setdefault("characteristics", []).append(char)
                    existing_chars.add(k)

            existing_sources = {
                (
                    s.get("file", "").lower(),
                    s.get("page_or_sheet", "").lower(),
                    s.get("fragment_type", "").lower(),
                    s.get("evidence", "").lower(),
                )
                for s in base.get("source_documents", [])
            }
            for source in item.get("source_documents", []):
                k = (
                    source.get("file", "").lower(),
                    source.get("page_or_sheet", "").lower(),
                    source.get("fragment_type", "").lower(),
                    source.get("evidence", "").lower(),
                )
                if k not in existing_sources:
                    base.setdefault("source_documents", []).append(source)
                    existing_sources.add(k)

            if not base.get("notes") and item.get("notes"):
                base["notes"] = item.get("notes")

            if item.get("general_requirements_applied"):
                base["general_requirements_applied"] = True

        final = list(merged.values())
        for idx, item in enumerate(final, start=1):
            item["position_id"] = idx

        return final, duplicates_merged

    def _normalize_extraction_result(
        self,
        parsed: Dict[str, Any],
        context_meta: Dict[str, Any]
    ) -> Dict[str, Any]:
        raw_positions = parsed.get("positions", []) if isinstance(parsed, dict) else []
        raw_general = parsed.get("general_goods_requirements", []) if isinstance(parsed, dict) else []
        raw_summary = parsed.get("extraction_summary", {}) if isinstance(parsed, dict) else {}

        positions: List[Dict[str, Any]] = []
        for item in raw_positions:
            if not isinstance(item, dict):
                continue

            raw_name = self._normalize_string(item.get("position_name_raw"))
            norm_name = self._normalize_position_name(
                item.get("position_name_normalized") or raw_name
            )
            if not raw_name and not norm_name:
                continue

            positions.append({
                "position_id": 0,
                "position_name_raw": raw_name or norm_name,
                "position_name_normalized": norm_name or raw_name,
                "quantity": self._normalize_quantity(item.get("quantity")),
                "unit": self._normalize_string(item.get("unit")),
                "characteristics": self._normalize_characteristics(item.get("characteristics")),
                "general_requirements_applied": bool(item.get("general_requirements_applied")),
                "analog_allowed": bool(item.get("analog_allowed")),
                "manufacturer_or_brand_required": bool(item.get("manufacturer_or_brand_required")),
                "source_documents": self._normalize_source_documents(item.get("source_documents")),
                "notes": self._normalize_string(item.get("notes")),
            })

        positions, duplicates_merged = self._merge_positions(positions)

        general_goods_requirements: List[Dict[str, Any]] = []
        seen_general = set()
        for item in raw_general:
            if not isinstance(item, dict):
                continue
            name = self._normalize_string(item.get("name"))
            value = self._normalize_string(item.get("value"))
            if not name and not value:
                continue
            key = (name.lower(), value.lower())
            if key in seen_general:
                continue
            seen_general.add(key)
            general_goods_requirements.append({
                "name": name,
                "value": value,
                "source_documents": self._normalize_source_documents(item.get("source_documents"))
            })

        warnings = []
        if context_meta["archives_skipped"]:
            warnings.append(
                "В пакете есть архивы, не пригодные для прямого анализа без распаковки: "
                + ", ".join(item["filename"] for item in context_meta["archives_skipped"])
            )

        if isinstance(raw_summary, dict):
            summary_warnings = raw_summary.get("warnings", [])
            if isinstance(summary_warnings, list):
                for w in summary_warnings:
                    w = self._normalize_string(w)
                    if w and w not in warnings:
                        warnings.append(w)

        result = {
            "positions": positions,
            "general_goods_requirements": general_goods_requirements,
            "warnings": warnings,
            "debug": context_meta,
            "extraction_summary": {
                "positions_count": len(positions),
                "duplicates_merged": duplicates_merged,
                "ignored_fragments_types": ["CONTRACT_TERMS", "PROCUREMENT_RULES", "PRICE_JUSTIFICATION"],
                "warnings": warnings,
            }
        }
        return result
