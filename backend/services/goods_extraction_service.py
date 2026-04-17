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
4) конкретные технические характеристики (масса, толщина, плотность, класс, размер, гост и т.д.),
5) общие требования к качеству, упаковке, происхождению, сертификатам, гарантиям,
6) признаки допуска аналога / эквивалента,
7) ссылки на источник (файл, страница, лист, таблица).

ТЕБЕ ПЕРЕДАЮТ НЕ ОТДЕЛЬНОЕ ТЗ, А ВЕСЬ ПАКЕТ ДОКУМЕНТАЦИИ:
включая извещение, документацию, описание объекта закупки, проект договора, приложения, спецификации, excel-листы, сметы и прочее.

ОБЯЗАТЕЛЬНОЕ ПРАВИЛО КЛАССИФИКАЦИИ (ПРИОРИТЕТЫ):
1. В первую очередь ищи данные в товарных спецификациях, таблицах и описании объекта закупки (GOODS_SPEC).
2. Общие требования к товару (гарантии, качество, новизна) ищи в соответствующих разделах требований (GOODS_GENERAL_REQUIREMENTS).
3. Игнорируй юридическую и финансовую информацию (условия оплаты, штрафы, НМЦК), если там нет характеристик товара. Но если требования к товару разбросаны по всему документу — собери их все.

ПРАВИЛА ИЗВЛЕЧЕНИЯ И НОРМАЛИЗАЦИИ НАИМЕНОВАНИЯ (КРИТИЧЕСКИ ВАЖНО):
1. ЗАПРЕЩАЕТСЯ сводить позицию к общему классу товара.
   ПЛОХО: "Материал кровельный рулонный"
   ХОРОШО: "Материал кровельный рулонный верхний слой ТКП-4,5"
   ПЛОХО: "Мастика битумная"
   ХОРОШО: "Мастика битумная ТехноНИКОЛЬ №24 20 кг" 
2. При нормализации (position_name_normalized) ОБЯЗАТЕЛЬНО сохраняй:
   - марку (например, ТКП, ХПП, ЭПП, Бикрост, Унифлекс, Техноэласт, Линокром, Стеклоизол и т.д.);
   - все буквенные и цифровые индексы (4.5, 4.0, П, К, 3.5, 2.5);
   - тип основы (стеклоткань, стеклохолст, полиэстер, полиэфирное полотно);
   - вид посыпки (сланец, гранулят, песок, пленка);
   - назначение (верхний слой, нижний слой, мостовой, К, П);
   - ключевые размерные признаки (толщина в мм, вес 1м2 в кг, масса рулона).
3. Слова "или эквивалент", "аналог" удаляются из названия товара, но флаг analog_allowed ставится в true.

ПРАВИЛА ИЗВЛЕЧЕНИЯ КОЛИЧЕСТВА И ЕДИНИЦ ИЗМЕРЕНИЯ:
1. Ищи количество и единицу измерения по всей строке таблицы или в соседних столбцах (шт, м2, м, мм, кг, т, л, рулон, упак, мешок, комплект, лист).
2. Обращай внимание на "кг/м2" и подобные — это характеристика, а не единица закупаемого количества. Количество обычно в м2 или шт.
3. Если закупка идет в рулонах, укажи количество рулонов и их площадь, если она известна.
4. ЗАПРЕЩАЕТСЯ выводить quantity: null / "не указано", если эти данные физически есть в переданном тексте.

ПРАВИЛА ИЗВЛЕЧЕНИЯ ХАРАКТЕРИСТИК (ПОЛНЫЙ СБОР):
1. Вытаскивай все технические сведения в поле characteristics (name и value).
2. Для гидроизоляции ВАЖНО: 
   - Толщина полотна (мм); 
   - Масса 1 кв.м. (кг); 
   - Теплостойкость (градусы С); 
   - Гибкость на брусе (градусы С); 
   - Разрывная сила при растяжении (Н); 
   - Водонепроницаемость (МПа).
3. Если указан конкретный ГОСТ (например, ГОСТ 30547-97) или ТУ — обязательно добавь это в характеристики.
4. Собирай характеристики из сложных вложенных таблиц, даже если названия параметров стоят в шапке таблицы за несколько строк до значений.
5. Запрещено оставлять пустой массив characteristics, если в тексте рядом с названием позиции указаны параметры. Поищи параметры в той же ячейке, соседних, или общем тексте ТЗ к этой позиции.

ПРАВИЛА ВЫДЕЛЕНИЯ ОБЩИХ ТРЕБОВАНИЙ:
1. Если в тексте есть общие требования к качеству, новизне, упаковке, сертификации, ГОСТам, срокам годности товара — выдели их в массив `general_goods_requirements`.

ПРАВИЛА ФОРМАТА (ТОЛЬКО JSON):

{
  "positions": [
    {
      "position_id": 1,
      "position_name_raw": "ОРИГИНАЛЬНОЕ НАЗВАНИЕ ИЗ ТЕКСТА",
      "position_name_normalized": "ТОЧНОЕ НАИМЕНОВАНИЕ С МАРКОЙ И СЛОЕМ",
      "quantity": "число или диапазон",
      "unit": "м2, шт, кг и др.",
      "characteristics": [
        {"name": "Толщина", "value": "не менее 3 мм"}
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
      "name": "Требования к новизне",
      "value": "Товар должен быть новым, не бывшим в употреблении...",
      "source_documents": [...]
    }
  ],
  "extraction_summary": {
    "positions_count": 0,
    "duplicates_merged": 0,
    "ignored_fragments_types": ["CONTRACT_TERMS", "PROCUREMENT_RULES", "PRICE_JUSTIFICATION"],
    "warnings": []
  }
}

ПОМНИ: Твоя главная задача — ИЗБЕЖАТЬ ПОТЕРИ ДАННЫХ. Сохрани техническую конкретику товара, его количество и характеристики. Не обобщай!

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
        documents_block, context_meta = self._build_goods_documents_block(files_data, tender_id=tender_id)

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
            
            logger.warning(f"[GOODS_EXTRACTION_WARNING] tender_id={tender_id} reason='no_documents_block' raw_data=''")
            return self._empty_result(context_meta, warnings)

        prompt = PROMPT_UNIFIED_GOODS_EXTRACTION.replace("__DOCUMENTS__", documents_block)

        try:
            if callback:
                callback("Извлечение товарных позиций из всей документации", 55, "running")

            start_time = time.time()
            prompt_chars = len(prompt)

            logger.info(
                f"[GOODS_EXTRACTION_PROMPT_STATS] tender_id={tender_id} job_id={job_id} "
                f"prompt_length_chars={prompt_chars} model_tier='gemini' max_output_tokens='N/A' temperature='N/A'"
            )

            response = self.ai_service._call_ai_with_retry(
                self.ai_service.client.models.generate_content,
                contents=prompt,
            )

            raw_text = response.text if response else ""
            preview = raw_text[:100].replace('\n', ' ').replace('\r', '')

            logger.info(
                f"[GOODS_EXTRACTION_AI_RAW] tender_id={tender_id} job_id={job_id} "
                f"response_length_chars={len(raw_text)} response_preview='{preview}...'"
            )

            parsed = self._parse_json_response(raw_text)

            logger.info(
                f"[GOODS_EXTRACTION_JSON_PARSE] tender_id={tender_id} job_id={job_id} "
                f"status='success' raw_positions_count={len(parsed.get('positions', []))} "
                f"raw_general_requirements_count={len(parsed.get('general_goods_requirements', []))}"
            )

            normalized = self._normalize_extraction_result(parsed, context_meta, tender_id, job_id)
            duration = time.time() - start_time

            logger.info(
                f"[GOODS_EXTRACTION_DONE] tender_id={tender_id} job_id={job_id} "
                f"final_positions_count={len(normalized.get('positions', []))} final_general_requirements_count={len(normalized.get('general_goods_requirements', []))} "
                f"duration_seconds={duration:.2f} errors_count={len(normalized.get('warnings', []))}"
            )
            return normalized

        except Exception as e:
            logger.error(f"Goods extraction error for tender {tender_id}: {e}", exc_info=True)
            logger.info(
                f"[GOODS_EXTRACTION_JSON_PARSE] tender_id={tender_id} job_id={job_id} "
                f"status='failed' raw_positions_count=0 raw_general_requirements_count=0"
            )
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

    def _render_goods_pages(self, file_data: Dict[str, Any], tender_id: str = "N/A") -> str:
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
            logger.info(
                f"[GOODS_CONTEXT_FILE_CLASSIFIED] tender_id={tender_id} filename='{filename}' doc_class_hint='{doc_hint}' "
                f"priority={priority} status='single_block' pages_included=1 tables_included=0 text_chars_included={len(cleaned)}"
            )
            return header + f"--- PAGE_OR_SHEET: 1 | FRAGMENT_TYPE_HINT: {frag_type} ---\n[TEXT]\n{cleaned}\n"

        blocks: List[str] = [header]
        fragments_stats = {
            "GOODS_SPEC": 0,
            "GOODS_GENERAL_REQUIREMENTS": 0,
            "CONTRACT_TERMS": 0,
            "PROCUREMENT_RULES": 0,
            "PRICE_JUSTIFICATION": 0,
            "OTHER": 0,
        }
        tables_count = 0
        text_chars = 0

        for page in pages:
            page_num = page.get("page_num", "")
            page_text = self._clean_text(page.get("text", "") or "")
            tables = page.get("tables", []) or []

            all_fragment_text = page_text
            if tables:
                all_fragment_text += "\n\n" + "\n\n".join(self._clean_text(t) for t in tables if t)
            fragment_type = self._classify_text_fragment(all_fragment_text)

            if fragment_type in fragments_stats:
                fragments_stats[fragment_type] += 1
            else:
                fragments_stats["OTHER"] = fragments_stats.get("OTHER", 0) + 1

            blocks.append(
                f"--- PAGE_OR_SHEET: {page_num} | FRAGMENT_TYPE_HINT: {fragment_type} ---"
            )

            if page_text:
                blocks.append("[TEXT]")
                blocks.append(page_text)
                text_chars += len(page_text)

            if tables:
                blocks.append("[TABLES]")
                for idx, table_text in enumerate(tables, start=1):
                    table_clean = self._clean_text(table_text or "")
                    if table_clean:
                        blocks.append(f"[TABLE {idx}]")
                        blocks.append(table_clean)
                        tables_count += 1
                        text_chars += len(table_clean)

        logger.info(
            f"[GOODS_CONTEXT_FRAGMENT_STATS] tender_id={tender_id} filename='{filename}' "
            f"goods_spec_fragments={fragments_stats['GOODS_SPEC']} goods_general_requirement_fragments={fragments_stats['GOODS_GENERAL_REQUIREMENTS']} "
            f"contract_terms_fragments={fragments_stats['CONTRACT_TERMS']} procurement_rule_fragments={fragments_stats['PROCUREMENT_RULES']} "
            f"price_fragments={fragments_stats['PRICE_JUSTIFICATION']} other_fragments={fragments_stats['OTHER']}"
        )
        
        logger.info(
            f"[GOODS_CONTEXT_FILE_CLASSIFIED] tender_id={tender_id} filename='{filename}' doc_class_hint='{doc_hint}' "
            f"priority={priority} status='success' pages_included={len(pages)} tables_included={tables_count} text_chars_included={text_chars}"
        )

        return "\n\n".join(blocks).strip() + "\n"

    def _build_goods_documents_block(
        self,
        files_data: List[Dict[str, Any]],
        tender_id: str = "N/A",
        max_total_chars: int = 400000,
    ) -> Tuple[str, Dict[str, Any]]:
        logger.info(f"[GOODS_CONTEXT_BUILD_START] tender_id={tender_id} files_count={len(files_data)}")

        sorted_files = sorted(files_data, key=lambda f: self._document_priority_for_goods(f.get("filename", "")))

        included_files: List[str] = []
        skipped_files: List[Dict[str, Any]] = []
        archives_skipped: List[Dict[str, Any]] = []
        rendered_parts: List[str] = []
        total_chars = 0
        total_pages = 0
        total_tables = 0

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

            rendered = self._render_goods_pages(file_data, tender_id=tender_id)
            if not rendered.strip():
                skipped_files.append({"filename": filename, "reason": "empty_render"})
                continue

            if total_chars + len(rendered) > max_total_chars:
                skipped_files.append({"filename": filename, "reason": "max_total_chars_exceeded"})
                continue

            rendered_parts.append(rendered)
            included_files.append(filename)
            total_chars += len(rendered)
            total_pages += len(file_data.get("pages", []))
            total_tables += sum(len(p.get("tables", [])) for p in file_data.get("pages", []))

        block = "\n\n".join(rendered_parts).strip()
        meta = {
            "documents_count": len(included_files),
            "included_files": included_files,
            "skipped_files": skipped_files,
            "archives_skipped": archives_skipped,
            "total_chars": total_chars,
        }

        logger.info(
            f"[GOODS_CONTEXT_BUILD_DONE] tender_id={tender_id} included_files={len(included_files)} skipped_files={len(skipped_files)} "
            f"total_chars={total_chars} total_pages={total_pages} total_tables={total_tables} block_count={len(rendered_parts)} context_truncated={len(skipped_files) > 0}"
        )

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
        context_meta: Dict[str, Any],
        tender_id: str = "N/A",
        job_id: str = "N/A",
    ) -> Dict[str, Any]:
        raw_positions = parsed.get("positions", []) if isinstance(parsed, dict) else []
        raw_general = parsed.get("general_goods_requirements", []) if isinstance(parsed, dict) else []
        raw_summary = parsed.get("extraction_summary", {}) if isinstance(parsed, dict) else {}

        positions: List[Dict[str, Any]] = []
        rejected_count = 0

        for item in raw_positions:
            if not isinstance(item, dict):
                continue

            raw_name = self._normalize_string(item.get("position_name_raw"))
            norm_name = self._normalize_position_name(
                item.get("position_name_normalized") or raw_name
            )
            
            logger.debug(
                f"[GOODS_EXTRACTION_POSITION_RAW] tender_id={tender_id} position_name_raw='{raw_name}' "
                f"quantity='{item.get('quantity')}' unit='{item.get('unit')}' chars_count={len(item.get('characteristics', []))}"
            )

            if not raw_name and not norm_name:
                rejected_count += 1
                logger.warning(
                    f"[GOODS_EXTRACTION_WARNING] tender_id={tender_id} reason='empty_name' "
                    f"raw_data='{json.dumps(item, ensure_ascii=False)[:200]}'"
                )
                continue

            validated_item = {
                "position_id": 0,
                "position_name_raw": raw_name or norm_name,
                "position_name_normalized": norm_name or raw_name,
                "quantity": self._normalize_string(item.get("quantity")), # DO NOT STRIP ALL SPACES
                "unit": self._normalize_string(item.get("unit")),
                "characteristics": self._normalize_characteristics(item.get("characteristics")),
                "general_requirements_applied": bool(item.get("general_requirements_applied")),
                "analog_allowed": bool(item.get("analog_allowed")),
                "manufacturer_or_brand_required": bool(item.get("manufacturer_or_brand_required")),
                "source_documents": self._normalize_source_documents(item.get("source_documents")),
                "notes": self._normalize_string(item.get("notes")),
            }
            positions.append(validated_item)

            logger.debug(
                f"[GOODS_EXTRACTION_POSITION_NORMALIZED] tender_id={tender_id} position_name_normalized='{validated_item['position_name_normalized']}' "
                f"quantity='{validated_item['quantity']}' unit='{validated_item['unit']}' chars_count={len(validated_item['characteristics'])} "
                f"analog_allowed={validated_item['analog_allowed']}"
            )

        positions, duplicates_merged = self._merge_positions(positions)
        
        logger.info(
            f"[GOODS_EXTRACTION_MERGE] tender_id={tender_id} initial_positions={len(raw_positions)} "
            f"rejected_positions={rejected_count} duplicates_merged={duplicates_merged} final_positions={len(positions)}"
        )

        if not positions:
            logger.warning(f"[GOODS_EXTRACTION_WARNING] tender_id={tender_id} reason='no_positions_found' raw_data=''")

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

        positions_with_quantity = sum(1 for p in positions if p.get("quantity") and str(p.get("quantity")).lower() not in ["не указано", "нет", "null", "none", ""])
        positions_with_unit = sum(1 for p in positions if p.get("unit") and str(p.get("unit")).lower() not in ["не указано", "нет", "null", "none", ""])
        positions_with_characteristics = sum(1 for p in positions if p.get("characteristics"))

        if positions:
            missing_qty = len(positions) - positions_with_quantity
            missing_chars = len(positions) - positions_with_characteristics
            if missing_chars == len(positions):
                warnings.append("У найденных позиций отсутствуют характеристики")
            if missing_qty == len(positions):
                warnings.append("У найденных позиций отсутствуют количества из-за неполного распознавания или их отсутствия в тексте")

        quality_summary = {
            "positions_found": len(positions),
            "positions_with_quantity": positions_with_quantity,
            "positions_with_unit": positions_with_unit,
            "positions_with_characteristics": positions_with_characteristics,
            "general_requirements_found": len(general_goods_requirements),
            "archives_blocking": len(context_meta.get("archives_skipped", [])) > 0,
            "critical_degraded": False
        }
        
        # Determine degraded status
        if (len(positions) > 0 and positions_with_characteristics == 0 and positions_with_quantity == 0) or quality_summary["archives_blocking"]:
             quality_summary["critical_degraded"] = True

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
                "quality_summary": quality_summary
            }
        }
        return result
