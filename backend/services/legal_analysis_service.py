import re
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

from backend.logger import logger, log_debug_event
from .ai_service import AiService
from .legal_prompts import PROMPT_UNIFIED_LEGAL_ANALYSIS, REQUIRED_REPORT_HEADERS


class LegalAnalysisService:
    """
    Unified prompt mode with mandatory customer + goods blocks
    and markdown table normalization before export.
    """

    def __init__(self, ai_service: Optional[AiService] = None):
        self.ai_service = ai_service or AiService()
        logger.info("LegalAnalysisService initialized (unified prompt mode, customer+goods required).")

    def analyze_tender(
        self,
        files_data: List[Dict[str, Any]],
        tender_id: str = "N/A",
        job_id: str = "N/A",
        callback: Optional[Callable[[str, int, str], None]] = None,
    ) -> Dict[str, Any]:
        logger.info(f"Starting unified legal analysis for tender {tender_id} (Job: {job_id})")

        if callback:
            callback("Подготовка полного контекста документов", 20, "running")

        try:
            start_time = time.time()

            documents_block, context_meta = self._build_documents_block(files_data)
            prompt = PROMPT_UNIFIED_LEGAL_ANALYSIS.replace("__DOCUMENTS__", documents_block)

            if callback:
                callback("Единый юридический анализ ИИ", 55, "running")

            ai_start = time.time()
            response = self.ai_service._call_ai_with_retry(
                self.ai_service.client.models.generate_content,
                contents=prompt,
            )
            ai_end = time.time()

            raw_text = response.text if response else ""
            final_report_markdown = self._normalize_report(raw_text)
            validation = self._validate_report(final_report_markdown)

            if callback:
                callback("Финальная проверка отчета", 85, "running")

            final_status = self._calculate_status(final_report_markdown, validation)
            summary = self._extract_summary(final_report_markdown)
            end_time = time.time()

            log_debug_event({
                "stage": "unified_prompt_analysis",
                "job_id": job_id,
                "tender_id": tender_id,
                "model_name": "unified-legal-prompt-mode-v2",
                "prompt_size": len(prompt),
                "documents_count": context_meta["documents_count"],
                "included_files": context_meta["included_files"],
                "skipped_files": context_meta["skipped_files"],
                "documents_block_size": len(documents_block),
                "raw_model_response": raw_text,
                "normalized_report_preview": final_report_markdown[:4000],
                "final_status": final_status,
                "validation": validation,
                "duration": end_time - start_time,
                "ai_duration": ai_end - ai_start,
            })

            logger.info(
                f"Unified legal analysis finished for tender {tender_id} "
                f"in {end_time - start_time:.2f}s, status={final_status}, report_len={len(final_report_markdown)}"
            )

            return {
                "status": final_status,
                "final_report_markdown": final_report_markdown,
                "summary_notes": summary,
                "cleaned_context_len": len(documents_block),
                "final_report_len": len(final_report_markdown),
                "structured_data": {},
                "extracted_facts": [],
                "merged_facts": {},
            }

        except Exception as e:
            logger.error(f"Unified legal analysis error for tender {tender_id}: {e}", exc_info=True)
            log_debug_event({
                "stage": "unified_prompt_analysis_error",
                "job_id": job_id,
                "tender_id": tender_id,
                "error": str(e),
            })
            return {
                "status": "error",
                "final_report_markdown": f"# Ошибка анализа\n\nПроизошла ошибка при обработке тендера: {str(e)}",
                "summary_notes": "Ошибка анализа.",
                "cleaned_context_len": 0,
                "final_report_len": 0,
                "structured_data": {},
                "extracted_facts": [],
                "merged_facts": {},
                "error_message": str(e),
            }

    def _document_priority(self, filename: str) -> Tuple[int, str]:
        name = (filename or "").lower()

        if "заявк" in name or "инструкц" in name or "состав" in name:
            return (1, name)
        if "проект" in name or "контракт" in name or "договор" in name or "пгк" in name:
            return (2, name)
        if "извещ" in name:
            return (3, name)
        if "описан" in name or "объект" in name or "тех" in name or "тз" in name:
            return (4, name)
        if "нмцк" in name or "обоснован" in name or "смет" in name:
            return (5, name)
        if name.endswith(".xls") or name.endswith(".xlsx"):
            return (6, name)
        return (10, name)

    def _clean_text(self, text: str) -> str:
        if not text:
            return ""
        text = re.sub(r"\s+\n", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = re.sub(r"[ \t]{2,}", " ", text)
        return text.strip()

    def _render_pages(self, file_data: Dict[str, Any]) -> str:
        filename = file_data.get("filename", "Unknown")
        status = file_data.get("status", "ok")
        error_message = file_data.get("error_message", "")
        pages = file_data.get("pages", []) or []
        text = file_data.get("text", "") or ""

        header = f"=== ФАЙЛ: {filename} | STATUS: {status} ===\n"

        if status != "ok":
            fallback_text = self._clean_text(text)
            return (
                header
                + f"ПРЕДУПРЕЖДЕНИЕ ПО ФАЙЛУ: {error_message or status}\n\n"
                + fallback_text
                + "\n"
            )

        if not pages:
            return header + self._clean_text(text) + "\n"

        blocks: List[str] = [header]
        for page in pages:
            page_num = page.get("page_num", "")
            page_text = self._clean_text(page.get("text", "") or "")
            tables = page.get("tables", []) or []

            page_header = f"--- СТРАНИЦА/ЛИСТ: {page_num} ---"
            blocks.append(page_header)

            if page_text:
                blocks.append(page_text)

            if tables:
                blocks.append("--- ТАБЛИЦЫ ---")
                for idx, table_text in enumerate(tables, start=1):
                    table_clean = self._clean_text(table_text or "")
                    if table_clean:
                        blocks.append(f"[ТАБЛИЦА {idx}]")
                        blocks.append(table_clean)

        return "\n\n".join(blocks).strip() + "\n"

    def _build_documents_block(self, files_data: List[Dict[str, Any]], max_total_chars: int = 180000) -> Tuple[str, Dict[str, Any]]:
        sorted_files = sorted(files_data, key=lambda f: self._document_priority(f.get("filename", "")))

        included_files: List[str] = []
        skipped_files: List[Dict[str, Any]] = []
        rendered_parts: List[str] = []
        total_chars = 0

        for file_data in sorted_files:
            filename = file_data.get("filename", "Unknown")
            rendered = self._render_pages(file_data)
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
            "total_chars": total_chars,
        }
        return block, meta

    def _insert_newlines_before_headings(self, text: str) -> str:
        text = re.sub(r'(?<!\n)(##\s+\d)', r'\n\1', text)
        text = re.sub(r'(?<!\n)(##\s+0\.)', r'\n\1', text)
        text = re.sub(r'(?<!\n)(##\s+Краткое резюме)', r'\n\1', text)
        text = re.sub(r'(?<!\n)(#\s+Юридическое заключение по тендеру)', r'\n\1', text)
        return text

    def _dedupe_main_heading(self, text: str) -> str:
        heading = "# Юридическое заключение по тендеру"
        occurrences = [m.start() for m in re.finditer(re.escape(heading), text)]
        if len(occurrences) <= 1:
            return text
        first = occurrences[0]
        tail = text[first:]
        tail = re.sub(r'(?:\n|^)\# Юридическое заключение по тендеру(?:\n+)?', '\n', tail, count=0)
        return heading + "\n\n" + tail.strip()

    def _normalize_table_block(self, lines: List[str]) -> List[str]:
        cleaned = []
        for line in lines:
            line = line.strip()
            if not line:
                continue
            if "|" not in line:
                continue
            # normalize double pipes and spacing
            line = re.sub(r'\|\|+', '|', line)
            if not line.startswith("|"):
                line = "| " + line.lstrip("| ").rstrip() + " |"
            if not line.endswith("|"):
                line = line.rstrip(" |") + " |"
            parts = [p.strip() for p in line.strip("|").split("|")]
            line = "| " + " | ".join(parts) + " |"
            cleaned.append(line)

        if not cleaned:
            return []

        # Ensure second line is separator
        if len(cleaned) == 1:
            col_count = len([p for p in cleaned[0].strip("|").split("|")])
            sep = "| " + " | ".join(["---"] * col_count) + " |"
            cleaned.append(sep)
        else:
            second = cleaned[1]
            if not re.match(r'^\|\s*:?-{3,}:?(?:\s*\|\s*:?-{3,}:?)+\s*\|$', second):
                col_count = len([p for p in cleaned[0].strip("|").split("|")])
                sep = "| " + " | ".join(["---"] * col_count) + " |"
                cleaned.insert(1, sep)

        return cleaned

    def _normalize_markdown_tables(self, text: str) -> str:
        raw_lines = text.split("\n")
        out_lines: List[str] = []
        table_buf: List[str] = []

        def flush_table():
            nonlocal table_buf, out_lines
            if table_buf:
                out_lines.extend(self._normalize_table_block(table_buf))
                table_buf = []

        for raw in raw_lines:
            line = raw.rstrip()

            if " | " in line or line.strip().startswith("|") or ("|" in line and line.count("|") >= 2):
                # split heading glued to table header
                if line.strip().startswith("## ") and "|" in line:
                    heading_part, table_part = line.split("|", 1)
                    flush_table()
                    out_lines.append(heading_part.strip())
                    table_buf.append("|" + table_part)
                else:
                    table_buf.append(line)
                continue

            flush_table()
            out_lines.append(line)

        flush_table()
        return "\n".join(out_lines)

    def _normalize_report(self, report_text: str) -> str:
        text = (report_text or "").strip()
        if not text:
            return "# Юридическое заключение по тендеру\n\nОтчет не сформирован."

        text = text.replace("\r\n", "\n")
        text = self._insert_newlines_before_headings(text)
        text = self._dedupe_main_heading(text)

        if not text.lstrip().startswith("# Юридическое заключение по тендеру"):
            text = "# Юридическое заключение по тендеру\n\n" + text.strip()

        text = self._normalize_markdown_tables(text)
        text = re.sub(r'\n{3,}', '\n\n', text).strip()
        return text

    def _extract_summary(self, markdown: str) -> str:
        if not markdown:
            return ""
        match = re.search(r"##\s*Краткое резюме(.*?)(?=\n##\s*1\.|\Z)", markdown, re.DOTALL | re.IGNORECASE)
        if not match:
            return ""
        content = match.group(1).strip()
        content = re.sub(r"\|.*", "", content).strip()
        return content[:1200]

    def _validate_report(self, report: str) -> Dict[str, bool]:
        """
        Проверка наличия всех обязательных разделов и таблиц.
        """
        validation = {}
        for header in REQUIRED_REPORT_HEADERS:
            validation[header] = header in report

        # Проверка наличия критических таблиц
        validation["table_customer"] = "## 0. Карточка Заказчика" in report and "|" in report.split("## 0. Карточка Заказчика")[1].split("##")[0]
        validation["table_goods"] = "## 0.1. Сводная таблица товаров" in report and "|" in report.split("## 0.1. Сводная таблица товаров")[1].split("##")[0]
        validation["table_equivalents"] = "## 3.1. Возможность поставки эквивалентов" in report and "|" in report.split("## 3.1. Возможность поставки эквивалентов")[1].split("##")[0]

        # Проверка на "Не найдено"
        not_found_count = report.count("Не найдено в ТД")
        validation["has_content"] = not_found_count < (len(REQUIRED_REPORT_HEADERS) * 0.8)

        return validation

    def _calculate_status(self, report: str, validation: Dict[str, bool]) -> str:
        """
        Определение статуса анализа.
        """
        if len(report) < 500:
            return "error"

        missing_headers = [h for h, found in validation.items() if h.startswith("##") and not found]
        
        # Если нет самых важных таблиц
        if not validation.get("table_customer") or not validation.get("table_goods") or not validation.get("table_equivalents"):
            return "partial"

        if len(missing_headers) > 2 or not validation["has_content"]:
            return "partial"

        return "success"
