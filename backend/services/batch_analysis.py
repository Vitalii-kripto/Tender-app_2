import os
import time
import zipfile
from typing import List, Dict, Any

from backend.config import DOCUMENTS_ROOT
from backend.logger import logger
from .document_service import DocumentService
from .job_service import job_service
from .legal_analysis_service import LegalAnalysisService


def analyze_tenders_batch_job(
    job_id: str,
    tender_ids: List[str],
    doc_service: DocumentService,
    legal_service: LegalAnalysisService,
    selected_files: Dict[str, List[str]] = None,
):
    """
    Воркер пакетного анализа тендеров.

    Логика:
    1. Проверка выбранных файлов.
    2. Извлечение текста по каждому файлу отдельно.
    3. Формирование одного пакета files_data на тендер.
    4. Запуск LegalAnalysisService только если есть хотя бы один успешно прочитанный файл.
    5. Экспорт Word-отчета и ZIP-архива.
    """
    selected_files = selected_files or {}
    documents_root = DOCUMENTS_ROOT
    report_paths: List[str] = []

    for tid in tender_ids:
        logger.info("--- [START TENDER ANALYSIS: %s] ---", tid)
        tender_dir = os.path.join(documents_root, tid)
        job_service.update_tender_stage(job_id, tid, "Подготовка документов", 10)

        requested_files = selected_files.get(tid, [])
        if not requested_files:
            logger.warning("No files selected for tender %s", tid)
            job_service.complete_tender(job_id, tid, {
                "status": "error",
                "final_report_markdown": "Ошибка: не выбрано ни одного файла для анализа. Пожалуйста, выберите хотя бы один документ.",
                "summary_notes": "Файлы не выбраны.",
                "file_statuses": [],
                "export_available": False,
            })
            continue

        if not os.path.exists(tender_dir):
            logger.warning("Tender directory not found: %s", tender_dir)
            job_service.complete_tender(job_id, tid, {
                "status": "error",
                "final_report_markdown": "Ошибка: директория с документами не найдена. Возможно, тендер еще не был обработан или файлы были удалены.",
                "summary_notes": "Директория не найдена.",
                "file_statuses": [{"filename": f, "status": "error", "message": "Директория не найдена"} for f in requested_files],
                "export_available": False,
            })
            continue

        job_service.update_tender_stage(job_id, tid, "Извлечение текста", 20)

        files_data: List[Dict[str, Any]] = []
        file_statuses: List[Dict[str, Any]] = []
        available_files = set(os.listdir(tender_dir))

        logger.info("--- [STARTING TEXT EXTRACTION FOR TENDER %s] ---", tid)
        logger.info("Requested files: %s", requested_files)

        docx_bases = {os.path.splitext(f)[0] for f in requested_files if f.lower().endswith(".docx")}
        filtered_files = []
        for f in requested_files:
            base, ext = os.path.splitext(f)
            if ext.lower() == ".doc" and base in docx_bases:
                logger.info("Skipping %s because %s.docx is also selected.", f, base)
                continue
            filtered_files.append(f)

        extraction_start_time = time.time()

        for filename in filtered_files:
            logger.info("[FILE_PROCESS_START] tender_id=%s filename=%s", tid, filename)

            if filename not in available_files:
                logger.warning("File %s not found in %s", filename, tender_dir)
                file_statuses.append({
                    "filename": filename,
                    "status": "skipped_not_found",
                    "message": "Файл не найден на диске",
                })
                logger.info("[FILE_PROCESS_RESULT] tender_id=%s filename=%s status=skipped_not_found text_length=0", tid, filename)
                continue

            filepath = os.path.join(tender_dir, filename)

            try:
                doc_data = doc_service.extract_document_data(filepath)
                status = doc_data.get("status", "failed_unknown")
                text_len = doc_data.get("text_length", 0)

                logger.info(
                    "[FILE_PROCESS_RESULT] tender_id=%s filename=%s status=%s text_length=%s",
                    tid, filename, status, text_len
                )

                files_data.append(doc_data)
                file_statuses.append({
                    "filename": filename,
                    "status": status,
                    "message": doc_data.get("error_message") or ("Текст успешно извлечен" if status == "success" else status),
                })

            except Exception as e:
                logger.error("Failed to extract text from %s: %s", filename, e, exc_info=True)
                file_statuses.append({
                    "filename": filename,
                    "status": "failed_unknown",
                    "message": f"Ошибка извлечения: {str(e)}",
                })
                logger.info("[FILE_PROCESS_RESULT] tender_id=%s filename=%s status=failed_unknown text_length=0", tid, filename)

        extraction_time = time.time() - extraction_start_time

        success_files = [f.get("filename") for f in files_data if f.get("status") == "success"]
        failed_files = [f.get("filename") for f in files_data if f.get("status") != "success"]
        has_critical_degraded_file = any(bool(f.get("critical_for_analysis")) for f in files_data)

        logger.info(
            "[TENDER_PACKET_READY] tender_id=%s success_files=%s failed_files=%s critical_degraded=%s",
            tid, len(success_files), len(failed_files), has_critical_degraded_file
        )

        if not success_files:
            logger.error("No text extracted from any of the selected files for tender %s", tid)
            job_service.complete_tender(job_id, tid, {
                "status": "error",
                "final_report_markdown": "Ошибка: не удалось извлечь текст ни из одного выбранного файла. Проверьте форматы документов.",
                "summary_notes": "Текст не извлечен.",
                "file_statuses": file_statuses,
                "export_available": False,
            })
            continue

        try:
            def stage_callback(stage, progress, status="running"):
                job_service.update_tender_stage(job_id, tid, stage, progress, status)

            analysis_start_time = time.time()
            analysis_result = legal_service.analyze_tender(
                files_data,
                tender_id=tid,
                job_id=job_id,
                callback=stage_callback,
            )
            analysis_time = time.time() - analysis_start_time

            if has_critical_degraded_file and analysis_result.get("status") == "success":
                analysis_result["status"] = "partial"
                if analysis_result.get("final_report_markdown"):
                    analysis_result["final_report_markdown"] += (
                        "\n\n## Ограничение полноты анализа\n"
                        "В составе тендера есть критичный PDF-файл, по которому OCR отработал с ошибкой или неполно. "
                        "Выводы по НМЦК / сметным данным могут быть неполными."
                    )

            final_markdown = analysis_result.get("final_report_markdown", "")
            summary_notes = analysis_result.get("summary_notes", "")
            if summary_notes and len(summary_notes.strip()) > 1200:
                summary_notes = summary_notes[:1200].strip()

            cleaned_context_len = analysis_result.get("cleaned_context_len", 0)
            final_report_len = analysis_result.get("final_report_len", 0)
            structured_data = analysis_result.get("structured_data", {})
            merged_facts = analysis_result.get("merged_facts", {})
            extracted_facts = analysis_result.get("extracted_facts", [])

            report_path = "N/A"
            export_available = False

            if analysis_result.get("status") in ("success", "partial") and final_markdown and len(final_markdown.strip()) > 300:
                try:
                    from docx import Document
                    from docx.shared import Pt
                    from backend.markdown_parser import add_markdown_to_docx

                    doc = Document()
                    style = doc.styles["Normal"]
                    font = style.font
                    font.name = "Arial"
                    font.size = Pt(11)

                    doc.add_heading(f"Юридическое заключение по тендеру {tid}", 0)
                    add_markdown_to_docx(doc, final_markdown)

                    os.makedirs(tender_dir, exist_ok=True)
                    report_filename = f"report_{tid}.docx"
                    report_path = os.path.abspath(os.path.join(tender_dir, report_filename))
                    doc.save(report_path)
                    report_paths.append(report_path)
                    export_available = True
                except Exception as e:
                    logger.error("Error generating Word report for tender %s: %s", tid, e, exc_info=True)
            else:
                logger.warning(
                    "Skipping Word report for tender %s due to analysis status=%s or insufficient report content",
                    tid, analysis_result.get("status")
                )
                if analysis_result.get("status") == "success":
                    analysis_result["status"] = "partial"

            logger.info("--- [ANALYSIS LOGS FOR TENDER %s] ---", tid)
            logger.info("- Extraction time: %.2fs", extraction_time)
            logger.info("- AI Analysis time: %.2fs", analysis_time)
            logger.info("- Cleaned context length: %s chars", cleaned_context_len)
            logger.info("- Final markdown report length: %s chars", final_report_len)
            logger.info("- Word report path: %s", report_path)
            logger.info("----------------------------------------")

            job_service.complete_tender(job_id, tid, {
                "status": analysis_result.get("status", "success"),
                "final_report_markdown": final_markdown,
                "error_message": analysis_result.get("error_message", ""),
                "summary_notes": summary_notes,
                "file_statuses": file_statuses,
                "report_path": report_path,
                "export_available": export_available,
                "structured_data": structured_data,
                "merged_facts": merged_facts,
                "extracted_facts": extracted_facts,
            })
            logger.info("--- [END TENDER ANALYSIS: %s] ---", tid)

        except Exception as e:
            logger.error("Analysis failed for tender %s: %s", tid, e, exc_info=True)
            job_service.complete_tender(job_id, tid, {
                "status": "error",
                "final_report_markdown": f"Критическая ошибка анализа: {str(e)}",
                "summary_notes": "Ошибка анализа.",
                "file_statuses": file_statuses,
                "export_available": False,
                "structured_data": {},
            })

    zip_path = "N/A"
    if len(report_paths) > 1:
        try:
            batch_dir = os.path.join(documents_root, "batch_results")
            os.makedirs(batch_dir, exist_ok=True)
            zip_filename = f"batch_{job_id}.zip"
            zip_path = os.path.abspath(os.path.join(batch_dir, zip_filename))

            with zipfile.ZipFile(zip_path, "w") as zipf:
                for r_path in report_paths:
                    if os.path.exists(r_path):
                        zipf.write(r_path, os.path.basename(r_path))

            logger.info("--- [BATCH ZIP LOG] ---")
            logger.info("- Batch ZIP path: %s", zip_path)
            logger.info("------------------------")
        except Exception as e:
            logger.error("Error creating batch ZIP: %s", e, exc_info=True)

    job_service.check_job_completion(job_id)
