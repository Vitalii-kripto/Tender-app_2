import os
import re
import io
import numpy as np
from pypdf import PdfReader
from fastapi import UploadFile
import aiofiles
import platform
import logging
from typing import Any, Dict, List, Optional

from backend.logger import logger

import pypdfium2 as pdfium
from paddleocr import PaddleOCR

class DocumentService:
    """
    Сервис для работы с документами.
    Поддерживает извлечение текста из PDF и OCR (распознавание сканов) через PaddleOCR.
    """
    UPLOAD_DIR = "uploaded_docs"

    def __init__(self):
        os.makedirs(self.UPLOAD_DIR, exist_ok=True)
        self.text_cache = {} # {(file_path, mtime): result_dict}
        try:
            # Инициализация PaddleOCR (модели скачиваются при первом запуске)
            self.ocr_engine = PaddleOCR(use_angle_cls=True, lang='ru')
            logger.info("PaddleOCR engine initialized successfully.")
        except Exception as e:
            logger.error(f"Failed to initialize PaddleOCR: {e}")
            raise RuntimeError(f"OCR Configuration Error: Failed to initialize PaddleOCR: {e}")

    async def save_file(self, file: UploadFile) -> str:
        """Сохраняет загруженный файл"""
        file_path = os.path.join(self.UPLOAD_DIR, file.filename)
        logger.info(f"Saving file to: {file_path}")
        async with aiofiles.open(file_path, 'wb') as out_file:
            content = await file.read()
            await out_file.write(content)
        return file_path

    def _convert_doc_to_docx(self, doc_path: str) -> str:
        """
        Пытается конвертировать .doc в .docx.
        Возвращает путь к новому файлу или оригинальный путь, если не удалось.
        """
        docx_path = doc_path + "x"
        if os.path.exists(docx_path):
            logger.info(f"DOCX version already exists: {docx_path}")
            return docx_path

        # 1. Попытка через win32com (только Windows + Word)
        if platform.system() == "Windows":
            try:
                import win32com.client as win32
                import pythoncom
                # Инициализация COM для текущего потока (важно для FastAPI/async)
                pythoncom.CoInitialize()
                
                word = win32.gencache.EnsureDispatch('Word.Application')
                word.Visible = False
                doc = word.Documents.Open(os.path.abspath(doc_path))
                # 16 = wdFormatXMLDocument (.docx)
                doc.SaveAs2(os.path.abspath(docx_path), FileFormat=16)
                doc.Close()
                word.Quit()
                logger.info(f"Successfully converted {doc_path} to {docx_path} using MS Word.")
                return docx_path
            except Exception as e:
                logger.warning(f"win32com conversion failed (check if Word is installed): {e}")
            finally:
                try:
                    pythoncom.CoUninitialize()
                except:
                    pass

        # 2. Попытка через извлечение текста и создание нового .docx (fallback)
        text = self._extract_text_from_doc(doc_path)
        if text and not text.startswith("[ОШИБКА"):
            try:
                import docx
                new_doc = docx.Document()
                new_doc.add_paragraph(f"--- АВТОМАТИЧЕСКАЯ КОНВЕРТАЦИЯ ИЗ .DOC ---\nОригинал: {os.path.basename(doc_path)}\n\n")
                new_doc.add_paragraph(text)
                new_doc.save(docx_path)
                logger.info(f"Created {docx_path} from extracted text of {doc_path}")
                return docx_path
            except Exception as e:
                logger.error(f"Failed to create docx from text: {e}")

        return doc_path

    def extract_document_data(self, file_path: str, use_cache: bool = True) -> Dict[str, Any]:
        """
        Извлечение структурированных данных из документа (страницы, листы, статус).
        """
        if not os.path.exists(file_path):
            return {"filename": os.path.basename(file_path), "text": "", "status": "error", "error_message": "Файл не найден"}
            
        mtime = os.path.getmtime(file_path)
        cache_key = (file_path, mtime)
        
        if use_cache and cache_key in self.text_cache:
            logger.info(f"Returning cached text for {file_path}")
            return self.text_cache[cache_key]

        ext = os.path.splitext(file_path)[1].lower()
        filename = os.path.basename(file_path)
        logger.info(f"--- [START STRUCTURED EXTRACTION] ---")
        logger.info(f"File path: {file_path}")
        logger.info(f"Extension: {ext}")
        
        result = {
            "filename": filename,
            "text": "",
            "pages": [],
            "status": "ok",
            "error_message": ""
        }

        try:
            if ext == '.doc':
                docx_path = file_path + "x"
                if os.path.exists(docx_path):
                    file_path = docx_path
                    ext = '.docx'
                else:
                    new_path = self._convert_doc_to_docx(file_path)
                    if new_path.endswith('.docx'):
                        file_path = new_path
                        ext = '.docx'
                    else:
                        full_text = self._extract_text_from_doc(file_path)
                        result["text"] = full_text
                        result["pages"] = [{"page_num": 1, "text": full_text}]
                        if not full_text.strip() or full_text.startswith("[ОШИБКА"):
                            result["status"] = "error"
                            result["error_message"] = full_text
                        return result

            if ext == '.docx':
                import docx
                doc = docx.Document(file_path)
                full_text = "\n".join([para.text for para in doc.paragraphs])
                result["text"] = full_text
                result["pages"] = [{"page_num": 1, "text": full_text}]
                
                tables_data = []
                for table in doc.tables:
                    table_content = []
                    for row in table.rows:
                        row_data = [cell.text.strip() for cell in row.cells]
                        table_content.append(" | ".join(row_data))
                    tables_data.append("\n".join(table_content))
                
                if tables_data:
                    result["pages"][0]["tables"] = tables_data
                    result["text"] += "\n\n--- ТАБЛИЦЫ ---\n" + "\n\n".join(tables_data)
            
            elif ext == '.xlsx':
                import openpyxl
                wb = openpyxl.load_workbook(file_path, data_only=True)
                sheets_text = []
                for sheet in wb.worksheets:
                    sheet_data = []
                    for row in sheet.iter_rows(values_only=True):
                        row_text = " | ".join([str(cell) if cell is not None else "" for cell in row])
                        if row_text.strip().replace("|", "").strip():
                            sheet_data.append(row_text)
                    if sheet_data:
                        sheet_text = f"=== ЛИСТ: {sheet.title} ===\n" + "\n".join(sheet_data)
                        sheets_text.append(sheet_text)
                        result["pages"].append({"page_num": sheet.title, "text": sheet_text, "is_sheet": True, "tables": ["\n".join(sheet_data)]})
                result["text"] = "\n\n".join(sheets_text)
            
            elif ext == '.xls':
                import xlrd
                wb = xlrd.open_workbook(file_path)
                sheets_text = []
                for sheet in wb.sheets():
                    sheet_data = []
                    for row_idx in range(sheet.nrows):
                        row = sheet.row_values(row_idx)
                        row_text = " | ".join([str(cell) if cell is not None else "" for cell in row])
                        if row_text.strip().replace("|", "").strip():
                            sheet_data.append(row_text)
                    if sheet_data:
                        sheet_text = f"=== ЛИСТ: {sheet.name} ===\n" + "\n".join(sheet_data)
                        sheets_text.append(sheet_text)
                        result["pages"].append({"page_num": sheet.name, "text": sheet_text, "is_sheet": True, "tables": ["\n".join(sheet_data)]})
                result["text"] = "\n\n".join(sheets_text)
            
            elif ext == '.pdf':
                reader = PdfReader(file_path)
                text_pages = []
                for i, page in enumerate(reader.pages):
                    try:
                        extracted = page.extract_text()
                        if extracted:
                            text_pages.append(extracted)
                            result["pages"].append({"page_num": i + 1, "text": extracted})
                    except Exception as e:
                        logger.warning(f"Failed to extract text from a PDF page {i+1}: {e}")
                
                full_text = "\n".join(text_pages)
                is_quality_good = self._is_text_quality_good(full_text)
                
                if not is_quality_good:
                    logger.info(f"PDF text quality is POOR. Triggering OCR fallback...")
                    try:
                        ocr_pages = self._perform_ocr(file_path)
                        result["pages"] = ocr_pages
                        result["text"] = "\n\n".join([f"--- Page {p['page_num']} ---\n{p['text']}" for p in ocr_pages])
                        result["ocr_used"] = True

                        if not result["text"].strip() or "[OCR WARNING]" in result["text"]:
                            result["status"] = "ocr_failed"
                            result["error_message"] = "OCR отработал, но текст не найден или найден только мусор"
                            result["text"] = full_text
                            result["pages"] = [{"page_num": i + 1, "text": p} for i, p in enumerate(text_pages)]
                    except Exception as e:
                        logger.error(f"OCR failed: {e}")
                        result["status"] = "ocr_failed"
                        result["error_message"] = f"OCR failed: {str(e)}"
                        result["text"] = full_text
                        result["pages"] = [{"page_num": i + 1, "text": p} for i, p in enumerate(text_pages)]
                        result["ocr_used"] = True
                else:
                    result["text"] = full_text

                filename_lower = filename.lower()
                if result.get("status") == "ocr_failed" and any(x in filename_lower for x in ["нмцк", "обоснование", "смет"]):
                    result["critical_for_analysis"] = True
                else:
                    result["critical_for_analysis"] = False

            else:
                result["status"] = "error"
                result["error_message"] = f"Unsupported file extension: {ext}"

            if not result["text"].strip() and result["status"] == "ok":
                result["status"] = "empty"
                result["error_message"] = "Файл пуст или текст не извлечен"

        except Exception as e:
            logger.error(f"Extraction error for {file_path}: {e}", exc_info=True)
            result["status"] = "error"
            result["error_message"] = str(e)

        if result.get("status") == "ok":
            self.text_cache[cache_key] = result
            
        return result

    def extract_text(self, file_path: str) -> str:
        """
        Обертка для обратной совместимости.
        """
        data = self.extract_document_data(file_path)
        if data["status"] in ["error", "ocr_failed", "empty"]:
            logger.warning(f"Extraction issue: {data['status']} - {data['error_message']}")
        return data.get("text", "")

    def _is_text_quality_good(self, text: str) -> bool:
        """
        Проверяет качество извлеченного текста.
        Возвращает True, если текст качественный, и False, если требуется OCR.
        Ужесточенные критерии для PDF.
        """
        if not text or len(text.strip()) < 150: # Увеличили порог минимальной длины
            return False

        total_chars = len(text)
        
        # 1. Доля мусорных символов
        clean_pattern = r'[a-zA-Zа-яА-ЯёЁ0-9\s\.,!?;:()""\'\'\-\+=\[\]/\\<>@#\$%\^&\*«»№]'
        clean_chars_count = len(re.findall(clean_pattern, text))
        garbage_ratio = 1 - (clean_chars_count / total_chars) if total_chars > 0 else 1
        
        # 2. Доля нормальных слов на русском
        words = text.split()
        if not words:
            return False
            
        russian_words = [w for w in words if re.search(r'[а-яА-ЯёЁ]{3,}', w)]
        russian_words_ratio = len(russian_words) / len(words) if words else 0
        
        # 3. Наличие длинных испорченных строк
        max_word_len = max(len(w) for w in words)
        avg_word_len = sum(len(w) for w in words) / len(words)
        
        # 4. Доля нечитаемых фрагментов
        unreadable_chars = len(re.findall(r'[\?\x00-\x08\x0b\x0c\x0e-\x1f]', text))
        unreadable_ratio = unreadable_chars / total_chars if total_chars > 0 else 0

        logger.info(f"PDF Quality Metrics: Garbage={garbage_ratio:.2f}, RusWords={russian_words_ratio:.2f}, MaxWord={max_word_len}, AvgWord={avg_word_len:.1f}, Unreadable={unreadable_ratio:.2f}")

        # Ужесточенные пороговые значения:
        # - Мусора > 10% (было 15%)
        # - Русских слов < 30% (было 20%)
        # - Слишком длинные "слова" (> 80 символов, было 100)
        # - Слишком много нечитаемых знаков (> 3%, было 5%)
        
        if garbage_ratio > 0.10:
            return False
        if russian_words_ratio < 0.30:
            # Если это не технический документ на английском
            english_words = [w for w in words if re.search(r'[a-zA-Z]{3,}', w)]
            english_words_ratio = len(english_words) / len(words)
            if english_words_ratio < 0.40: # И не английский тоже
                return False
        if max_word_len > 80 or avg_word_len > 18:
            return False
        if unreadable_ratio > 0.03:
            return False

        return True

    def _perform_ocr(self, file_path: str) -> List[Dict[str, Any]]:
        """Вспомогательный метод для OCR распознавания через pypdfium2 и PaddleOCR"""
        import time
        start_time = time.time()
        logger.info(f"Starting OCR for {file_path} using pypdfium2 + PaddleOCR")
        
        # Открываем PDF через pypdfium2
        pdf = pdfium.PdfDocument(file_path)
        num_pages = len(pdf)
        # Ограничиваем количество страниц для OCR (первые 20 для баланса скорости и качества)
        pages_to_process = min(num_pages, 20)
        
        ocr_pages = []
        
        for i in range(pages_to_process):
            logger.info(f"Processing page {i+1}/{pages_to_process}...")
            page = pdf[i]
            
            # Рендерим страницу в изображение (scale=2 для 144 DPI, scale=3 для 216 DPI)
            # PaddleOCR хорошо работает на 144-200 DPI
            bitmap = page.render(scale=2)
            pil_image = bitmap.to_pil()
            
            # Конвертируем PIL Image в numpy array для PaddleOCR
            img_array = np.array(pil_image)
            
            # Выполняем OCR
            try:
                result = self.ocr_engine.ocr(img_array, cls=True)
            except Exception as e:
                logger.warning(f"OCR with cls=True failed ({e}), retrying without cls...")
                result = self.ocr_engine.ocr(img_array)
            
            page_text = []
            if result and result[0]:
                for line in result[0]:
                    # line[1][0] - это текст, line[1][1] - это уверенность (confidence)
                    text_line = line[1][0]
                    page_text.append(text_line)
            
            page_content = "\n".join(page_text)
            ocr_pages.append({"page_num": i + 1, "text": page_content})
        
        pdf.close()
        
        end_time = time.time()
        
        full_ocr_text = "\n\n".join([p["text"] for p in ocr_pages])
        if full_ocr_text.strip():
            logger.info(f"OCR successful. Extracted {len(full_ocr_text)} characters from {pages_to_process} pages in {end_time - start_time:.2f}s.")
            return ocr_pages
        else:
            logger.warning(f"OCR ran but found no text in {end_time - start_time:.2f}s.")
            return [{"page_num": 1, "text": "[OCR WARNING] OCR отработал, но текст не найден."}]

    def _extract_text_from_doc(self, file_path: str) -> str:
        """
        Извлечение текста из старого формата .doc.
        Использует striprtf (если это RTF) или системную команду antiword.
        """
        # 1. Попытка через striprtf (некоторые .doc - это на самом деле RTF)
        try:
            from striprtf.striprtf import rtf_to_text
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
                if "{\\rtf" in content:
                    text = rtf_to_text(content)
                    if text.strip():
                        logger.info(f"Striprtf extracted {len(text)} characters from .doc (RTF)")
                        return text
        except Exception as e:
            logger.warning(f"Striprtf failed for .doc: {e}")

        # 2. Попытка через системную команду antiword
        import shutil
        import subprocess
        
        antiword_cmd = shutil.which('antiword')
        if antiword_cmd:
            try:
                result = subprocess.run([antiword_cmd, file_path], capture_output=True, text=True, errors='ignore')
                if result.returncode == 0 and result.stdout.strip():
                    logger.info(f"Antiword extracted {len(result.stdout)} characters from .doc")
                    return result.stdout
            except Exception as e:
                logger.warning(f"Antiword execution failed: {e}")
        else:
            logger.warning("Antiword executable not found in PATH.")

        # 3. Последняя попытка: чтение как простого текста (иногда помогает для очень старых или поврежденных файлов)
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                raw_content = f.read()
                # Пытаемся найти хоть какой-то осмысленный текст среди бинарных данных
                import re
                clean_text = re.sub(r'[^\x20-\x7E\u0400-\u04FF\n\t]', ' ', raw_content)
                clean_text = re.sub(r'\s+', ' ', clean_text).strip()
                if len(clean_text) > 100:
                    logger.info("Extracted partial text from .doc using raw fallback.")
                    return f"[ВНИМАНИЕ: Текст извлечен частично]\n\n{clean_text}"
        except:
            pass

        msg = f"Не удалось прочитать файл {os.path.basename(file_path)}."
        if platform.system() == "Windows":
            msg += "\n\nДЛЯ ИСПРАВЛЕНИЯ:\n1. Установите утилиту Antiword и добавьте её в PATH.\n2. ИЛИ (проще) пересохраните файл в формате .docx."
        else:
            msg += "\n\nУстановите пакет antiword (sudo apt install antiword)."
            
        return f"[ОШИБКА ФОРМАТА] {msg}"
