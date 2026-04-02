from backend.logging_setup import setup_logging

logger = setup_logging()
logger.info("--- [UNIFIED LOGGER INITIALIZED] ---")

import uvicorn
import asyncio
import sys
import os
import logging
import tempfile
import re
import sqlite3
from dotenv import load_dotenv

# Загружаем переменные окружения в самом начале
load_dotenv()
from typing import List, Dict, Any
from docx import Document
from docx.shared import Pt, RGBColor

from fastapi import FastAPI, Depends, HTTPException, UploadFile, File, Body, BackgroundTasks
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session

if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

from .database import engine, Base, get_db
from .models import TenderModel, ProductModel
from .services.eis_service import EisService, Notice, mark_seen
from backend.config import DOCUMENTS_ROOT
from .services.parser import GidroizolParser
from .services.document_service import DocumentService
from .services.ai_service import AiService
from .services.legal_analysis_service import LegalAnalysisService
from .services.batch_analysis import analyze_tenders_batch_job
from .services.job_service import job_service
from pydantic import BaseModel

class FrontendLog(BaseModel):
    level: str = "info"
    message: str
    context: Dict[str, Any] = {}

def clean_markdown(text):
    """Удаляет markdown-артефакты из текста"""
    if not text: return ""
    # Удаляем жирный/курсив
    text = re.sub(r'\*\*|\*|__|_', '', text)
    # Удаляем заголовки
    text = re.sub(r'#+\s+', '', text)
    # Удаляем горизонтальные линии
    text = re.sub(r'^-{3,}\s*$', '', text, flags=re.MULTILINE)
    # Удаляем лишние пустые строки
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()

def parse_markdown_table(text):
    """Парсит markdown-таблицу в список списков"""
    if not text or '|' not in text: return []
    lines = text.strip().split('\n')
    table_rows = []
    for line in lines:
        if '|' in line:
            # Пропускаем разделители |---|---|
            if re.match(r'^[\s|:\-\+]+$', line.strip()):
                continue
            # Разбиваем по |
            parts = [p.strip() for p in line.split('|')]
            # Убираем пустые элементы по краям, если строка начиналась/заканчивалась на |
            if line.strip().startswith('|'):
                parts = parts[1:]
            if line.strip().endswith('|'):
                parts = parts[:-1]
            
            if parts and any(p for p in parts):
                table_rows.append([clean_markdown(p) for p in parts])
    return table_rows

def parse_markdown_list(text):
    """Парсит markdown-список в список строк"""
    if not text: return []
    lines = text.strip().split('\n')
    list_items = []
    for line in lines:
        # Ищем маркеры списка: -, *, 1., 1)
        match = re.match(r'^\s*(?:[\-\*\+]|\d+[\.\)])\s+(.*)', line)
        if match:
            list_items.append(clean_markdown(match.group(1)))
        elif line.strip() and not re.match(r'^[\s|:\-\+]+$', line.strip()) and '|' not in line:
            # Если это просто строка текста, тоже берем ее как элемент, если она не пустая
            list_items.append(clean_markdown(line))
    return list_items



# --- SETUP ---
def migrate_db():
    """Простейшая миграция для добавления недостающих колонок в SQLite"""
    from .database import DB_PATH
    
    logger.info(f"Checking schema for {DB_PATH}...")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Список колонок, которые могли быть добавлены позже
    # (column_name, table_name, column_type)
    new_columns = [
        ("docs_url", "tenders", "TEXT"),
        ("search_url", "tenders", "TEXT"),
        ("keyword", "tenders", "TEXT"),
        ("ntype", "tenders", "TEXT"),
        ("local_file_path", "tenders", "TEXT"),
        ("extracted_text", "tenders", "TEXT"),
        ("created_at", "tenders", "DATETIME"),
        ("description", "products", "TEXT"),
        ("updated_at", "products", "DATETIME"),
    ]
    
    for col_name, table_name, col_type in new_columns:
        try:
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_type}")
            logger.info(f"Added column {col_name} to table {table_name}")
        except sqlite3.OperationalError:
            # Колонка уже существует
            pass
            
    conn.commit()
    conn.close()

try:
    logger.info("Initializing Database...")
    Base.metadata.create_all(bind=engine)
    migrate_db()
    logger.info("Database initialized successfully.")
except Exception as e:
    logger.critical(f"Database initialization failed: {e}", exc_info=True)

app = FastAPI(title="TenderSmart Gidroizol API", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Разрешить все для локальной разработки
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Services
eis_service = None
parser_service = None
doc_service = None
ai_service = None
legal_analysis_service = None

logger.info("Initializing Services...")

try:
    eis_service = EisService()
    logger.info("[OK] EisService initialized.")
except Exception as e:
    logger.error(f"[FAILED] EisService initialization error: {e}")

try:
    parser_service = GidroizolParser()
    logger.info("[OK] GidroizolParser initialized.")
except Exception as e:
    logger.error(f"[FAILED] GidroizolParser initialization error: {e}")

try:
    doc_service = DocumentService()
    logger.info("[OK] DocumentService initialized.")
except Exception as e:
    logger.error(f"[FAILED] DocumentService initialization error: {e}")

try:
    ai_service = AiService()
    # Выполняем тестовый запрос к ИИ при старте
    active_model = ai_service.test_model_availability()
    if not active_model:
        logger.critical("[WARNING] AI Service is UNAVAILABLE. Backend will start in degraded mode.")
    else:
        logger.info(f"[OK] AI Service is READY. Working model: {active_model}")
        
    try:
        legal_analysis_service = LegalAnalysisService(ai_service)
        logger.info("[OK] LegalAnalysisService initialized.")
    except Exception as e:
        logger.error(f"[FAILED] LegalAnalysisService initialization error: {e}")
        
except Exception as e:
    logger.error(f"[FAILED] AiService initialization error: {e}")

logger.info("Service initialization phase completed.")

# --- DEGRADED MODE CHECKS ---
def check_eis_service():
    if not eis_service:
        raise HTTPException(status_code=503, detail="EIS Service is not initialized.")

def check_parser_service():
    if not parser_service:
        raise HTTPException(status_code=503, detail="Parser Service is not initialized.")

def check_doc_service():
    if not doc_service:
        raise HTTPException(status_code=503, detail="Document Service is not initialized.")

def check_ai_service():
    if not ai_service or not ai_service.client:
        raise HTTPException(status_code=503, detail="AI Service is not initialized or unavailable.")

def check_legal_service():
    if not legal_analysis_service:
        raise HTTPException(status_code=503, detail="Legal Analysis Service is not initialized.")

# --- ENDPOINTS ---

@app.post("/api/frontend-log")
async def post_frontend_log(log_data: FrontendLog):
    """Принимает логи с фронтенда и записывает их в frontend.log"""
    f_logger = logging.getLogger("Frontend")
    msg = f"[{log_data.level.upper()}] {log_data.message}"
    if log_data.context:
        msg += f" | Context: {log_data.context}"
    
    if log_data.level.lower() == "error":
        f_logger.error(msg)
    elif log_data.level.lower() == "warning":
        f_logger.warning(msg)
    else:
        f_logger.info(msg)
        
    return {"status": "ok"}

@app.get("/")
def read_root():
    logger.info("Health check endpoint hit.")
    return {"status": "online", "system": "TenderSmart PRO Backend"}

# --- CRM ENDPOINTS (Database Sync) ---

@app.get("/api/crm/tenders")
def get_crm_tenders(db: Session = Depends(get_db)):
    """Получить все тендеры из базы"""
    logger.info("Fetching all CRM tenders.")
    return db.query(TenderModel).all()

@app.post("/api/crm/tenders")
def add_update_tender(background_tasks: BackgroundTasks, tender: dict = Body(...), db: Session = Depends(get_db)):
    """Добавить или обновить тендер в CRM"""
    logger.info(f"Add/Update tender request: {tender.get('id')}")
    try:
        existing = db.query(TenderModel).filter(TenderModel.id == tender['id']).first()
        
        # Parse initial_price to float
        raw_price = tender.get('initial_price', 0)
        parsed_price = 0.0
        if isinstance(raw_price, str):
            cleaned = re.sub(r'[^\d,.-]', '', raw_price).replace(',', '.')
            try:
                parsed_price = float(cleaned)
            except ValueError:
                parsed_price = 0.0
        else:
            parsed_price = float(raw_price)

        if existing:
            existing.status = tender.get('status', existing.status)
            existing.risk_level = tender.get('risk_level', existing.risk_level)
            logger.info(f"Updated existing tender: {tender['id']}")
        else:
            new_tender = TenderModel(
                id=tender['id'],
                title=tender['title'],
                description=tender.get('description', ''),
                initial_price=parsed_price,
                deadline=tender.get('deadline', '-'),
                status=tender.get('status', 'Found'),
                risk_level=tender.get('risk_level', 'Low'),
                region=tender.get('region', 'РФ'),
                law_type=tender.get('law_type', '44-ФЗ'),
                url=tender.get('url', ''),
                docs_url=tender.get('docs_url', ''),
                search_url=tender.get('search_url', ''),
                keyword=tender.get('keyword', ''),
                ntype=tender.get('ntype', '')
            )
            db.add(new_tender)
            logger.info(f"Created new tender: {tender['id']}")
            
            # Если это новый тендер, запускаем скачивание документов
            if tender.get('docs_url'):
                notice = Notice(
                    reg=tender['id'],
                    ntype=tender.get('ntype', ''),
                    keyword=tender.get('keyword', ''),
                    search_url=tender.get('search_url', ''),
                    href=tender.get('url', ''),
                    docs_url=tender.get('docs_url', ''),
                    title=tender.get('title', ''),
                    object_info=tender.get('description', ''),
                    initial_price=str(tender.get('initial_price', '')),
                    application_deadline=tender.get('deadline', '')
                )
                background_tasks.add_task(eis_service.process_tenders, [notice])
        
        db.commit()
        return {"status": "success"}
    except Exception as e:
        logger.error(f"Error saving tender: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/crm/tenders/{tender_id}")
def delete_tender(tender_id: str, db: Session = Depends(get_db)):
    """Удалить тендер из базы"""
    logger.info(f"Deleting tender: {tender_id}")
    tender = db.query(TenderModel).filter(TenderModel.id == tender_id).first()
    if tender:
        db.delete(tender)
        db.commit()
    return {"status": "deleted"}

# --- SEARCH & PARSING ---

@app.post("/api/search-tenders/cancel")
def cancel_search():
    """Отменить текущий поиск"""
    logger.info("Cancel search request received")
    eis_service.cancel_search()
    return {"status": "cancelled"}

@app.get("/api/search-tenders")
def search_tenders_endpoint(
    query: str, 
    fz44: bool = True, 
    fz223: bool = True, 
    only_application_stage: bool = True, 
    publish_days_back: int = 30,
    _ = Depends(check_eis_service)
):
    """Поиск через Playwright"""
    logger.info(f"Search request received: {query}")
    try:
        notices = eis_service.search_tenders(
            query=query, 
            fz44=fz44, 
            fz223=fz223, 
            only_application_stage=only_application_stage, 
            publish_days_back=publish_days_back
        )
    except RuntimeError as e:
        logger.error(f"Search failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        logger.error(f"Search failed with unexpected error: {e}")
        raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")
    
    # Convert Notice dataclass to dict for JSON response
    result = []
    for n in notices:
        raw_price = n.initial_price
        parsed_price = 0.0
        if isinstance(raw_price, str):
            cleaned = re.sub(r'[^\d,.-]', '', raw_price).replace(',', '.')
            try:
                parsed_price = float(cleaned)
            except ValueError:
                parsed_price = 0.0
        else:
            parsed_price = float(raw_price)

        result.append({
            "id": n.reg,
            "eis_number": n.reg,
            "title": n.title,
            "description": n.object_info,
            "initial_price": parsed_price,
            "initial_price_text": str(raw_price),
            "initial_price_value": parsed_price,
            "deadline": n.application_deadline,
            "status": "Found",
            "risk_level": "Low",
            "region": "РФ",
            "law_type": n.ntype,
            "url": n.href,
            "docs_url": f"https://zakupki.gov.ru/epz/order/notice/{n.ntype}/view/documents.html?regNumber={n.reg}",
            "search_url": n.search_url,
            "keyword": n.keyword,
            "ntype": n.ntype,
            "seen": n.seen
        })
    return result

@app.post("/api/search-tenders/process")
def process_tenders(background_tasks: BackgroundTasks, tenders: list = Body(...), db: Session = Depends(get_db)):
    """Обработать выбранные тендеры"""
    logger.info(f"Processing {len(tenders)} selected tenders")
    try:
        for tender in tenders:
            existing = db.query(TenderModel).filter(TenderModel.id == tender['id']).first()
            
            # Parse initial_price to float
            raw_price = tender.get('initial_price', 0)
            parsed_price = 0.0
            if isinstance(raw_price, str):
                cleaned = re.sub(r'[^\d,.-]', '', raw_price).replace(',', '.')
                try:
                    parsed_price = float(cleaned)
                except ValueError:
                    parsed_price = 0.0
            else:
                parsed_price = float(raw_price)

            if existing:
                existing.status = tender.get('status', existing.status)
                existing.risk_level = tender.get('risk_level', existing.risk_level)
                logger.info(f"Updated existing tender: {tender['id']}")
            else:
                new_tender = TenderModel(
                    id=tender['id'],
                    title=tender['title'],
                    description=tender.get('description', ''),
                    initial_price=parsed_price,
                    deadline=tender.get('deadline', '-'),
                    status=tender.get('status', 'Found'),
                    risk_level=tender.get('risk_level', 'Low'),
                    region=tender.get('region', 'РФ'),
                    law_type=tender.get('law_type', '44-ФЗ'),
                    url=tender.get('url', ''),
                    docs_url=tender.get('docs_url', ''),
                    search_url=tender.get('search_url', ''),
                    keyword=tender.get('keyword', ''),
                    ntype=tender.get('ntype', '')
                )
                db.add(new_tender)
                logger.info(f"Created new tender: {tender['id']}")
                
                # Если это новый тендер, запускаем скачивание документов
                if tender.get('docs_url'):
                    notice = Notice(
                        reg=tender['id'],
                        ntype=tender.get('ntype', ''),
                        keyword=tender.get('keyword', ''),
                        search_url=tender.get('search_url', ''),
                        href=tender.get('url', ''),
                        docs_url=tender.get('docs_url', ''),
                        title=tender.get('title', ''),
                        object_info=tender.get('description', ''),
                        initial_price=str(tender.get('initial_price', '')),
                        application_deadline=tender.get('deadline', '')
                    )
                    background_tasks.add_task(eis_service.process_tenders, [notice])
            
        db.commit()
        return {"status": "success", "processed": len(tenders)}
    except Exception as e:
        logger.error(f"Error processing tenders: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/search-tenders/skip")
def skip_tender(tender: dict = Body(...)):
    """Пропустить тендер (отметить как просмотренный)"""
    logger.info(f"Skipping tender: {tender.get('id')}")
    try:
        mark_seen(tender['id'])
        return {"status": "success"}
    except Exception as e:
        logger.error(f"Error skipping tender: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/products")
def get_products_endpoint(db: Session = Depends(get_db)):
    """Получение сохраненных товаров из БД без запуска парсера"""
    logger.info("Fetching products from DB.")
    products = db.query(ProductModel).all()
    result = []
    for p in products:
        result.append({
            "id": str(p.id),
            "title": p.title,
            "category": p.category,
            "material_type": p.material_type,
            "price": p.price,
            "specs": p.specs if p.specs else {},
            "url": p.url,
            "description": p.description # Added description
        })
    return result

@app.get("/api/parse-catalog")
async def parse_catalog_endpoint(db: Session = Depends(get_db), _ = Depends(check_parser_service)):
    """Запуск парсера каталога Gidroizol.ru и обновление БД"""
    logger.info("Starting catalog parser manually.")
    
    # Новый парсер возвращает количество сохраненных записей (int), а не список объектов
    saved_count = await parser_service.parse_and_save(db)
    logger.info(f"Parser finished. Saved/Updated {saved_count} items.")
    
    # После парсинга забираем актуальные данные из БД
    products = db.query(ProductModel).all()
    
    result = []
    for p in products:
        result.append({
            "id": str(p.id),
            "title": p.title,
            "category": p.category,
            "material_type": p.material_type,
            "price": p.price,
            "specs": p.specs if p.specs else {},
            "url": p.url,
            "description": p.description # Added description
        })
    return result

# --- AI & DOCS ENDPOINTS ---

@app.get("/api/tenders/{tender_id}/files")
def get_tender_files(tender_id: str, _ = Depends(check_doc_service)):
    """Получить список скачанных файлов для тендера"""
    logger.info(f"Fetching files for tender {tender_id}")
    tender_dir = os.path.join(DOCUMENTS_ROOT, tender_id)
    if not os.path.exists(tender_dir):
        return []
    
    files = []
    for filename in os.listdir(tender_dir):
        filepath = os.path.join(tender_dir, filename)
        if os.path.isfile(filepath):
            files.append({
                "name": filename,
                "size": os.path.getsize(filepath),
                "ext": os.path.splitext(filename)[1].lower()
            })
    return files

@app.post("/api/ai/analyze-tenders-batch")
async def api_analyze_tenders_batch(background_tasks: BackgroundTasks, data: dict = Body(...), _ = Depends(check_legal_service)):
    logger.info("Batch AI Analysis request received.")
    
    tender_ids = data.get('tender_ids', [])
    selected_files = data.get('selected_files', {}) # {tender_id: [filenames]}
    
    if not tender_ids:
        raise HTTPException(status_code=400, detail="No tender IDs provided")
    
    job_id = job_service.create_job(tender_ids)
    background_tasks.add_task(analyze_tenders_batch_job, job_id, tender_ids, doc_service, legal_analysis_service, selected_files)
    
    return {"job_id": job_id}

@app.get("/api/ai/jobs/{job_id}")
async def get_job_status(job_id: str):
    job = job_service.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job

@app.post("/api/ai/extract-details")
async def api_extract_details(data: dict = Body(...), _ = Depends(check_ai_service)):
    """Извлечение данных о тендере из текста"""
    logger.info("AI Extract Details request.")
    text = data.get('text', '')
    return ai_service.extract_tender_details(text)

@app.post("/api/ai/extract-products")
async def api_extract_products(data: dict = Body(...), _ = Depends(check_ai_service)):
    """Извлечение списка товаров из сметы/КП"""
    logger.info("AI Extract Products request.")
    text = data.get('text', '')
    return ai_service.extract_products_from_text(text)

@app.post("/api/ai/enrich-specs")
async def api_enrich_specs(data: dict = Body(...), _ = Depends(check_ai_service)):
    """Поиск характеристик товара в интернете"""
    logger.info("AI Enrich Specs request.")
    product_name = data.get('product_name', '')
    result = ai_service.enrich_product_specs(product_name)
    return {"specs": result}

@app.post("/api/ai/match-product")
async def api_match_product(data: dict = Body(...), db: Session = Depends(get_db), _ = Depends(check_ai_service)):
    specs = data.get('specs', '')
    mode = data.get('mode', 'database') # 'database' or 'internet'
    logger.info(f"AI Match Product request. Mode: {mode}, Query len: {len(specs)}")

    if mode == 'internet':
        # Поиск в интернете через Grounding
        result_text = ai_service.search_products_internet(specs)
        return {"mode": "internet", "text": result_text}
    else:
        # Поиск по базе
        products_db = db.query(ProductModel).limit(50).all()
        catalog = [{"id": str(p.id), "title": p.title, "specs": p.specs} for p in products_db]
        matches = ai_service.find_product_equivalent(specs, catalog)
        return {"mode": "database", "matches": matches}

@app.post("/api/ai/validate-compliance")
async def api_validate_compliance(data: dict = Body(...), _ = Depends(check_ai_service)):
    """Валидация ТЗ vs Материал (Complex)"""
    logger.info("AI Compliance Validation request.")
    requirements = data.get('requirements', '')
    proposal = data.get('proposal', '[]')
    return ai_service.compare_requirements_vs_proposal(requirements, proposal)

@app.post("/api/ai/check-compliance")
async def api_check_compliance(data: dict = Body(...), _ = Depends(check_ai_service)):
    """Проверка пакета документов"""
    logger.info("AI Document Package Check request.")
    return ai_service.check_compliance(data['title'], data['description'], data['filenames'])

@app.post("/api/tenders/upload")
async def upload_file(file: UploadFile = File(...), _ = Depends(check_doc_service)):
    logger.info(f"File upload request: {file.filename}")
    
    try:
        file_path = await doc_service.save_file(file)
        # Запускаем OCR или извлечение текста
        text = doc_service.extract_text(file_path)
        logger.info("File processed successfully.")
        return {"text": text, "path": file_path}
    except Exception as e:
        logger.error(f"Upload Error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/dashboard-stats")
async def get_dashboard_stats(db: Session = Depends(get_db)):
    logger.info("Dashboard stats requested.")
    count = db.query(TenderModel).count()
    return {
        "active_tenders": count,
        "margin_val": "₽14.2M",
        "risks_count": 5,
        "contracts_count": 12,
        "chart_data": [{"name": "Пн", "Тендеры": 10, "Выиграно": 2}],
        "tasks": [{"id": "1", "title": "Запустить парсер", "time": "Сейчас", "type": "info"}],
        "is_demo": False
    }

from backend.markdown_parser import add_markdown_to_docx
import zipfile
import io

@app.post("/api/ai/export-risks-word")
async def api_export_risks_word(data: dict = Body(...)):
    """Экспорт результатов анализа рисков в Word .docx (или ZIP для нескольких)"""
    logger.info("Word export started")
    results = data.get('results', [])
    if not results:
        raise HTTPException(status_code=400, detail="No results to export")

    try:
        if len(results) == 1:
            # Single file export
            tender = results[0]
            tid = str(tender.get('id', 'N/A'))
            desc = tender.get('description', 'Нет описания')
            final_report_markdown = tender.get('final_report_markdown') or ""
            summary_notes = tender.get('summary_notes') or ""
            file_statuses = tender.get('file_statuses', [])
            
            doc = Document()
            style = doc.styles['Normal']
            font = style.font
            font.name = 'Arial'
            font.size = Pt(11)
            
            doc.add_heading('Юридическое заключение по тендеру', 0)
            doc.add_heading(f'Тендер: {tid}', level=1)
            
            p = doc.add_paragraph()
            p.add_run('Описание: ').bold = True
            p.add_run(desc)
            
            if summary_notes:
                doc.add_heading('Краткое резюме:', level=2)
                doc.add_paragraph(summary_notes)
            
            if file_statuses:
                doc.add_heading('Обработанные документы:', level=2)
                for fs in file_statuses:
                    status_text = "Успешно" if fs.get('status') == 'ok' else "Ошибка"
                    doc.add_paragraph(f"{fs.get('filename')} - {status_text}", style='List Bullet')
            
            if not final_report_markdown:
                doc.add_paragraph("Отчет отсутствует.")
            else:
                add_markdown_to_docx(doc, final_report_markdown)
                
            with tempfile.NamedTemporaryFile(delete=False, suffix=".docx") as tmp:
                doc.save(tmp.name)
                logger.info(f"Word export finished successfully (single). Path: {tmp.name}")
                return FileResponse(tmp.name, media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document", filename=f"tender_{tid}_report.docx")
        
        else:
            # Multiple files export (ZIP)
            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
                for tender in results:
                    tid = str(tender.get('id', 'N/A'))
                    desc = tender.get('description', 'Нет описания')
                    final_report_markdown = tender.get('final_report_markdown') or ""
                    summary_notes = tender.get('summary_notes') or ""
                    file_statuses = tender.get('file_statuses', [])
                    
                    doc = Document()
                    style = doc.styles['Normal']
                    font = style.font
                    font.name = 'Arial'
                    font.size = Pt(11)
                    
                    doc.add_heading('Юридическое заключение по тендеру', 0)
                    doc.add_heading(f'Тендер: {tid}', level=1)
                    
                    p = doc.add_paragraph()
                    p.add_run('Описание: ').bold = True
                    p.add_run(desc)
                    
                    if summary_notes:
                        doc.add_heading('Краткое резюме:', level=2)
                        doc.add_paragraph(summary_notes)
                    
                    if file_statuses:
                        doc.add_heading('Обработанные документы:', level=2)
                        for fs in file_statuses:
                            status_text = "Успешно" if fs.get('status') == 'ok' else "Ошибка"
                            doc.add_paragraph(f"{fs.get('filename')} - {status_text}", style='List Bullet')
                    
                    if not final_report_markdown:
                        doc.add_paragraph("Отчет отсутствует.")
                    else:
                        add_markdown_to_docx(doc, final_report_markdown)
                        
                    doc_buffer = io.BytesIO()
                    doc.save(doc_buffer)
                    zip_file.writestr(f"tender_{tid}_report.docx", doc_buffer.getvalue())
            
            zip_buffer.seek(0)
            with tempfile.NamedTemporaryFile(delete=False, suffix=".zip") as tmp:
                tmp.write(zip_buffer.getvalue())
                logger.info(f"Word export finished successfully (zip). Path: {tmp.name}")
                return FileResponse(tmp.name, media_type="application/zip", filename="tenders_reports.zip")

    except Exception as e:
        logger.error(f"Word Export Error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run("backend.main:app", host="0.0.0.0", port=8000, reload=True)
