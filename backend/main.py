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
import shutil
from starlette.concurrency import run_in_threadpool

if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

from .database import engine, Base, get_db, SessionLocal
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


def parse_price_to_float(raw_price: Any) -> float:
    if raw_price is None or raw_price == "":
        return 0.0
    if isinstance(raw_price, (int, float)):
        return float(raw_price)

    text = str(raw_price).replace("\xa0", " ").strip()
    cleaned = re.sub(r"[^\d,.\-]", "", text)

    if not cleaned:
        return 0.0

    # Наиболее частый кейс для RU-формата
    if cleaned.count(",") == 1 and cleaned.count(".") == 0:
        cleaned = cleaned.replace(",", ".")
    elif cleaned.count(",") > 0 and cleaned.count(".") > 0:
        # Если и точка, и запятая — считаем последнюю запятую десятичным разделителем
        if cleaned.rfind(",") > cleaned.rfind("."):
            cleaned = cleaned.replace(".", "").replace(",", ".")
        else:
            cleaned = cleaned.replace(",", "")
    else:
        cleaned = cleaned.replace(",", "")

    try:
        return float(cleaned)
    except Exception:
        return 0.0


def normalize_law_type(
    *,
    reg: str = "",
    ntype: str = "",
    url: str = "",
    docs_url: str = "",
    explicit: Any = None,
) -> str:
    explicit_text = str(explicit or "").strip()
    if explicit_text in {"44-ФЗ", "223-ФЗ", "Коммерч."}:
        return explicit_text

    reg = str(reg or "")
    haystack = " ".join([
        reg,
        str(ntype or ""),
        str(url or ""),
        str(docs_url or ""),
    ]).lower()

    if reg.startswith("223-") or "notice223" in haystack:
        return "223-ФЗ"

    if "коммер" in haystack or "commercial" in haystack:
        return "Коммерч."

    return "44-ФЗ"


def build_docs_url_from_payload(payload: Dict[str, Any]) -> str:
    docs_url = str(payload.get("docs_url") or "").strip()
    if docs_url:
        return docs_url

    reg = str(payload.get("id") or payload.get("eis_number") or "").strip()
    ntype = str(payload.get("ntype") or "").strip()

    if not reg:
        return ""

    if reg.startswith("223-"):
        notice_id = reg.replace("223-", "", 1).strip()
        if notice_id:
            return f"https://zakupki.gov.ru/epz/order/notice/notice223/documents.html?noticeInfoId={notice_id}"
        return ""

    if ntype:
        return f"https://zakupki.gov.ru/epz/order/notice/{ntype}/view/documents.html?regNumber={reg}"

    return ""


def tender_has_local_files(tender_id: str) -> bool:
    tender_dir = os.path.join(DOCUMENTS_ROOT, tender_id)
    if not os.path.isdir(tender_dir):
        return False
    return any(os.path.isfile(os.path.join(tender_dir, name)) for name in os.listdir(tender_dir))


def tender_model_to_payload(tender: TenderModel) -> Dict[str, Any]:
    return {
        "id": tender.id,
        "eis_number": tender.id,
        "title": tender.title or "",
        "description": tender.description or "",
        "initial_price": tender.initial_price or 0,
        "deadline": tender.deadline or "",
        "status": tender.status or "Found",
        "risk_level": tender.risk_level or "Low",
        "region": tender.region or "РФ",
        "law_type": normalize_law_type(
            reg=tender.id,
            ntype=tender.ntype or "",
            url=tender.url or "",
            docs_url=tender.docs_url or "",
            explicit=tender.law_type,
        ),
        "url": tender.url or "",
        "docs_url": tender.docs_url or "",
        "search_url": tender.search_url or "",
        "keyword": tender.keyword or "",
        "ntype": tender.ntype or "",
        "selected_for_matching": bool(tender.selected_for_matching),
    }


def build_notice_from_payload(payload: Dict[str, Any]) -> Notice:
    reg = str(payload.get("id") or payload.get("eis_number") or "").strip()
    docs_url = build_docs_url_from_payload(payload)

    return Notice(
        reg=reg,
        ntype=str(payload.get("ntype") or "").strip(),
        keyword=str(payload.get("keyword") or "").strip(),
        search_url=str(payload.get("search_url") or "").strip(),
        href=str(payload.get("url") or "").strip(),
        docs_url=docs_url,
        title=str(payload.get("title") or "").strip(),
        object_info=str(payload.get("description") or "").strip(),
        initial_price=str(payload.get("initial_price") or "").strip(),
        application_deadline=str(payload.get("deadline") or "").strip(),
    )


def save_or_update_tender_record(
    *,
    db: Session,
    tender_payload: Dict[str, Any],
    background_tasks: BackgroundTasks,
    force_download: bool = False,
) -> TenderModel:
    tender_id = str(tender_payload.get("id") or "").strip()
    if not tender_id:
        raise HTTPException(status_code=400, detail="У тендера отсутствует id")

    title = str(tender_payload.get("title") or "").strip()
    if not title:
        raise HTTPException(status_code=400, detail=f"У тендера {tender_id} отсутствует title")

    docs_url = build_docs_url_from_payload(tender_payload)
    law_type = normalize_law_type(
        reg=tender_id,
        ntype=str(tender_payload.get("ntype") or ""),
        url=str(tender_payload.get("url") or ""),
        docs_url=docs_url,
        explicit=tender_payload.get("law_type"),
    )
    parsed_price = parse_price_to_float(tender_payload.get("initial_price", 0))
    selected_for_matching = bool(tender_payload.get("selected_for_matching", False))

    existing = db.query(TenderModel).filter(TenderModel.id == tender_id).first()

    if existing:
        previous_docs_url = existing.docs_url or ""

        existing.title = title
        existing.description = str(tender_payload.get("description") or "")
        existing.initial_price = parsed_price
        existing.deadline = str(tender_payload.get("deadline") or "")
        existing.status = str(tender_payload.get("status") or existing.status or "Found")
        existing.risk_level = str(tender_payload.get("risk_level") or existing.risk_level or "Low")
        existing.region = str(tender_payload.get("region") or existing.region or "РФ")
        existing.law_type = law_type
        existing.url = str(tender_payload.get("url") or "")
        existing.docs_url = docs_url
        existing.search_url = str(tender_payload.get("search_url") or "")
        existing.keyword = str(tender_payload.get("keyword") or "")
        existing.ntype = str(tender_payload.get("ntype") or "")
        existing.selected_for_matching = selected_for_matching

        db.flush()

        need_download = (
            force_download
            or (bool(docs_url) and not tender_has_local_files(existing.id))
            or (bool(docs_url) and docs_url != previous_docs_url)
        )

        model = existing
        logger.info(f"Updated existing tender: {tender_id}")
    else:
        model = TenderModel(
            id=tender_id,
            title=title,
            description=str(tender_payload.get("description") or ""),
            initial_price=parsed_price,
            deadline=str(tender_payload.get("deadline") or ""),
            status=str(tender_payload.get("status") or "Found"),
            risk_level=str(tender_payload.get("risk_level") or "Low"),
            region=str(tender_payload.get("region") or "РФ"),
            law_type=law_type,
            url=str(tender_payload.get("url") or ""),
            docs_url=docs_url,
            search_url=str(tender_payload.get("search_url") or ""),
            keyword=str(tender_payload.get("keyword") or ""),
            ntype=str(tender_payload.get("ntype") or ""),
            selected_for_matching=selected_for_matching,
        )
        db.add(model)
        db.flush()

        need_download = bool(docs_url)
        logger.info(f"Created new tender: {tender_id}")

    if need_download and docs_url:
        notice = build_notice_from_payload(
            {
                **tender_payload,
                "id": tender_id,
                "title": title,
                "docs_url": docs_url,
                "law_type": law_type,
            }
        )
        background_tasks.add_task(eis_service.process_tenders, [notice])
        logger.info(f"Scheduled document download for tender {tender_id}")

    return model

from backend.logging_setup import setup_logging, get_logger
logger = setup_logging("LegalAI")
frontend_logger = get_logger("Frontend")

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
        ("selected_for_matching", "tenders", "BOOLEAN DEFAULT 0"),
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
analog_service = None

def db_session_factory():
    return SessionLocal()

logger.info("Initializing Services...")

try:
    eis_service = EisService()
    logger.info("[OK] EisService initialized.")
except Exception as e:
    logger.error(f"[FAILED] EisService initialization error: {e}")

try:
    doc_service = DocumentService()
    logger.info("[OK] DocumentService initialized.")
except Exception as e:
    logger.error(f"[FAILED] DocumentService initialization error: {e}")

try:
    ai_service = AiService()
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

try:
    parser_service = GidroizolParser()
    logger.info("[OK] GidroizolParser initialized.")
except Exception as e:
    logger.error(f"[FAILED] GidroizolParser initialization error: {e}")

try:
    from backend.services.analog_service import AnalogService

    if not ai_service:
        raise RuntimeError("AiService is not initialized, AnalogService cannot be created.")

    analog_service = AnalogService(
        ai_service=ai_service,
        db_session_factory=db_session_factory
    )
    logger.info("[OK] AnalogService initialized.")
except Exception as e:
    logger.error(f"[FAILED] AnalogService initialization error: {e}")

logger.info("Service initialization phase completed.")

def build_notice_from_tender_model(tender: TenderModel) -> Notice:
    return Notice(
        reg=tender.id,
        ntype=tender.ntype or "",
        keyword=tender.keyword or "",
        search_url=tender.search_url or "",
        href=tender.url or "",
        docs_url=tender.docs_url or "",
        title=tender.title or "",
        object_info=tender.description or "",
        initial_price=str(tender.initial_price or ""),
        application_deadline=tender.deadline or "",
    )

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

def check_analog_service():
    if not analog_service:
        raise HTTPException(status_code=503, detail="Analog Service is not initialized.")

# --- ENDPOINTS ---

@app.post("/api/frontend-log")
async def post_frontend_log(log_data: FrontendLog):
    """Принимает frontend-логи и пишет их в общий tendersmart.log"""
    f_logger = logging.getLogger("Frontend")

    msg = f"[FRONTEND][{log_data.level.upper()}] {log_data.message}"
    if log_data.context:
        msg += f" | Context: {log_data.context}"

    level = log_data.level.lower().strip()
    if level == "error":
        f_logger.error(msg)
    elif level == "warning":
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
def add_update_tender(
    background_tasks: BackgroundTasks,
    tender: dict = Body(...),
    db: Session = Depends(get_db),
):
    """Добавить или обновить тендер в CRM"""
    tender_id = str(tender.get("id") or "").strip()
    logger.info(f"Add/Update tender request: {tender_id}")

    try:
        save_or_update_tender_record(
            db=db,
            tender_payload=tender,
            background_tasks=background_tasks,
            force_download=False,
        )
        db.commit()
        return {"status": "success"}
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"Error saving tender: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/crm/tenders/{tender_id}/requeue-docs")
def requeue_tender_docs(tender_id: str, background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    """Повторно запустить скачивание документов для тендера"""
    tender = db.query(TenderModel).filter(TenderModel.id == tender_id).first()
    if not tender:
        raise HTTPException(status_code=404, detail="Tender not found")
        
    if not tender.docs_url:
        raise HTTPException(status_code=400, detail="Tender has no docs_url")
        
    notice = Notice(
        reg=tender.id,
        ntype=tender.ntype or "",
        keyword=tender.keyword or "",
        search_url=tender.search_url or "",
        title=tender.title or "",
        href=tender.url or "",
        object_info=tender.description or "",
        initial_price=str(tender.initial_price or ""),
        application_deadline=tender.deadline or "",
        docs_url=tender.docs_url
    )
    background_tasks.add_task(eis_service.process_tenders, [notice])
    logger.info(f"Manually requeued docs for tender: {tender_id}")
    return {"status": "requeued"}

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
    _ = Depends(check_eis_service),
):
    """Поиск через Playwright"""
    import uuid

    search_id = uuid.uuid4().hex[:8]
    logger.info(f"[SEARCH_API_START] [SearchID:{search_id}] Search request received: {query}")
    try:
        notices = eis_service.search_tenders(
            query=query,
            fz44=fz44,
            fz223=fz223,
            only_application_stage=only_application_stage,
            publish_days_back=publish_days_back,
            search_id=search_id,
        )
        logger.info(f"[SEARCH_API_RESPONSE] [SearchID:{search_id}] eis_service returned {len(notices)} items")
    except RuntimeError as e:
        logger.error(f"[SEARCH_API_ERROR] [SearchID:{search_id}] Search failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        logger.error(f"[SEARCH_API_ERROR] [SearchID:{search_id}] Search failed with unexpected error: {e}")
        raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")

    # Convert Notice dataclass to dict for JSON response
    result = []
    for n in notices:
        docs_url = build_docs_url_from_payload({
            "id": n.reg,
            "ntype": n.ntype,
            "docs_url": n.docs_url
        })
        law_type = normalize_law_type(
            reg=n.reg,
            ntype=n.ntype,
            url=n.href,
            docs_url=docs_url
        )
        parsed_price = parse_price_to_float(n.initial_price)

        result.append({
            "id": n.reg,
            "eis_number": n.reg,
            "title": n.title,
            "description": n.object_info,
            "initial_price": parsed_price,
            "initial_price_text": str(n.initial_price),
            "deadline": n.application_deadline,
            "status": "Found",
            "risk_level": "Low",
            "region": "РФ",
            "law_type": law_type,
            "url": n.href,
            "docs_url": docs_url,
            "search_url": n.search_url,
            "keyword": n.keyword,
            "ntype": n.ntype,
        })

    return result

@app.post("/api/search-tenders/process")
def process_tenders(
    background_tasks: BackgroundTasks,
    tenders: list = Body(...),
    db: Session = Depends(get_db),
):
    """Массовая обработка выбранных тендеров"""
    if not tenders:
        return {"status": "ok", "processed": 0}

    logger.info(f"Processing {len(tenders)} selected tenders")

    for t_payload in tenders:
        try:
            save_or_update_tender_record(
                db=db,
                tender_payload=t_payload,
                background_tasks=background_tasks,
                force_download=False,
            )
        except Exception as e:
            logger.error(f"Failed to process tender in batch: {e}", exc_info=True)

    db.commit()
    return {"status": "ok", "processed": len(tenders)}

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

def collect_tender_text_for_matching(tender: TenderModel) -> Dict[str, Any]:
    """
    Собирает текст ТЗ для подбора аналогов.
    Приоритет:
    1. extracted_text из БД
    2. тексты файлов из папки тендера
    3. description из CRM, если документов нет
    """
    chunks: List[str] = []
    warnings: List[Dict[str, Any]] = []
    files_used: List[str] = []

    if tender.extracted_text and tender.extracted_text.strip():
        chunks.append(f"[EXTRACTED_TEXT_FROM_DB]\\n{tender.extracted_text.strip()}")

    tender_dir = os.path.join(DOCUMENTS_ROOT, tender.id)
    if os.path.isdir(tender_dir) and doc_service:
        for filename in sorted(os.listdir(tender_dir)):
            file_path = os.path.join(tender_dir, filename)
            if not os.path.isfile(file_path):
                continue

            try:
                data = doc_service.extract_document_data(file_path)
            except Exception as e:
                warnings.append({
                    "filename": filename,
                    "status": "extract_error",
                    "message": str(e),
                })
                continue

            status = data.get("status", "")
            extracted_text = (data.get("extracted_text") or "").strip()

            if status == "success" and extracted_text:
                chunks.append(f"[FILE: {filename}]\\n{extracted_text[:50000]}")
                files_used.append(filename)
            else:
                warnings.append({
                    "filename": filename,
                    "status": status,
                    "message": data.get("error_message") or "Текст не извлечен",
                })

    if not chunks and (tender.description or "").strip():
        chunks.append(f"[CRM_DESCRIPTION]\\n{tender.description.strip()}")
        warnings.append({
            "filename": "CRM_DESCRIPTION",
            "status": "warning",
            "message": "Документы тендера не найдены или не прочитаны. Использовано описание из CRM.",
        })

    return {
        "text": "\\n\\n".join(chunks),
        "warnings": warnings,
        "files_used": files_used,
    }

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

import shutil

@app.post("/api/tenders/{tender_id}/refresh-files")
async def refresh_tender_files(
    tender_id: str,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """Очистить кэш файлов и скачать заново"""
    tender = db.query(TenderModel).filter(TenderModel.id == tender_id).first()
    if not tender:
        raise HTTPException(status_code=404, detail="Тендер не найден в БД")

    tender_dir = os.path.join(DOCUMENTS_ROOT, tender_id)
    if os.path.exists(tender_dir):
        logger.info("Deleting files for tender %s before refresh", tender_id)
        shutil.rmtree(tender_dir, ignore_errors=True)

    payload = tender_model_to_payload(tender)
    notice = build_notice_from_payload(payload)

    result = await run_in_threadpool(eis_service.redownload_tender_documents, notice)
    meta = (result or {}).get("meta", {}) or {}
    reason = (result or {}).get("reason", "unknown")

    files = []
    if os.path.exists(tender_dir):
        for f in os.listdir(tender_dir):
            f_path = os.path.join(tender_dir, f)
            if os.path.isfile(f_path):
                files.append({
                    "name": f,
                    "size": os.path.getsize(f_path),
                    "path": f_path
                })

    if result and result.get("ok") and files:
        logger.info("Refresh tender files completed for %s | files=%s", tender_id, len(files))
        return {
            "status": "success",
            "tender_id": tender_id,
            "files": files,
            "count": len(files),
            "errors": meta.get("failed_downloads", []),
        }

    errors = meta.get("failed_downloads", [])
    if not errors:
        errors = [{
            "reason": reason,
            "message": meta.get("reason", reason),
            "docs_url": meta.get("docs_url", ""),
        }]

    logger.warning(
        "Refresh tender files partial result for %s | reason=%s | errors=%s",
        tender_id,
        reason,
        len(errors),
    )

    return {
        "status": "partial",
        "tender_id": tender_id,
        "files": files,
        "count": len(files),
        "reason": reason,
        "errors": errors,
        "meta": meta,
    }

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

@app.post("/api/ai/extract-tender-requirements")
async def api_extract_tender_requirements(
    data: dict = Body(...),
    db: Session = Depends(get_db),
    _ = Depends(check_ai_service)
):
    """
    Извлечение позиций ТЗ:
    - либо из ручного текста,
    - либо из выбранных тендеров CRM.
    """
    manual_text = (data.get("manual_text") or "").strip()
    tender_ids = data.get("tender_ids") or []

    items: List[Dict[str, Any]] = []
    warnings: List[Dict[str, Any]] = []

    if manual_text:
        extracted = ai_service.extract_tender_requirement_positions(manual_text)
        for index, item in enumerate(extracted, start=1):
            position_name = str(item.get("position_name") or "").strip()
            if not position_name:
                continue

            items.append({
                "id": f"manual-{index}",
                "source": "manual",
                "source_label": "Ручной ввод",
                "position_name": position_name,
                "quantity": str(item.get("quantity") or "").strip(),
                "unit": str(item.get("unit") or "").strip(),
                "characteristics": item.get("characteristics") or [],
                "notes": str(item.get("notes") or "").strip(),
                "search_query": str(item.get("search_query") or position_name).strip(),
            })

    for tender_id in tender_ids:
        tender = db.query(TenderModel).filter(TenderModel.id == tender_id).first()
        if not tender:
            warnings.append({
                "tender_id": tender_id,
                "status": "not_found",
                "message": "Тендер не найден в CRM",
            })
            continue

        source_data = collect_tender_text_for_matching(tender)
        source_text = (source_data.get("text") or "").strip()

        if not source_text:
            warnings.append({
                "tender_id": tender.id,
                "status": "empty_source",
                "message": "Для тендера нет текста ТЗ и нет пригодного описания",
            })
            continue

        extracted = ai_service.extract_tender_requirement_positions(source_text)

        for index, item in enumerate(extracted, start=1):
            position_name = str(item.get("position_name") or "").strip()
            if not position_name:
                continue

            items.append({
                "id": f"{tender.id}-{index}",
                "tender_id": tender.id,
                "tender_title": tender.title or "",
                "source": "crm",
                "source_label": f"{tender.id} — {tender.title or ''}".strip(),
                "position_name": position_name,
                "quantity": str(item.get("quantity") or "").strip(),
                "unit": str(item.get("unit") or "").strip(),
                "characteristics": item.get("characteristics") or [],
                "notes": str(item.get("notes") or "").strip(),
                "search_query": str(item.get("search_query") or position_name).strip(),
            })

        for warning in source_data.get("warnings") or []:
            warnings.append({
                "tender_id": tender.id,
                **warning,
            })

    return {
        "items": items,
        "warnings": warnings,
    }

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

@app.post("/api/tenders/{tender_id}/upload")
async def upload_tender_file(
    tender_id: str,
    file: UploadFile = File(...),
    db: Session = Depends(get_db)
):
    """Вручную загрузить файл в папку тендера"""
    tender = db.query(TenderModel).filter(TenderModel.id == tender_id).first()
    if not tender:
        raise HTTPException(status_code=404, detail="Тендер не найден")

    tender_dir = os.path.join(DOCUMENTS_ROOT, tender_id)
    os.makedirs(tender_dir, exist_ok=True)

    file_path = os.path.join(tender_dir, file.filename)
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    logger.info(f"Manually uploaded file {file.filename} for tender {tender_id}")
    
    return {
        "status": "success",
        "file": {
            "name": file.filename,
            "size": os.path.getsize(file_path),
            "path": file_path
        }
    }

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

# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINTS: ПОДБОР АНАЛОГОВ
# ─────────────────────────────────────────────────────────────────────────────

@app.post("/api/products/search")
async def search_products_local(
    request: dict,
    db: Session = Depends(get_db),
    _ = Depends(check_analog_service)
):
    """
    Поиск аналогов только в локальной БД.
    """
    query = (request.get("query") or "").strip()
    category = request.get("category")
    limit = int(request.get("limit", 10))

    if not query:
        raise HTTPException(status_code=400, detail="query is required")

    results = analog_service.search_local_db(query, category=category, limit=limit)
    return {
        "query": query,
        "results": results,
        "total": len(results),
    }

@app.post("/api/products/search-ai")
async def search_products_ai(request: dict):
    """Поиск аналогов через Gemini AI с Google Search или в комбинированном режиме."""
    query = str(request.get("query", "")).strip()
    requirements = request.get("requirements")
    max_results = int(request.get("max_results", 5) or 5)
    mode = str(request.get("mode", "both")).strip().lower()

    if not query:
        raise HTTPException(status_code=400, detail="query is required")

    if analog_service is None:
        raise HTTPException(status_code=503, detail="AnalogService is not initialized")

    logger.info(f"AI analog search: '{query}' | mode={mode}")

    result = await analog_service.search_analogs(
        query=query,
        requirements=requirements,
        mode=mode,
        limit=max_results
    )
    return result

@app.post("/api/products/refresh-catalog")
async def refresh_catalog(background_tasks: BackgroundTasks):
    """Запускает фоновое обновление каталога с gidroizol.ru."""
    async def _do_refresh():
        logger.info("[CATALOG] Starting catalog refresh from gidroizol.ru...")
        try:
            products = parser_service.parse_full_catalog(max_categories=5)
            from sqlalchemy import text
            import json as json_lib
            with next(get_db()) as db:
                saved = 0
                for p in products:
                    try:
                        db.execute(
                            text(
                                "INSERT OR REPLACE INTO products "
                                "(title, category, material_type, price, specs, url, description) "
                                "VALUES (:title, :cat, :mat, :price, :specs, :url, :desc)"
                            ),
                            {
                                "title": p.get("title", ""),
                                "cat": p.get("category", ""),
                                "mat": p.get("material_type", ""),
                                "price": p.get("price"),
                                "specs": json_lib.dumps(p.get("specs", {}), ensure_ascii=False),
                                "url": p.get("url", ""),
                                "desc": p.get("description", ""),
                            }
                        )
                        saved += 1
                    except Exception as e:
                        logger.debug(f"[CATALOG] Skip product: {e}")
                db.commit()
            logger.info(f"[CATALOG] Refresh complete. Saved {saved} products.")
        except Exception as e:
            logger.error(f"[CATALOG] Refresh failed: {e}")

    background_tasks.add_task(_do_refresh)
    return {"status": "started", "message": "Обновление каталога запущено в фоне"}


@app.post("/api/products")
async def create_product(request: dict, db: Session = Depends(get_db)):
    """Добавить товар в локальную БД вручную."""
    from sqlalchemy import text
    import json as json_lib
    try:
        specs_json = json_lib.dumps(request.get("specs", {}), ensure_ascii=False)
        result = db.execute(
            text(
                "INSERT INTO products (title, category, material_type, price, specs, url, description) "
                "VALUES (:title, :cat, :mat, :price, :specs, :url, :desc)"
            ),
            {
                "title": request.get("title", ""),
                "cat": request.get("category", ""),
                "mat": request.get("material_type", ""),
                "price": request.get("price"),
                "specs": specs_json,
                "url": request.get("url", ""),
                "desc": request.get("description", ""),
            }
        )
        db.commit()
        return {"id": result.lastrowid, "status": "created"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/products/{product_id}")
async def delete_product(product_id: int, db: Session = Depends(get_db)):
    """Удалить товар из БД."""
    from sqlalchemy import text
    db.execute(text("DELETE FROM products WHERE id = :id"), {"id": product_id})
    db.commit()
    return {"status": "deleted", "id": product_id}

# ─────────────────────────────────────────────────────────────────────────────
# КОНЕЦ НОВЫХ ENDPOINTS
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    uvicorn.run("backend.main:app", host="0.0.0.0", port=8000, reload=True)
