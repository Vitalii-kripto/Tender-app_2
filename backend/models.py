from sqlalchemy import Column, Integer, String, Float, JSON, DateTime, Text, Boolean
from datetime import datetime
from .database import Base

class ProductModel(Base):
    __tablename__ = "products"

    id = Column(Integer, primary_key=True, index=True)
    title = Column(String, index=True)
    category = Column(String)
    material_type = Column(String, default="Гидроизоляция")
    price = Column(Float, default=0.0)
    specs = Column(JSON, default={})
    url = Column(String, unique=True)
    description = Column(Text, nullable=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class TenderModel(Base):
    __tablename__ = "tenders"

    id = Column(String, primary_key=True, index=True)
    title = Column(String)
    description = Column(Text)
    initial_price = Column(Float)
    deadline = Column(String)
    status = Column(String, default="Found")
    risk_level = Column(String, default="Low")
    region = Column(String)
    law_type = Column(String, default="44-ФЗ")
    url = Column(String)
    docs_url = Column(String, nullable=True)
    search_url = Column(String, nullable=True)
    keyword = Column(String, nullable=True)
    ntype = Column(String, nullable=True)
    local_file_path = Column(String, nullable=True)
    extracted_text = Column(Text, nullable=True)
    selected_for_matching = Column(Boolean, default=False, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
