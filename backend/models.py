from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    JSON,
    String,
    Text,
    UniqueConstraint,
)
from datetime import datetime
from .database import Base

class ProductModel(Base):
    __tablename__ = "products"

    id = Column(Integer, primary_key=True, index=True)
    vendor = Column(String, default="gidroizol", nullable=False)
    site_product_id = Column(String, nullable=True)
    source_url = Column(String, nullable=True)
    city_id = Column(Integer, default=1, nullable=False)
    title = Column(String, index=True)
    category = Column(String)
    category_leaf = Column(String, nullable=True)
    normalized_category = Column(String, nullable=True)
    searchable_for_analogs = Column(Boolean, default=True)
    material_type = Column(String, default="Гидроизоляция")
    price = Column(Float, default=0.0)
    price_wholesale = Column(Float, nullable=True)
    price_special = Column(Float, nullable=True)
    price_currency = Column(String, default="RUB")
    price_unit = Column(String, nullable=True)
    availability_status = Column(String, default="unknown")
    specs = Column(JSON, default=dict)
    specs_text = Column(Text, nullable=True)
    url = Column(String, nullable=True, unique=True)
    description = Column(Text, nullable=True)
    meta_description = Column(Text, nullable=True)
    quality_score = Column(Integer, default=0)
    content_hash = Column(String, nullable=True)
    parse_version = Column(String, nullable=True)
    first_seen_at = Column(DateTime, default=datetime.utcnow)
    last_seen_at = Column(DateTime, default=datetime.utcnow)
    scraped_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    is_active = Column(Boolean, default=True)


class ProductAnalogIndexModel(Base):
    __tablename__ = "product_analog_index"

    product_id = Column(Integer, ForeignKey("products.id", ondelete="CASCADE"), primary_key=True)
    normalized_title = Column(Text, nullable=False)
    brand = Column(String, nullable=True)
    series = Column(String, nullable=True)
    product_family = Column(String, nullable=True)
    material_group = Column(String, nullable=False)
    material_subgroup = Column(String, nullable=True)
    application_scope = Column(String, nullable=True)
    base_material = Column(String, nullable=True)
    binder_type = Column(String, nullable=True)
    top_surface = Column(String, nullable=True)
    bottom_surface = Column(String, nullable=True)
    thickness_mm = Column(Float, nullable=True)
    mass_kg_m2 = Column(Float, nullable=True)
    density_kg_m3 = Column(Float, nullable=True)
    roll_length_m = Column(Float, nullable=True)
    roll_width_m = Column(Float, nullable=True)
    roll_area_m2 = Column(Float, nullable=True)
    package_weight_kg = Column(Float, nullable=True)
    flexibility_temp_c = Column(Float, nullable=True)
    heat_resistance_c = Column(Float, nullable=True)
    color = Column(String, nullable=True)
    standard_code = Column(String, nullable=True)
    extracted_attrs_json = Column(JSON, default=dict)
    search_text = Column(Text, nullable=False)
    analog_group_key = Column(String, nullable=True)
    exact_model_key = Column(String, nullable=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class ProductAliasModel(Base):
    __tablename__ = "product_aliases"
    __table_args__ = (
        UniqueConstraint("product_id", "alias_normalized", name="uq_product_aliases_product_norm"),
    )

    id = Column(Integer, primary_key=True, index=True)
    product_id = Column(Integer, ForeignKey("products.id", ondelete="CASCADE"), nullable=False, index=True)
    alias = Column(Text, nullable=False)
    alias_normalized = Column(Text, nullable=False, index=True)
    alias_type = Column(String, nullable=False, default="source_title")

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
    customer_name = Column(String, nullable=True)
    customer_inn = Column(String, nullable=True)
    customer_location = Column(Text, nullable=True)
    local_file_path = Column(String, nullable=True)
    extracted_text = Column(Text, nullable=True)
    selected_for_matching = Column(Boolean, default=False, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
