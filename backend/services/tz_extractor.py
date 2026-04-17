import re
import json
from dataclasses import dataclass, asdict, field
from typing import List, Dict, Any, Optional, Tuple

# ============================================================
# UNIVERSAL TENDER TZ EXTRACTOR FOR CONSTRUCTION MATERIALS
# Работает по plain text / тексту после OCR / тексту из docx/pdf/xlsx.
# Не привязан к кровельным материалам: рассчитан на любые строительные материалы.
# ============================================================

EXCLUDE_SECTION_PATTERNS = [
    r"\bпорядок оплаты\b",
    r"\bответственност[ьи]\b",
    r"\bштраф\b",
    r"\bпен[яеи]\b",
    r"\bрасторжени[ея]\b",
    r"\bбанковск\w+\s+гарант",
    r"\bобеспечени[ея]\s+заявк",
    r"\bобеспечени[ея]\s+исполнени",
    r"\bреестр\b",
    r"\bнациональн\w+\s+режим\b",
    r"\bнмцк\b",
    r"\bобосновани\w+\s+цен",
    r"\bпорядок\s+подачи\s+заяв",
    r"\bкритери\w+\s+оценк",
    r"\bтребовани\w+\s+к\s+участник",
    r"\bанкет\w+\b",
    r"\bсогласие\s+на\s+обработку\b",
    r"\bцепочк\w+\s+собственник",
    r"\bантикоррупцион\w+\b",
    r"\bюридическ\w+\s+адрес\b",
    r"\bбанковск\w+\s+реквизит",
]

SECTION_START_PATTERNS = [
    r"\bтехническ\w+\s+задани",
    r"\bописани\w+\s+предмета\s+закупк",
    r"\bспецификаци",
    r"\bведомост\w+\s+материал",
    r"\bперечен\w+\s+материал",
    r"\bнаименование\s+товара",
    r"\bхарактеристик",
    r"\bтребования\s+к\s+продукци",
    r"\bобщие\s+требования\s+к\s+товар",
]

SECTION_END_PATTERNS = [
    r"\bпроект\s+договора\b",
    r"\bцена\s+договора\b",
    r"\bпорядок\s+приемк",
    r"\bответственност[ьи]\s+сторон\b",
    r"\bобстоятельств\w+\s+непреодолимой\s+силы\b",
    r"\bпорядок\s+рассмотрения\s+споров\b",
    r"\bзаключительн\w+\s+услов",
    r"\bреквизит",
    r"\bобосновани\w+\s+начальн",
    r"\bраздел\s+\d+\.\s+формы\s+документ",
]

UNIT_PATTERNS = [
    r"\bм2\b", r"\bм3\b", r"\bм\b", r"\bмм\b",
    r"\bкг\b", r"\bт\b", r"\bл\b", r"\bшт\b",
    r"\bупак\b", r"\bмеш\w*\b", r"\bрулон\w*\b",
    r"\bведро\b", r"\bканистр\w*\b", r"\bлист\w*\b",
    r"\bкомпл\w*\b", r"\bпаллет\w*\b", r"\bбухт\w*\b",
    r"\bпог\.\s*м\b", r"\bм\.п\.\b",
]

MATERIAL_HINTS = [
    # Общестроительные
    "кирпич", "блок", "цемент", "бетон", "раствор", "смесь", "шпатлев", "штукатур",
    "грунтовк", "краск", "эмаль", "лак", "герметик", "клей", "пена",
    "утеплител", "вата", "пенополистирол", "пеноплекс", "экструз",
    "гипсокартон", "гвл", "фанер", "осп", "osb", "дсп", "доска", "брус",
    "профнастил", "металлочерепиц", "лист оцинк", "арматур", "сетка",
    "труба", "фитинг", "муфт", "отвод", "кран", "кабель", "провод",
    "плитка", "керамогранит", "линолеум", "ламинат", "мембран", "геотекстил",
    "праймер", "мастик", "рубероид", "линокром", "техноэласт", "унифлекс",
    "рулемаст", "рубеmast", "брит", "изопласт", "битум", "шпонк", "лента"
]

SPEC_KEYWORDS = [
    "толщина", "ширина", "длина", "масса", "плотность", "фракция", "зернистость",
    "марка", "класс", "прочность", "разрывная сила", "гибкость", "теплостойкость",
    "теплопроводность", "влагостойкость", "морозостойкость", "горючесть",
    "водопоглощение", "адгезия", "расход", "время высыхания", "основа",
    "тип покрытия", "покрытие", "назначение", "применение", "цвет",
    "формат", "размер", "диаметр", "длина рулона", "ширина рулона",
    "температура", "давление", "напряжение", "сечение", "материал основы"
]

GENERAL_REQUIREMENT_HINTS = [
    "товар должен быть новым",
    "не бывшим в употреблении",
    "гарантийный срок",
    "сертификат",
    "декларация соответствия",
    "упаковка",
    "маркировка",
    "дата изготовления",
    "срок поставки",
    "доставка",
    "аналог",
    "эквивалент",
    "идентичности технических характеристик",
]

EXACT_NAME_HINTS = [
    r"\b[А-ЯA-Z]{2,}[ -]?[А-ЯA-Z0-9-]{2,}\b",   # грубая эвристика для марок/моделей
    r"\b[Рр]убемаст\b",
    r"\b[Лл]инокром\b",
    r"\b[Тт]ехноэласт\b",
    r"\b[Уу]нифлекс\b",
    r"\b[Бб]рит\b",
]


@dataclass
class RequirementItem:
    position_name: str
    normalized_name: str = ""
    quantity: Optional[str] = None
    unit: Optional[str] = None
    characteristics: List[str] = field(default_factory=list)
    notes: Optional[str] = None
    search_query: str = ""
    source_fragment: Optional[str] = None
    extraction_mode: str = "parametric"   # parametric | exact_name | mixed
    confidence: float = 0.0


@dataclass
class ExtractionResult:
    items: List[RequirementItem] = field(default_factory=list)
    general_requirements: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    debug: Dict[str, Any] = field(default_factory=dict)


def normalize_spaces(text: str) -> str:
    text = text.replace("\xa0", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def split_lines(text: str) -> List[str]:
    return [line.strip() for line in normalize_spaces(text).splitlines() if line.strip()]


def contains_any(text: str, patterns: List[str], flags=re.I) -> bool:
    return any(re.search(p, text, flags) for p in patterns)


def looks_like_excluded_section(text: str) -> bool:
    return contains_any(text, EXCLUDE_SECTION_PATTERNS)


def looks_like_tz_section_start(text: str) -> bool:
    return contains_any(text, SECTION_START_PATTERNS)


def looks_like_tz_section_end(text: str) -> bool:
    return contains_any(text, SECTION_END_PATTERNS)


def extract_relevant_text(text: str) -> Tuple[str, Dict[str, Any]]:
    """
    Извлекает релевантные блоки ТЗ.
    Если явный раздел ТЗ найден — берёт его и соседние таблицы/строки.
    Если нет — использует fallback по score блоков.
    """
    lines = split_lines(text)
    if not lines:
        return "", {"mode": "empty", "blocks": 0}

    blocks: List[str] = []
    current: List[str] = []
    for line in lines:
        if re.match(r"^\s*раздел\s+\d+", line, re.I) and current:
            blocks.append("\n".join(current))
            current = [line]
        else:
            current.append(line)
    if current:
        blocks.append("\n".join(current))

    selected_blocks = []
    in_tz = False
    for block in blocks:
        low = block.lower()
        if looks_like_tz_section_start(low):
            in_tz = True
        if in_tz and not looks_like_excluded_section(low):
            selected_blocks.append(block)
        if in_tz and looks_like_tz_section_end(low):
            in_tz = False

    if selected_blocks:
        return "\n\n".join(selected_blocks), {"mode": "explicit_section", "blocks": len(selected_blocks)}

    scored = []
    for block in blocks:
        low = block.lower()
        score = 0
        if contains_any(low, SECTION_START_PATTERNS):
            score += 5
        if any(h in low for h in MATERIAL_HINTS):
            score += 3
        if any(h in low for h in SPEC_KEYWORDS):
            score += 2
        if re.search(r"\bкол-?во\b|\bколичество\b", low):
            score += 2
        if "|" in block:
            score += 2
        if looks_like_excluded_section(low):
            score -= 6
        if score >= 3:
            scored.append(block)

    return "\n\n".join(scored), {"mode": "fallback_scored_blocks", "blocks": len(scored)}


def detect_general_requirements(text: str) -> List[str]:
    lines = split_lines(text)
    found = []
    for line in lines:
        low = line.lower()
        if any(h in low for h in GENERAL_REQUIREMENT_HINTS):
            found.append(line)
    return dedupe_preserve_order(found)


def dedupe_preserve_order(values: List[str]) -> List[str]:
    seen = set()
    out = []
    for v in values:
        k = normalize_spaces(v).lower()
        if not k or k in seen:
            continue
        seen.add(k)
        out.append(normalize_spaces(v))
    return out


def clean_cell(cell: str) -> str:
    cell = normalize_spaces(cell)
    cell = re.sub(r"^[№#\-\u2022\*]+\s*", "", cell)
    return cell.strip(" |;")


def parse_table_like_lines(text: str) -> List[List[str]]:
    rows = []
    for line in split_lines(text):
        if line.count("|") >= 2:
            parts = [clean_cell(p) for p in line.split("|")]
            parts = [p for p in parts if p]
            if len(parts) >= 3:
                rows.append(parts)
    return rows


def detect_quantity_and_unit(text: str) -> Tuple[Optional[str], Optional[str]]:
    low = text.lower().replace(",", ".")
    patterns = [
        r"(\d+(?:[ .]\d+)?(?:[.,]\d+)?)\s*(м2|м3|м|мм|кг|т|л|шт|упак(?:овка)?|меш(?:ок)?|рулон(?:ов)?|ведро|канистр(?:а|ы)?|лист(?:ов)?|компл(?:ект)?|паллет(?:а|ы)?|бухт(?:а|ы)?)\b",
        r"(не менее\s+\d+(?:[.,]\d+)?)\s*(м2|м3|м|мм|кг|т|л|шт)\b",
    ]
    for pat in patterns:
        m = re.search(pat, low, re.I)
        if m:
            return m.group(1).replace(" ", ""), m.group(2)
    return None, None


def extract_characteristics_from_text(text: str) -> List[str]:
    lines = split_lines(text)
    chars = []
    for line in lines:
        low = line.lower()
        if any(k in low for k in SPEC_KEYWORDS):
            chars.append(line)
            continue
        if re.search(r"\bне\s+(менее|более|выше|ниже)\b", low):
            chars.append(line)
            continue
        if re.search(r"\b\d+(?:[.,]\d+)?\s*(мм|м|м2|м3|кг|г/м2|кг/м2|н/50\s*мм|°с|с0|в|квт|мпа)\b", low):
            chars.append(line)
    return dedupe_preserve_order(chars)


def normalize_material_name(name: str) -> str:
    name = normalize_spaces(name)
    name = re.sub(r"^[0-9]+[.)]?\s*", "", name)
    name = re.sub(r"\b(ед\.?\s*изм\.?|кол-?во|количество|цена|стоимость|окпд2|код енс)\b.*$", "", name, flags=re.I)
    name = re.sub(r"\bили\s+эквивалент\b", "", name, flags=re.I)
    name = re.sub(r"\bэквивалент\b", "", name, flags=re.I)
    name = re.sub(r"\s{2,}", " ", name)
    return name.strip(" ,;:-")


def build_search_query(name: str, characteristics: List[str]) -> str:
    """
    Для поиска аналогов надо оставлять:
    - само название / марку / тип;
    - только ключевые характеристики 2-4 шт;
    - не тащить длинное ТЗ целиком.
    """
    q = normalize_material_name(name)

    important = []
    for c in characteristics:
        low = c.lower()
        if any(k in low for k in [
            "толщина", "ширина", "длина", "масса", "плотность", "марка",
            "класс", "основа", "тип покрытия", "покрытие", "диаметр",
            "сечение", "прочность", "гибкость", "теплостойкость", "расход",
            "морозостойкость", "горючесть"
        ]):
            important.append(c)

    important = important[:4]
    if important:
        q = q + " | " + "; ".join(important)

    q = re.sub(r"\s{2,}", " ", q).strip(" |;")
    return q


def looks_like_material_name(text: str) -> bool:
    low = text.lower()
    if any(h in low for h in MATERIAL_HINTS):
        return True
    if contains_any(text, EXACT_NAME_HINTS):
        return True
    if re.search(r"\bматериал\b|\bтовар\b|\bпродукц", low):
        return True
    return False


def parse_parametric_rows(text: str) -> List[RequirementItem]:
    rows = parse_table_like_lines(text)
    items: List[RequirementItem] = []

    for row in rows:
        raw = " | ".join(row)
        low = raw.lower()

        # ищем строку, похожую на товарную
        joined = " ".join(row)
        if not looks_like_material_name(joined):
            continue

        qty, unit = detect_quantity_and_unit(joined)
        if qty is None:
            # отдельный поиск количества по последним ячейкам
            for cell in reversed(row):
                qty, unit = detect_quantity_and_unit(cell)
                if qty:
                    break

        # выбираем вероятное название
        candidate_name = ""
        for cell in row:
            if looks_like_material_name(cell) and len(cell) > len(candidate_name):
                candidate_name = cell

        if not candidate_name:
            continue

        chars = extract_characteristics_from_text(joined)
        mode = "parametric"

        items.append(
            RequirementItem(
                position_name=normalize_material_name(candidate_name),
                normalized_name=normalize_material_name(candidate_name),
                quantity=qty,
                unit=unit,
                characteristics=chars,
                notes=None,
                search_query=build_search_query(candidate_name, chars),
                source_fragment=raw,
                extraction_mode=mode,
                confidence=0.86 if qty else 0.74
            )
        )

    return items


def parse_inline_positions(text: str) -> List[RequirementItem]:
    """
    Режим для документов, где позиции заданы строками без нормальной таблицы:
    1. РУБЕМАСТ РНК 350 — 900 м2
    2. Смесь сухая штукатурная ... 120 мешков
    """
    items: List[RequirementItem] = []
    lines = split_lines(text)

    for line in lines:
        low = line.lower()
        if looks_like_excluded_section(low):
            continue
        if not looks_like_material_name(line):
            continue

        qty, unit = detect_quantity_and_unit(line)
        chars = extract_characteristics_from_text(line)

        if qty or len(line) < 300:
            name = line
            if qty and unit:
                name = re.sub(rf"(\d+(?:[ .]\d+)?(?:[.,]\d+)?)\s*{re.escape(unit)}\b.*$", "", name, flags=re.I)
            name = normalize_material_name(name)

            if len(name) >= 5:
                mode = "exact_name" if contains_any(name, EXACT_NAME_HINTS) else "mixed"
                items.append(
                    RequirementItem(
                        position_name=name,
                        normalized_name=name,
                        quantity=qty,
                        unit=unit,
                        characteristics=chars,
                        notes=None,
                        search_query=build_search_query(name, chars),
                        source_fragment=line,
                        extraction_mode=mode,
                        confidence=0.76 if qty else 0.62
                    )
                )

    return items


def merge_similar_items(items: List[RequirementItem]) -> List[RequirementItem]:
    merged: Dict[str, RequirementItem] = {}

    for item in items:
        key = re.sub(r"\s+", " ", item.normalized_name.lower()).strip()
        if not key:
            continue

        if key not in merged:
            merged[key] = item
            continue

        base = merged[key]
        if not base.quantity and item.quantity:
            base.quantity = item.quantity
        if not base.unit and item.unit:
            base.unit = item.unit

        base.characteristics = dedupe_preserve_order(base.characteristics + item.characteristics)

        if item.confidence > base.confidence:
            base.confidence = item.confidence
            base.source_fragment = item.source_fragment

        # если один режим exact_name, а другой parametric, ставим mixed
        if base.extraction_mode != item.extraction_mode:
            base.extraction_mode = "mixed"

        base.search_query = build_search_query(base.normalized_name, base.characteristics)

    return list(merged.values())


def attach_global_characteristics(items: List[RequirementItem], general_requirements: List[str]) -> List[RequirementItem]:
    """
    Если документ содержит общую таблицу характеристик для всех позиций,
    а у позиций своих характеристик мало — добавляем общие требования как общие характеристики.
    """
    global_chars = []
    for req in general_requirements:
        low = req.lower()
        if any(k in low for k in SPEC_KEYWORDS) or re.search(r"\bне\s+(менее|выше|ниже|более)\b", low):
            global_chars.append(req)

    global_chars = dedupe_preserve_order(global_chars)

    out = []
    for item in items:
        if len(item.characteristics) < 3 and global_chars:
            item.characteristics = dedupe_preserve_order(item.characteristics + global_chars[:12])
            item.search_query = build_search_query(item.normalized_name, item.characteristics)
        out.append(item)
    return out


def validate_item(item: RequirementItem) -> bool:
    if not item.position_name or len(item.position_name) < 4:
        return False
    if looks_like_excluded_section(item.position_name.lower()):
        return False
    return True


def extract_tz_from_text(text: str) -> Dict[str, Any]:
    relevant_text, debug_info = extract_relevant_text(text)
    if not relevant_text:
        return asdict(ExtractionResult(
            items=[],
            general_requirements=[],
            warnings=["Не найден релевантный фрагмент ТЗ/спецификации"],
            debug=debug_info
        ))

    general_requirements = detect_general_requirements(relevant_text)

    table_items = parse_parametric_rows(relevant_text)
    inline_items = parse_inline_positions(relevant_text)

    items = merge_similar_items(table_items + inline_items)
    items = attach_global_characteristics(items, general_requirements)
    items = [item for item in items if validate_item(item)]

    warnings = []
    if not items:
        warnings.append("Позиции не выделены автоматически. Требуется ручная проверка блока ТЗ.")
    if len(items) > 50:
        warnings.append("Найдено аномально много позиций. Возможна ошибка OCR или захват служебных таблиц.")

    result = ExtractionResult(
        items=items,
        general_requirements=general_requirements,
        warnings=warnings,
        debug=debug_info
    )
    return asdict(result)


MODEL_AGNOSTIC_PROMPT = """
Ты получаешь уже очищенный фрагмент ТЗ/спецификации по строительным материалам.
Извлеки только товарные позиции и общие требования к товару.
Не извлекай требования к участнику, оплате, доставке, санкциям, реквизитам, НМЦК.
Верни строго JSON:
{
  "items": [
    {
      "position_name": "...",
      "normalized_name": "...",
      "quantity": "...",
      "unit": "...",
      "characteristics": ["...", "..."],
      "notes": "...",
      "search_query": "...",
      "extraction_mode": "parametric|exact_name|mixed"
    }
  ],
  "general_requirements": ["...", "..."],
  "warnings": ["..."]
}

Правила:
1. Если позиция задана точным наименованием/маркой, сохрани её как есть.
2. Если позиция задана через параметры, собери название из типа материала и ключевых признаков.
3. Количество и единицы измерения извлекай отдельно.
4. Общие характеристики добавляй в characteristics только если они относятся к товару.
5. search_query должен быть коротким и пригодным для поиска аналогов.
""".strip()
