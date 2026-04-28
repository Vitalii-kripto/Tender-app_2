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
    "рулемаст", "рубеmast", "брит", "изопласт", "битум", "шпонк", "лента", "резин",
    "шнур", "жгут", "пленк",
    "гидроизоляц", "бетоноконтакт"
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
    "поставляемый товар должен быть новым",
    "не бывшим в употреблении",
    "гарантийный срок",
    "гарантия качества",
    "сертификат",
    "декларация соответствия",
    "упаковка",
    "маркировка",
    "дата изготовления",
    "срок поставки",
    "сроки поставки",
    "место поставки",
    "доставка",
    "доля вторичного сырья",
    "аналог",
    "эквивалент",
    "идентичности технических характеристик",
]

CHARACTERISTIC_MARKER_PATTERNS = [
    r"Материал\s*:",
    r"Цвет\s*:",
    r"Покрытие\s*:",
    r"Высота(?:\s*\(мм\)|,\s*мм)?\s*:",
    r"Ширина(?:\s+листа|\s+общая|\s+рулона)?(?:\s*\(мм\)|,\s*м|,\s*мм)?\s*:",
    r"Длина(?:\s+рулона)?(?:\s*\(мм\)|,\s*мм)?\s*:",
    r"Диаметр(?:,\s*мм)?\s*:",
    r"Толщина(?:\s+металла)?(?:\s*\(мм\)|,\s*мм)?\s*:",
    r"Профиль\s*:",
    r"Серия\s*:",
    r"Основа покрытия\s*:",
    r"Цветовой оттенок\s*:",
    r"Тип формы\s*:",
    r"Тип продукции\s*:",
    r"Тип изоляции\s*:",
    r"Тип наконечника\s*:",
    r"Тип шлица\s*:",
    r"Тип шляпки\s*\(головка\)\s*:",
    r"Шайба\s*:",
    r"Водонепроницаемость\s*:",
    r"Водоупорность(?:,\s*мм\.?\s*вод\.?\s*ст\.?)?\s*:",
    r"Количество слоев(?:,\s*шт)?\s*:",
    r"Площадь рулона(?:,\s*м2)?\s*:",
    r"Вид\s*:",
    r"Размер\s*:",
]

CHARACTERISTIC_SENTENCE_MARKERS = [
    r"По\s+способу\s+монтажа\b",
    r"Толщина\b",
    r"Ширина\b",
    r"Высота\b",
    r"Длина\b",
    r"Длина\s+рулона\b",
    r"Диаметр\b",
    r"Посыпка\b",
    r"Основа\b",
    r"Покрытие\b",
    r"Тип\b",
    r"Количество\s+слоев\b",
    r"Площадь\s+рулона\b",
    r"Водоупорность\b",
    r"Верхняя\s+сторона\b",
    r"Нижняя\s+сторона\b",
    r"Время\s+высыхания\b",
    r"Температура\s+(?:размягчения|применения)\b",
    r"Расход\b",
    r"Массовая\s+доля\b",
    r"Условная\s+вязкость\b",
    r"Тип\s+материала\b",
    r"Назначение\b",
    r"Группа\s+горючести\b",
    r"Прочность\s+сцепления\b",
    r"Теплостойкость\b",
    r"Условная\s+прочность\b",
    r"Относительное\s+удлинение\b",
    r"Минимально\s+допустимая\s+температура\b",
    r"Максимально\s+допустимая\s+температура\b",
    r"Класс\s+пожарной\s+опасности\b",
]

CHARACTERISTIC_LABEL_RE = (
    r"(Посыпка|Основа покрытия|Основа|Цветовой оттенок|Цвет|Покрытие|"
    r"Высота(?:\s*\(мм\)|,\s*мм)?|Ширина(?:\s+листа|\s+общая|\s+рулона)?(?:\s*\(мм\)|,\s*м|,\s*мм)?|"
    r"Длина(?:\s+рулона)?(?:\s*\(мм\)|,\s*мм)?|Диаметр(?:,\s*мм)?|Толщина(?:\s+металла)?(?:\s*\(мм\)|,\s*мм)?|"
    r"Профиль|Серия|Материал|Вид|Размер|Тип продукции|Тип формы|Тип изоляции|Тип материала|Тип|"
    r"Тип наконечника|Тип шлица|Тип шляпки\s*\(головка\)|Шайба|"
    r"Количество слоев(?:,\s*шт)?|Площадь рулона(?:,\s*м2)?|"
    r"Верхняя сторона|Нижняя сторона|"
    r"Водонепроницаемость|Водоупорность(?:,\s*мм\.?\s*вод\.?\s*ст\.?)?|"
    r"Назначение|Способ монтажа|Время высыхания(?:\s+нанес[её]нного\s+слоя)?|"
    r"Температура(?:\s+размягчения|\s+применения)?|Расход|Массовая доля|"
    r"Условная вязкость|Группа горючести|Прочность сцепления(?:\s*\(адгезия\)\s*с\s*(?:бетоном|металлом))?|"
    r"Теплостойкость|Условная прочность|Относительное удлинение(?:\s+при\s+максимальной\s+силе\s+растяжения)?|"
    r"Минимально допустимая температура(?:\s+для\s+применения)?|"
    r"Максимально допустимая температура(?:\s+для\s+применения)?|"
    r"Класс пожарной опасности материала|Внешний вид|Вид товара|Тара)"
)

EXACT_NAME_HINTS = [
    r"\b[А-ЯA-Z]{1,5}-?\d{1,4}(?:[.,]\d+)?\b",
    r"\b(?:ТКП|ТКП-4,5|ТПП|ХПП|ХКП|ЭПП|ЭКП|ЭМП)\b",
    r"\b[Рр]убемаст\b",
    r"\b[Лл]инокром\b",
    r"\b[Тт]ехноэласт\b",
    r"\b[Уу]нифлекс\b",
    r"\b[Бб]рит\b",
]

SERVICE_ROW_PATTERNS = [
    r"^\s*[xivlcdm0-9.\- ]*техническ\w+\s+задани[ея]?\s*$",
    r"^\s*[xivlcdm0-9.\- ]*техническ\w+\s+част",
    r"^\s*приложени[ея]\b",
    r"^\s*таблиц[аы]\b",
    r"^\s*=+\s*лист\s*:",
    r"^\s*предмет\s+контракт",
    r"^\s*функциональн\w+.*характеристик",
    r"^\s*что\s+указано\s+в\s+тд\b",
    r"^\s*что\s+найдено\b",
    r"\bнаименование\s+товара(?:,\s*работы,\s*услуги)?\b",
    r"\bкод\s+позиции\b",
    r"\bтип\s+позиции\b",
    r"\bцена\s+за\s+единицу\b",
    r"\bстоимость\s+позиции\b",
    r"\bхарактеристики\s+товара(?:,\s*работы,\s*услуги)?\b",
    r"\bнаименование\s+характеристики\b",
    r"\bзначение\s+характеристики\b",
    r"\bединица\s+измерения\s+характеристики\b",
    r"\bинструкция\s+по\s+заполнению\s+характеристики\b",
    r"\bзначение\s+характеристики\s+не\s+может\s+изменяться\b",
    r"\bидентификатор\b",
    r"\bневозможно\s+определить\s+количество\b",
    r"\bцена\s+контракта\b",
]

NON_PRODUCT_LINE_PATTERNS = [
    r"^\s*идентификационн\w+\s+код\s+закуп",
    r"^\s*информаци\w+\s+о\s+заказчик",
    r"^\s*общие\s+сведени\w+",
    r"^\s*описани\w+\s+задач",
    r"^\s*полное\s+наименовани\w+\s+заказчик",
    r"^\s*место\s+нахождени\w+",
    r"^\s*почтов\w+\s+адрес",
    r"^\s*ответственн\w+\s+должностн\w+\s+лиц",
    r"^\s*адрес\s+электронн\w+\s+почт",
    r"^\s*номер\s+контактн\w+\s+телефон",
    r"^\s*источник\s+финансирован",
    r"^\s*начальн\w+.*цен",
    r"^\s*сроки?\s+действи\w+\s+договор",
    r"^\s*сроки?\s+поставк",
    r"^\s*место\s+поставк",
    r"^\s*услови\w+\s+оплат",
    r"^\s*авансирован",
    r"^\s*приемк",
    r"^\s*цена\s+договор",
    r"^\s*сумма\s+контракт",
    r"^\s*код\s+видов\s+расход",
    r"^\s*номер\s+банковск",
    r"^\s*наименовани\w+\s+контрагент",
    r"^\s*дополнительн\w+\s+информац",
    r"^\s*характеристик\w+\s+объекта\s+закуп",
    r"^\s*качественн\w+\s+характеристик\w+.*объекта\s+закуп",
    r"^\s*требовани\w+\s+к\s+маркировк",
    r"^\s*декларировани\w+\s+факт",
    r"^\s*копи\w+\s+уведомлен",
    r"^\s*требуетс\w+\s+гаранти",
    r"^\s*гарантийн\w+\s+срок",
    r"^\s*грузополучател",
    r"^\s*объем\s+поставляемого\s+товара",
    r"^\s*результат\s+поставки\s+товара",
    r"^\s*одновременно\s+с\s+товаром",
    r"^\s*оригинал\s+товарной\s+накладной",
    r"^\s*а\)\s+товарную\s+накладную",
    r"^\s*инспектор\b",
    r"^\s*заказчик\b.*\b(обязан|вправе|производит|осуществляет)\b",
    r"^\s*поставщик\b.*\b(должен|обязуетс|гарантирует)\b",
    r"^\s*[•\u2022\*]\s*(письма|выписки|документаци)\b",
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
    text = text.replace("²", "2").replace("³", "3")
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


def has_material_hint(text: str) -> bool:
    low = normalize_spaces(text).lower()
    return any(h in low for h in MATERIAL_HINTS)


def normalize_unit_label(text: str) -> Optional[str]:
    low = normalize_spaces(text).lower()
    low = re.sub(r"\s*\([^)]*\)\s*$", "", low).strip(" .")
    unit_aliases = [
        (r"(м2|м²|кв\.?\s*м\.?|квадратн\w+\s+метр\w*)", "м2"),
        (r"(м3|м³|куб\.?\s*м\.?|кубическ\w+\s+метр\w*)", "м3"),
        (r"(пог\.?\s*м\.?|погонн\w+\s+метр\w*|м\.?\s*п\.?)", "м"),
        (r"(м|метр\w*)", "м"),
        (r"(мм|миллиметр\w*)", "мм"),
        (r"(кг|килограмм\w*)", "кг"),
        (r"(метрическ\w+\s+тонн\w*|т|тонн\w*)", "т"),
        (r"(л|литр\w*)", "л"),
        (r"(шт|штук\w*|штука\w*)", "шт"),
        (r"(упак(?:овка)?|упаковк\w*)", "упаковка"),
        (r"(меш(?:ок)?|мешк\w*)", "мешок"),
        (r"(рул(?:он)?(?:ов)?|рул\.?|рулон\w*)", "рулон"),
        (r"(ведро|ведр\w*)", "ведро"),
        (r"(канистр(?:а|ы)?|канистр\w*)", "канистра"),
        (r"(лист(?:ов)?|лист\w*)", "лист"),
        (r"(компл(?:ект)?|комплект\w*)", "комплект"),
        (r"(паллет(?:а|ы)?|паллет\w*)", "паллет"),
        (r"(бухт(?:а|ы)?|бухт\w*)", "бухта"),
    ]
    for pattern, canonical in unit_aliases:
        if re.fullmatch(pattern, low, re.I):
            return canonical
    return None


def looks_like_unit_cell(text: str) -> bool:
    return normalize_unit_label(text) is not None


def looks_like_code_cell(text: str) -> bool:
    low = normalize_spaces(text).lower()
    if not low:
        return False
    if re.fullmatch(r"(окпд2?|окпд|код|код\s+окпд2?)", low):
        return True
    if re.fullmatch(r"\d+(?:\.\d+){1,}", low):
        return True
    if re.match(r"^\d+(?:\.\d+){2,}\s+\S+", low):
        return True
    return False


def extract_embedded_indexed_name(text: str) -> str:
    cleaned = normalize_spaces(text)
    stripped = re.sub(r"^\s*(?:№\s*)?\d+[.)]?\s*", "", cleaned)
    stripped = normalize_material_name(stripped)
    if stripped == cleaned or not stripped:
        return ""
    if looks_like_material_name(stripped):
        return stripped
    low = stripped.lower()
    if re.search(r"\b(материал|товар|продукц)\b", low) and 1 < len(low.split()) <= 8 and not looks_like_non_product_line(stripped):
        return stripped
    return ""


def detect_quantity_and_unit_from_cells(cells: List[str]) -> Tuple[Optional[str], Optional[str]]:
    normalized = [normalize_spaces(cell) for cell in cells if normalize_spaces(cell)]

    for idx in range(len(normalized) - 1, -1, -1):
        cell = normalized[idx]
        if re.fullmatch(r"\d+(?:[.,]\d+)?", cell):
            prev = normalized[idx - 1] if idx > 0 else ""
            if prev and looks_like_unit_cell(prev):
                return cell, normalize_unit_label(prev)
            next_cell = normalized[idx + 1] if idx + 1 < len(normalized) else ""
            if next_cell and looks_like_unit_cell(next_cell):
                return cell, normalize_unit_label(next_cell)

    for idx in range(len(normalized) - 1, -1, -1):
        cell = normalized[idx]
        if looks_like_unit_cell(cell):
            continue
        qty, unit = detect_quantity_and_unit(cell)
        if qty:
            return qty, unit

    non_unit_cells = [cell for cell in normalized if not looks_like_unit_cell(cell)]
    return detect_quantity_and_unit(" ".join(non_unit_cells or normalized))


def looks_like_non_product_line(text: str) -> bool:
    low = normalize_spaces(text).lower()
    if not low:
        return False
    if "://" in low or low.startswith("www."):
        return True
    if re.match(r"^\s*(подходит|предназначен(?:а|о)?|предназначены)\s+для\b", low):
        return True
    if contains_any(low, NON_PRODUCT_LINE_PATTERNS):
        return True
    if low.startswith("=== лист:"):
        return True
    if low.startswith("информация о проводимом аукционе"):
        return True
    if re.match(r"^\s*(прям(ое|ой)\s+(разрешение|запрет)\s+эквивалента|возможность\s+эквивалента\s+по\s+смыслу)\b", low):
        return True
    if "/" in text and text == text.upper() and len(low.split()) >= 2:
        return True
    if low.startswith("техническое задание"):
        return True
    if low.startswith("на поставку "):
        return True
    if re.match(r'^\s*(?:\d+(?:\.\d+)*[.)]?\s*)?"?гост\b', low):
        return True
    if re.search(r"\b(межгосударственн\w+\s+стандарт|национальн\w+\s+стандарт|техническ\w+\s+услови)\b", low):
        return True
    if low.startswith("соответствует требованиям гост"):
        return True
    if re.match(r"^\s*(группа\s+\w+|воспламеняемость|распространение\s+пламени)\b", low):
        return True
    if "товары/товары" in low or "классификатора предметов" in low:
        return True
    if low.startswith("поставка ") and "|" not in text:
        qty, _ = detect_quantity_and_unit(low)
        if not qty:
            return True
    if len(low.split()) > 12 and ("," in low or ";" in low) and not detect_quantity_and_unit(low)[0]:
        return True
    if len(low) > 180 and re.search(r"\b(товар|материал|продукц)\b", low) and not has_material_hint(low):
        qty, _ = detect_quantity_and_unit(low)
        if not qty:
            return True
    return False


def looks_like_characteristic_cell(text: str) -> bool:
    low = normalize_spaces(text).lower()
    low = re.sub(r"^\d+(?:\.\d+)+[.)]?\s*", "", low)
    if not low:
        return False
    if low.startswith("[") and low.endswith("]"):
        return True
    if re.match(r"^материал[а-я]*:", low):
        return True
    if re.fullmatch(r"(мпа|кн/м|°с|гр\s*с|%|кг/м²|кг/м2|л/кг|мм|м|м2|м3|шт|кг|л)", low, re.I):
        return True
    if re.match(r'^\s*(?:\d+(?:\.\d+)*[.)]?\s*)?"?гост\b', low):
        return True
    if re.search(r"\bне\s+(менее|более|выше|ниже)\b", low):
        return True
    if re.match(
        r"^(тип\s+структуры\s+полотна|вид\s+защитного\s+слоя|вид\s+основн\w+\s+компонент\w+|"
        r"количеств\w+\s+слоев|основа\s+перфорированная|стойкость\s+к\s+ультрафиолетовому\s+излучению|"
        r"химическая\s+стойкость|материал\s+поверхности|тип\s+лкм|свойства|консистенция|"
        r"количество\s+компонентов|объект\w+\s+применения|клеящаяся\s+сторона|вид\s+тары|"
        r"компонентность|форма\s+выпуска|маркировка/спецификация|массовая\s+доля|"
        r"условная\s+вязкость|разрывное\s+усилие|водопоглощение|площадь\s+рулона|"
        r"предмет\s+договора|срок\s+поставки|место\s+поставки)\b",
        low,
    ):
        return True
    if re.fullmatch(
        r"(материал|марка|вид|фасовка|упаковка|цвет|длина|длина\s+рулона|"
        r"ширина|толщина|объем|объём|влагостойкость|термостойкость|"
        r"клапан|вид\s+сечения|класс\s+огнестойкости|работоспособность,\s*в\s*диапазоне|"
        r"материал\s+назначения|материал\s+поверхности\s+применения|основной\s+материал|"
        r"верхнее\s+покрытие|форма\s+выпуска|предназначен.*)",
        low,
    ):
        return True
    if re.match(
        r"^(тип|тип\s+товара|цвет|назначение|применение|состав|основа|покрытие|"
        r"материал\s+(?:основания|основы|поверхности)|место\s+использования|"
        r"вид|"
        r"метод\s+нанесения|способ\s+нанесения|тип\s+поверхности|тип\s+работ|"
        r"упаковка|тип\s+упаковки|вид\s+упаковки|форма\s+выпуска|технология\s+применения|"
        r"объем|вес|масса|ширина|длина|толщина|плотность|температура|время|"
        r"водостойкий|морозостойкий|под\s+покраску|однокомпонентн\w*|нетвердеющ\w*|"
        r"группа\s+\w+|воспламеняемость|распространение\s+пламени)\b",
        low,
    ):
        return True
    if re.search(
        r"\b(прочност\w*|водопоглощен\w*|водопроницаем\w*|теплостойк\w*|адгези\w*|"
        r"амплитуд\w*|текучест\w*|массов\w+\s+дол\w*|количеств\w*|"
        r"соответствует\s+требованиям|работоспособност\w*|"
        r"масса\w*|толщин\w*|ширин\w*|длин\w*|разрывн\w+\s+сил\w*|верхняя\s+сторона|"
        r"нижняя\s+сторона|материал\s+основы|доля\s+вторичн\w+\s+сырья|"
        r"температур\w*|давлен\w*|размер\w*|диаметр\w*|класс\w*|марка\w*)\b",
        low,
    ):
        return True
    if any(k in low for k in SPEC_KEYWORDS):
        return True
    return False


def looks_like_generic_position_label(text: str) -> bool:
    low = normalize_spaces(text).lower()
    if not low or looks_like_non_product_line(text) or looks_like_characteristic_cell(text):
        return False
    if "/" in low and low.upper() == normalize_spaces(text):
        return False
    return bool(
        re.search(r"\b(материал|товар|продукц)\b", low)
        and 2 <= len(low.split()) <= 6
    )


def looks_like_goods_relevant_line(text: str) -> bool:
    low = normalize_spaces(text).lower()
    if not low:
        return False
    if has_material_hint(low):
        return True
    if contains_any(text, EXACT_NAME_HINTS):
        return True
    if re.search(r"\b(гост|ту)\b", low):
        return True
    if low.count("|") >= 2 and re.search(r"\b(наименование|характеристик|ед\.?\s*изм|количеств|размер)\b", low):
        return True
    qty, _ = detect_quantity_and_unit(low)
    if qty and ("|" in text or has_material_hint(low)):
        return True
    if any(h in low for h in GENERAL_REQUIREMENT_HINTS):
        return True
    return False


def looks_like_goods_position_line(text: str) -> bool:
    low = normalize_spaces(text).lower()
    if not low:
        return False
    if looks_like_non_product_line(text):
        return False
    qty, _ = detect_quantity_and_unit(low)
    if has_material_hint(low):
        if "|" in text or qty or contains_any(text, EXACT_NAME_HINTS):
            return True
        if (
            len(low.split()) <= 8
            and len(low) <= 120
            and not re.search(
                r"\b(требовани|стандарт|услови|регламент|безопасност|"
                r"классификатор|поставка\s+материалов|проект|контракт)\b",
                low,
            )
        ):
            return True
    if low.count("|") >= 2 and re.search(r"\b(наименование|характеристик|ед\.?\s*изм|количеств|размер)\b", low):
        return True
    if contains_any(text, EXACT_NAME_HINTS):
        if qty or "|" in text:
            return True
    return False


def block_has_goods_position_signals(text: str) -> bool:
    lines = split_lines(text)
    if not lines:
        return False

    for line in lines:
        if looks_like_goods_position_line(line):
            return True
        if "|" in line and re.search(r"\b(ед\.?\s*изм|кол-?во|количеств|объем)\b", line, re.I):
            return True

    return False


def looks_like_service_row(text: str) -> bool:
    low = normalize_spaces(text).lower()
    if not low:
        return True
    if low.count("|") >= 2:
        cells = [clean_cell(part) for part in text.split("|")]
        nonempty = [cell for cell in cells if cell]
        if nonempty:
            qty, _ = detect_quantity_and_unit_from_cells(nonempty)
            if re.fullmatch(r"\d+(?:[.,]\d+)?[.)]?", nonempty[0]) and qty:
                return False
            if looks_like_characteristic_cell(nonempty[0]) and len(nonempty) <= 4:
                return False
    if contains_any(low, SERVICE_ROW_PATTERNS):
        return True
    if low.count("|") >= 2 and "инструкция по заполнению" in low:
        return True
    return False


def looks_like_table_header_row(text: str) -> bool:
    low = normalize_spaces(text).lower()
    if "|" not in text:
        return False
    cells = [clean_cell(part) for part in text.split("|")]
    nonempty = [cell for cell in cells if cell]
    if nonempty and re.fullmatch(r"\d+(?:[.,]\d+)?[.)]?", nonempty[0]):
        qty, _ = detect_quantity_and_unit_from_cells(nonempty)
        if qty:
            return False
    if re.match(r"^\s*№\s*п/?п\b", low):
        return True

    header_hits = 0
    for pattern in [
        r"\bнаименование\b",
        r"\bхарактеристик\w*\b",
        r"\bпоказател\w*\b",
        r"\bед(?:\.|\s|иниц)\w*\s*измерени\w*\b",
        r"\bкол-?во\b|\bколичеств\w*\b",
        r"\bзначение\s+характеристики\b",
        r"\bинструкц\w+\s+по\s+заполнению\b",
        r"\bокпд\b",
    ]:
        if re.search(pattern, low):
            header_hits += 1

    if header_hits >= 2:
        return True

    if "инструкция по заполнению" in low and "характерист" in low:
        return True

    return False


def _slice_block_to_relevant_window(block: str) -> str:
    block_lines = split_lines(block)
    if not block_lines:
        return ""

    start_idx = None
    end_idx = None
    last_relevant_idx = None
    first_position_idx = None

    for idx, line in enumerate(block_lines):
        if looks_like_tz_section_start(line.lower()):
            start_idx = idx
            break

    if start_idx is None:
        return "\n".join(block_lines).strip()

    for idx in range(start_idx + 1, len(block_lines)):
        if looks_like_tz_section_end(block_lines[idx].lower()):
            end_idx = idx
            break

    for idx in range(start_idx, len(block_lines)):
        if looks_like_goods_relevant_line(block_lines[idx]):
            last_relevant_idx = idx
        if first_position_idx is None and looks_like_goods_position_line(block_lines[idx]):
            first_position_idx = idx

    window_start = max(0, start_idx - 2)
    if first_position_idx is not None:
        window_start = max(start_idx, first_position_idx - 4)
    window_end = end_idx + 2 if end_idx is not None else len(block_lines)
    if last_relevant_idx is not None:
        window_end = max(window_end, last_relevant_idx + 2)
    window_end = min(len(block_lines), window_end)
    return "\n".join(block_lines[window_start:window_end]).strip()


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
            clipped = _slice_block_to_relevant_window(block)
            if clipped and (
                not looks_like_excluded_section(clipped.lower())
                or block_has_goods_position_signals(clipped)
            ):
                selected_blocks.append(clipped)
            if looks_like_tz_section_end(low):
                in_tz = False
            continue
        if in_tz and not looks_like_excluded_section(low):
            clipped = _slice_block_to_relevant_window(block)
            if clipped and (
                not looks_like_excluded_section(clipped.lower())
                or block_has_goods_position_signals(clipped)
            ):
                selected_blocks.append(clipped)
        if in_tz and looks_like_tz_section_end(low):
            in_tz = False

    if selected_blocks:
        unique_blocks = dedupe_preserve_order(selected_blocks)
        return "\n\n".join(unique_blocks), {"mode": "explicit_section", "blocks": len(unique_blocks)}

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
        if line.count("|") >= 2:
            continue
        if re.search(
            r"\b(участник\w*\s+закупки|в\s+заявке|значени\w+\s+показател\w+|"
            r"инструкц\w+|символ|обозначени\w+|крайн\w+\s+значени\w+|"
            r"товарн\w+\s+знак\w+.*или\s+эквивалент)\b",
            low,
        ):
            continue
        if any(h in low for h in GENERAL_REQUIREMENT_HINTS):
            found.append(line)
            continue
        if re.search(r"\b(гарант|упаковк|маркировк|место\s+поставк|сроки?\s+поставк|доля\s+вторичн\w+\s+сырья)\b", low):
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
    low = normalize_spaces(text).lower().replace(",", ".")
    paren = re.search(r"(\d+(?:[ .]\d+)?(?:[.,]\d+)?)\s*\(([^)]+)\)", low, re.I)
    if paren:
        unit = normalize_unit_label(paren.group(2))
        if unit:
            return paren.group(1).replace(" ", ""), unit
    patterns = [
        r"(\d+(?:[ .]\d+)?(?:[.,]\d+)?)\s*(м2|м3|м|мм|кг|т|л|шт|упак(?:овка)?|меш(?:ок)?|рулон(?:ов)?|ведро|канистр(?:а|ы)?|лист(?:ов)?|компл(?:ект)?|паллет(?:а|ы)?|бухт(?:а|ы)?)\b",
        r"(не менее\s+\d+(?:[.,]\d+)?)\s*(м2|м3|м|мм|кг|т|л|шт)\b",
    ]
    for pat in patterns:
        m = re.search(pat, low, re.I)
        if m:
            return m.group(1).replace(" ", ""), normalize_unit_label(m.group(2)) or m.group(2)
    return None, None


def infer_unit_only_row(cells: List[str]) -> Optional[str]:
    nonempty = [normalize_spaces(cell) for cell in cells if normalize_spaces(cell)]
    if len(nonempty) != 2:
        return None
    if not looks_like_material_name(nonempty[0]):
        return None
    return normalize_unit_label(nonempty[1])


def looks_like_procurement_instruction_line(text: str) -> bool:
    low = normalize_spaces(text).lower()
    if not low:
        return False
    return bool(
        re.search(
            r"\b(участник\w*\s+закупки|в\s+заявке|значени\w+\s+показател\w+|"
            r"инструкц\w+|символ|обозначени\w+|товарн\w+\s+знак\w+|"
            r"следует\s+предоставить|необходимо\s+указывать)\b",
            low,
        )
    )


def sanitize_characteristic_text(text: str) -> str:
    cleaned = normalize_spaces(text)
    cleaned = re.sub(r"https?://\S+", "", cleaned, flags=re.I)
    cleaned = re.sub(r"\b[ОO][КK]ПД2?\s*:\s*[\d.\-]+", "", cleaned, flags=re.I)
    cleaned = re.sub(
        r"\s*:\s*(?:шт|м2|м3|м|мм|кг|л|рул(?:он)?|рулон|упаковка|мешок|ведро|канистра)\s+\d+(?:[.,]\d+)?\s*$",
        "",
        cleaned,
        flags=re.I,
    )
    cleaned = re.sub(
        r"\s*:\s*(?:шт|м2|м3|м|мм|кг|л|рул(?:он)?|рулон|упаковка|мешок|ведро|канистра)\s*$",
        "",
        cleaned,
        flags=re.I,
    )
    cleaned = normalize_spaces(cleaned)
    low = cleaned.lower()
    if not cleaned:
        return ""
    if (
        ":" not in cleaned
        and re.search(r"\b(город|улиц|дом|корпус|контракта\s+по|даты\s+заключения)\b", low)
    ):
        return ""
    if re.fullmatch(r"(объем\s*\(единица|объем\s*\(единиц\w*|измерения\)?|характеристики|адрес|срок)", low):
        return ""
    if ":" not in cleaned:
        return cleaned

    name, value = cleaned.split(":", 1)
    value = normalize_spaces(value)
    value = re.sub(r"\s+(город|улиц|дом|корпус)\b.*$", "", value, flags=re.I)
    value = re.sub(r"\s+фёдора\s+полетаева.*$", "", value, flags=re.I)
    value = re.sub(r"\s+(контракта\s+по|c\s+даты\s+заключения|с\s+даты\s+заключения|c\s+\d{2}\.\d{2}\.\d{4}|с\s+\d{2}\.\d{2}\.\d{4}).*$", "", value, flags=re.I)
    value = re.sub(
        r"\s+\d+(?:[.,]\d+)?\s*\((?:квадратн\w+\s+метр\w*|килограмм\w*|метр\w*|литр\w*|штук\w*|штука)\)\s*$",
        "",
        value,
        flags=re.I,
    )
    value = value.strip(" .;,")
    if not value:
        return ""
    return f"{name.strip()}: {value}"


def parse_characteristic_piece(piece: str) -> Optional[str]:
    piece = normalize_spaces(piece).strip(" .;,")
    if not piece:
        return None
    if re.fullmatch(r"(шт|м2|м3|м|мм|кг|л|рул(?:он)?|рулон|упаковка|мешок|ведро|канистра)", piece, re.I):
        return None
    if re.match(r"^(?:\d+(?:[.,]\d+)?\s*м2\s*)?[ОO][КK]ПД2?\b", piece, re.I):
        return None

    piece = re.sub(r"^по\s+способу\s+монтажа\s+(.+)$", r"Способ монтажа: \1", piece, flags=re.I)
    piece = re.sub(r"^по\s+назначению\s+(.+)$", r"Назначение: \1", piece, flags=re.I)
    piece = re.sub(
        r"\s*:\s*(?:шт|м2|м3|м|мм|кг|л|рул(?:он)?|рулон)\s+\d+(?:[.,]\d+)?\s*$",
        "",
        piece,
        flags=re.I,
    )
    piece = re.sub(r"\s*:\s*(?:шт|м2|м3|м|мм|кг|л|рул(?:он)?|рулон)\s*$", "", piece, flags=re.I)
    piece = normalize_spaces(piece).strip(" .;,")
    if not piece:
        return None

    area_match = re.fullmatch(r"(\d+(?:[.,]\d+)?)\s*м2", piece, re.I)
    if area_match:
        return f"Площадь рулона, м2: {area_match.group(1)}"

    if ":" in piece:
        name, value = piece.split(":", 1)
    else:
        match = re.match(rf"^(?P<name>{CHARACTERISTIC_LABEL_RE})\s+(?P<value>.+)$", piece, re.I)
        if match:
            name = match.group("name")
            value = match.group("value")
        else:
            match = re.match(
                r"^(?P<name>.+?),\s*(?P<value>(?:не\s+менее|не\s+более|от\s+|до\s+|в\s+пределах\b|\d).+)$",
                piece,
                re.I,
            )
            if match:
                name = match.group("name")
                value = match.group("value")
            else:
                if len(piece) <= 160 and re.search(
                    r"\b(не\s+менее|не\s+более|мм|м2|м3|кг|л|сталь|битум|полиэстер|"
                    r"коричнев|белый|гидроизоляц|стеклоткан|гост|ту)\b",
                    piece,
                    re.I,
                ):
                    return piece
                return None

    name = normalize_spaces(name).strip(" .;,")
    value = normalize_spaces(value).strip(" .;,")
    if not value:
        return None
    return f"{name}: {value}" if name else value


def split_characteristic_blob(text: str) -> List[str]:
    text = normalize_spaces(text)
    if not text:
        return []

    extracted_parts: List[str] = []
    text = re.sub(r"https?://\S+", "", text, flags=re.I)
    text = text.replace("мм. вод.ст.", "мм вод ст")
    text = text.replace("час.", "час")

    for match in re.finditer(r"(?<!\d)(\d+(?:[.,]\d+)?)\s*м2(?=\s*[ОO][КK]ПД2?\b)", text, re.I):
        extracted_parts.append(f"Площадь рулона, м2: {match.group(1)}")

    text = re.sub(r"(?<!\d)\d+(?:[.,]\d+)?\s*м2(?=\s*[ОO][КK]ПД2?\b)", "", text, flags=re.I)
    text = re.sub(r"\b[ОO][КK]ПД2?\s*:\s*[\d.\-]+", "", text, flags=re.I)
    text = re.sub(r"^\s*рул(?:он)?\.?\s*\d+(?:[.,]\d+)?\s*м2\s*", "", text, flags=re.I)
    text = re.sub(r"^\s*рул(?:он)?\.?\s*\d+(?:[.,]\d+)?\s*м\b", "", text, flags=re.I)
    text = re.sub(r"^\s*\d+(?:[.,]\d+)?\s*м2\s*", "", text, flags=re.I)
    text = re.sub(
        r"\s*:\s*(?:шт|м2|м3|м|мм|кг|л|рул(?:он)?|рулон)\s+\d+(?:[.,]\d+)?\s*$",
        "",
        text,
        flags=re.I,
    )
    text = normalize_spaces(text)

    for pattern in CHARACTERISTIC_MARKER_PATTERNS:
        text = re.sub(rf"(?<!^)(?={pattern})", "||", text, flags=re.I)

    for pattern in CHARACTERISTIC_SENTENCE_MARKERS:
        text = re.sub(rf"(?<!^)(?={pattern})", "||", text, flags=re.I)

    pieces: List[str] = []
    for chunk in text.split("||"):
        for sentence in re.split(r"(?<!\d)[.;](?!\d)", chunk):
            parsed = parse_characteristic_piece(sentence)
            if parsed:
                pieces.append(parsed)

    return dedupe_preserve_order(extracted_parts + pieces)


def enrich_candidate_name(candidate_name: str, cells: List[str]) -> str:
    base_name = normalize_material_name(candidate_name)
    if not base_name:
        return base_name

    if not (looks_like_generic_position_label(base_name) or len(base_name.split()) <= 2):
        return base_name

    best_name = base_name
    base_low = base_name.lower()

    for cell in cells:
        cell = normalize_spaces(cell)
        if not cell or cell == candidate_name:
            continue
        if looks_like_unit_cell(cell) or looks_like_code_cell(cell):
            continue
        if re.fullmatch(r"\d+(?:[.,]\d+)?", cell):
            continue

        detailed_name = normalize_material_name(extract_name_from_mixed_spec_cell(cell))
        if not detailed_name or detailed_name.lower() == base_low:
            continue
        if looks_like_non_product_line(detailed_name):
            continue

        detailed_low = detailed_name.lower()
        if (
            detailed_low.startswith(base_low + " ")
            or base_low in detailed_low
            or has_material_hint(detailed_low)
        ) and len(detailed_name) > len(best_name):
            best_name = detailed_name

    return best_name


def looks_like_instruction_cell(text: str) -> bool:
    low = normalize_spaces(text).lower()
    if not low:
        return False
    return bool(
        re.search(
            r"\b(значение\s+характеристики|инструкция\s+по\s+заполнению|"
            r"участник\s+закупки|не\s+может\s+изменяться|указывает\s+в\s+заявке)\b",
            low,
        )
    )


def looks_like_eis_card_metadata_row(text: str) -> bool:
    low = normalize_spaces(text).lower()
    if "|" not in text or not low.startswith("идентификатор:"):
        return False
    if "товар" not in low:
        return False
    return any(looks_like_unit_cell(part) for part in text.split("|"))


def extract_numeric_token(text: str) -> Optional[str]:
    cleaned = normalize_spaces(text).replace(" ", "")
    if re.fullmatch(r"\d+(?:[.,]\d+)?", cleaned):
        return cleaned
    return None


def extract_eis_card_quantity_and_unit(cells: List[str]) -> Tuple[Optional[str], Optional[str]]:
    nonempty = [normalize_spaces(cell) for cell in cells if normalize_spaces(cell)]
    if not nonempty:
        return None, None

    unit = None
    for idx, cell in enumerate(nonempty):
        if normalize_spaces(cell).lower() in {"товар", "работа", "услуга"} and idx + 1 < len(nonempty):
            unit = normalize_unit_label(nonempty[idx + 1])
            if unit:
                break
    if not unit:
        for cell in nonempty:
            unit = normalize_unit_label(cell)
            if unit:
                break

    numeric_cells = [token for token in (extract_numeric_token(cell) for cell in nonempty) if token]
    if len(numeric_cells) >= 3:
        quantity = numeric_cells[-2]
    elif numeric_cells:
        quantity = numeric_cells[-1]
    else:
        quantity = None

    return quantity, unit


def characteristic_from_eis_row(cells: List[str]) -> Optional[str]:
    nonempty = [normalize_spaces(cell) for cell in cells if normalize_spaces(cell)]
    if len(nonempty) < 2:
        return None

    name = nonempty[0]
    value = nonempty[1]
    if looks_like_instruction_cell(name) or looks_like_instruction_cell(value):
        return None

    extras = []
    for extra in nonempty[2:]:
        if looks_like_instruction_cell(extra):
            continue
        if len(extra) > 120 and not normalize_unit_label(extra):
            continue
        extras.append(extra)

    if extras:
        value = f"{value} {' '.join(extras)}".strip()

    if not name or not value:
        return None

    return f"{name}: {value}"


def suppress_embedded_dimension_quantity(
    candidate_name: str,
    qty: Optional[str],
    unit: Optional[str],
    cells: List[str],
) -> Tuple[Optional[str], Optional[str]]:
    if not candidate_name or not qty or not unit:
        return qty, unit

    name_qty, name_unit = detect_quantity_and_unit(candidate_name)
    if name_qty != qty or name_unit != unit:
        return qty, unit

    normalized_cells = [normalize_spaces(cell) for cell in cells if normalize_spaces(cell)]
    numeric_cells = [
        normalize_spaces(cell).replace(" ", "")
        for cell in normalized_cells
        if re.fullmatch(r"\d+(?:[.,]\d+)?", normalize_spaces(cell).replace(" ", ""))
    ]
    standalone_numeric = bool(numeric_cells)
    if (
        standalone_numeric
        and len(numeric_cells) == 1
        and normalized_cells
        and re.fullmatch(r"\d+[.)]?", normalized_cells[0])
        and normalized_cells[0].rstrip(".)") == numeric_cells[0]
    ):
        standalone_numeric = False
    dedicated_units = [
        normalize_unit_label(cell)
        for cell in normalized_cells
        if normalize_unit_label(cell)
    ]

    if not standalone_numeric:
        alternate_units = [declared_unit for declared_unit in dedicated_units if declared_unit and declared_unit != unit]
        if alternate_units:
            return None, alternate_units[-1]
        if unit in {"мм", "м", "м2", "м3"} and len(normalized_cells) >= 3:
            return None, None

    return qty, unit


def continuation_row_can_define_quantity(cells: List[str]) -> bool:
    if not cells:
        return False
    first = normalize_spaces(cells[0]).lower()
    if re.search(r"\b(кол-?во|количество|объем|объе?м\s+поставк|объем\s+закупк)\b", first):
        return True
    return False


def looks_like_inline_context_header_line(text: str) -> bool:
    low = normalize_spaces(text).lower()
    if not low or "|" in text:
        return False
    return bool(
        re.search(
            r"\b(характеристик\w*|объем\s*\(.*измерени\w*|единиц\w+\s+измерени\w*|"
            r"адрес|срок|измерени\w*\)?|объем\s*\(единица)\b",
            low,
        )
    )


def extract_characteristics_from_text(text: str) -> List[str]:
    lines = split_lines(text)
    chars = []
    for line in lines:
        low = line.lower()
        if re.search(r"\b(гост|ту)\b", low):
            chars.append(line)
            continue
        if any(k in low for k in SPEC_KEYWORDS):
            chars.append(line)
            continue
        if re.search(r"\bне\s+(менее|более|выше|ниже)\b", low):
            chars.append(line)
            continue
        if re.search(r"\b\d+(?:[.,]\d+)?\s*(мм|м|м2|м3|кг|г/м2|кг/м2|н/50\s*мм|°с|с0|в|квт|мпа)\b", low):
            chars.append(line)
    return dedupe_preserve_order(chars)


def extract_name_from_mixed_spec_cell(text: str) -> str:
    cleaned = normalize_spaces(text)
    if not cleaned or looks_like_non_product_line(cleaned):
        return ""

    candidate = re.sub(r"\bОКПД2?\b.*$", "", cleaned, flags=re.I)
    candidate = re.sub(
        r"\s*/\s*(?:рул(?:он)?\.?|рулон|шт|компл(?:ект)?|упак(?:овка)?)\b.*$",
        "",
        candidate,
        flags=re.I,
    )
    candidate = re.split(
        r"(?i)\b("
        r"посыпк|основа|по\s+способу|толщин|ширин|длин|высот|цвет|"
        r"покрыти|тип|назначени|размер|вид|диаметр|профиль|водонепроницаем|"
        r"водоупорност|количеств\w+\s+слоев|площадь\s+рулона|маркировк"
        r")\b",
        candidate,
        maxsplit=1,
    )[0]
    candidate = normalize_material_name(candidate)

    if not candidate or ":" in candidate or "://" in candidate:
        return ""
    if len(candidate.split()) > 10:
        return ""
    if len(candidate) < 3:
        return ""
    return candidate


def extract_inline_characteristics_from_name_cell(text: str, candidate_name: str) -> List[str]:
    cleaned = normalize_spaces(text)
    if not cleaned or not candidate_name:
        return []

    remainder = cleaned
    if cleaned.lower().startswith(candidate_name.lower()):
        remainder = cleaned[len(candidate_name):]

    remainder = re.sub(r"^\s*[/,;:-]+\s*", "", remainder)
    remainder = re.sub(
        r"\bОКПД2?\b.*?(?=(?:[А-ЯЁA-Z][а-яёa-z-]{2,}\s*:)|$)",
        "",
        remainder,
        flags=re.I,
    )
    remainder = normalize_spaces(remainder)
    if not remainder:
        return []

    chars = extract_characteristics_from_text(remainder)
    if chars:
        return chars

    if (
        10 <= len(remainder) <= 220
        and (
            any(k in remainder.lower() for k in SPEC_KEYWORDS)
            or re.search(r"\b\d+(?:[.,]\d+)?\s*(мм|м|м2|м3|кг|л|шт|рул)\b", remainder, re.I)
        )
    ):
        sanitized = sanitize_characteristic_text(remainder)
        return [sanitized] if sanitized else []

    return []


def normalize_material_name(name: str) -> str:
    name = normalize_spaces(name)
    name = re.sub(r"^[0-9]+[.)]?\s*", "", name)
    name = re.sub(r"^(?:\d+[.)]?\s*)?(?:\d+(?:\.\d+){1,}\s*)", "", name)
    name = re.sub(r"\b(ед\.?\s*изм\.?|кол-?во|количество|цена|стоимость|окпд2|код енс)\b.*$", "", name, flags=re.I)
    name = re.sub(r"\bили\s+эквив\w+\b", "", name, flags=re.I)
    name = re.sub(r"\bэквив\w+\b", "", name, flags=re.I)
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
    if "://" in low or low.startswith("www."):
        return False
    if looks_like_service_row(text):
        return False
    if looks_like_non_product_line(text):
        return False
    if looks_like_characteristic_cell(text):
        return False
    if len(low.split()) > 12 and not detect_quantity_and_unit(text)[0]:
        return False
    if has_material_hint(low):
        return True
    if contains_any(text, EXACT_NAME_HINTS):
        return True
    qty, _ = detect_quantity_and_unit(text)
    if re.search(r"\bматериал\b|\bтовар\b|\bпродукц", low) and (qty or ("|" in text and len(low.split()) <= 8)):
        return True
    if (
        re.search(r"\b(материал|товар|продукц)\b", low)
        and 2 <= len(low.split()) <= 6
        and not looks_like_non_product_line(text)
        and not looks_like_service_row(text)
    ):
        return True
    return False


def extract_row_characteristics(
    nonempty: List[str],
    raw: str,
    candidate_name: str = "",
    unit: Optional[str] = None,
) -> List[str]:
    working = [normalize_spaces(cell) for cell in nonempty if normalize_spaces(cell)]
    filtered = []
    candidate_consumed = False
    for cell in working:
        if candidate_name and not candidate_consumed and cell == candidate_name:
            candidate_consumed = True
            continue
        if re.fullmatch(r"\d+[.)]?", cell):
            continue
        if looks_like_code_cell(cell):
            continue
        filtered.append(cell)

    if len(filtered) >= 3 and (
        "эквив" in filtered[0].lower()
        or ("или " in filtered[0].lower() and looks_like_material_name(filtered[0]))
    ):
        filtered = filtered[1:]

    chars: List[str] = []
    structured_added = False
    if len(filtered) >= 2 and looks_like_characteristic_cell(filtered[0]):
        noisy_tail = any(
            detect_quantity_and_unit(cell)[0]
            or re.search(r"\b(город|улиц|дом|корпус|срок|даты\s+заключения|контракта)\b", cell.lower())
            for cell in filtered[1:]
        )
        if ":" in filtered[0] and noisy_tail:
            chars.append(filtered[0])
            structured_added = True
        else:
            value_parts = [
                cell for cell in filtered[1:]
                if cell not in {"-", "—"}
                and not looks_like_instruction_cell(cell)
            ]
            if value_parts:
                chars.append(f"{filtered[0]}: {' '.join(value_parts)}")
                structured_added = True

    if "|" not in raw:
        chars.extend(extract_characteristics_from_text(raw))

    if not structured_added:
        chars.extend(
            cell for cell in filtered
            if not re.fullmatch(r"\d+(?:[.,]\d+)?", cell)
            and not looks_like_service_row(cell)
            and not looks_like_non_product_line(cell)
            and (not unit or normalize_spaces(cell).lower() != unit)
            and looks_like_characteristic_cell(cell)
            and not any(cell == ch or ch.startswith(f"{cell}:") for ch in chars)
        )

    sanitized = []
    for char in chars:
        clean_char = sanitize_characteristic_text(char)
        if clean_char:
            sanitized.append(clean_char)
    return dedupe_preserve_order(sanitized)


def pick_best_candidate_name(cells: List[str]) -> str:
    best_name = ""
    best_score = 0.0

    for idx, cell in enumerate(cells):
        if looks_like_service_row(cell) or looks_like_non_product_line(cell) or looks_like_code_cell(cell):
            continue
        if looks_like_characteristic_cell(cell) and not looks_like_material_name(cell):
            continue
        if not looks_like_material_name(cell):
            continue

        score = 0.0
        if has_material_hint(cell):
            score += 2.0
        if contains_any(cell, EXACT_NAME_HINTS):
            score += 4.0
        if idx > 0:
            score += 1.0
        if idx + 1 < len(cells) and looks_like_characteristic_cell(cells[idx + 1]):
            score += 3.0
        if re.search(r"\d", cell) and re.search(r"[а-яa-z]", cell, re.I):
            score += 1.0
        if looks_like_generic_position_label(cell):
            score -= 1.5
        score += min(len(cell), 80) / 80.0

        if score > best_score:
            best_name = cell
            best_score = score

    return best_name


def parse_parametric_rows(text: str) -> List[RequirementItem]:
    items: List[RequirementItem] = []
    current_item: Optional[RequirementItem] = None

    rows = []
    for line in split_lines(text):
        if "|" not in line:
            continue
        parts = [clean_cell(part) for part in line.split("|")]
        nonempty = [part for part in parts if part]
        if nonempty:
            rows.append((parts, nonempty))

    for parts, nonempty in rows:
        raw = " | ".join(nonempty)
        low = raw.lower()

        if looks_like_table_header_row(raw):
            continue

        if looks_like_service_row(raw):
            if not looks_like_table_header_row(raw):
                current_item = None
            continue

        joined = " ".join(nonempty)
        qty, unit = detect_quantity_and_unit_from_cells(nonempty)
        if not unit:
            unit = infer_unit_only_row(nonempty)
        attribute_row = False
        candidate_name = ""
        mixed_name_chars: List[str] = []

        if len(nonempty) >= 2 and looks_like_characteristic_cell(nonempty[0]) and not looks_like_material_name(nonempty[0]):
            mixed_first_name = extract_name_from_mixed_spec_cell(nonempty[0]) if qty else ""
            if mixed_first_name:
                candidate_name = mixed_first_name
                mixed_name_chars.extend(extract_inline_characteristics_from_name_cell(nonempty[0], mixed_first_name))
            else:
                attribute_row = True
        elif len(nonempty) >= 3 and re.fullmatch(r"\d+[.)]?", nonempty[0]) and looks_like_code_cell(nonempty[1]) and looks_like_material_name(nonempty[2]):
            candidate_name = nonempty[2]
        elif len(nonempty) >= 2 and re.fullmatch(r"\d+(?:[.,]\d+)?[.)]?", nonempty[0]):
            second_cell_name = extract_name_from_mixed_spec_cell(nonempty[1]) or (
                nonempty[1]
                if not looks_like_characteristic_cell(nonempty[1]) and not looks_like_unit_cell(nonempty[1])
                else ""
            )
            if second_cell_name:
                candidate_name = second_cell_name
                if second_cell_name != nonempty[1]:
                    mixed_name_chars.extend(extract_inline_characteristics_from_name_cell(nonempty[1], second_cell_name))
        elif len(nonempty) >= 2 and looks_like_code_cell(nonempty[0]) and looks_like_material_name(nonempty[1]):
            candidate_name = nonempty[1]
        elif qty and len(nonempty) >= 3 and not looks_like_unit_cell(nonempty[0]):
            mixed_first_name = extract_name_from_mixed_spec_cell(nonempty[0])
            if not mixed_first_name and not looks_like_characteristic_cell(nonempty[0]):
                mixed_first_name = (
                    nonempty[0]
                    if len(nonempty[0].split()) <= 8 and len(nonempty[0]) <= 120 and not looks_like_non_product_line(nonempty[0])
                    else ""
                )
            if mixed_first_name:
                candidate_name = mixed_first_name
                if mixed_first_name != nonempty[0]:
                    mixed_name_chars.extend(extract_inline_characteristics_from_name_cell(nonempty[0], mixed_first_name))
        else:
            embedded_candidate = extract_embedded_indexed_name(nonempty[0]) if nonempty else ""
            if embedded_candidate:
                candidate_name = embedded_candidate
            elif len(nonempty) >= 2 and re.fullmatch(r"\d+[.)]?", nonempty[0]) and looks_like_material_name(nonempty[1]):
                candidate_name = nonempty[1]

        if not candidate_name and len(nonempty) >= 2 and (looks_like_material_name(nonempty[0]) or looks_like_generic_position_label(nonempty[0])) and looks_like_characteristic_cell(nonempty[1]):
            candidate_name = nonempty[0]
            attribute_row = True

        if not candidate_name and not attribute_row:
            candidate_name = pick_best_candidate_name(nonempty)

        continuation_row = attribute_row or (
            not candidate_name and any(looks_like_characteristic_cell(cell) for cell in nonempty)
        )

        if not candidate_name and current_item and (continuation_row or qty):
            if (
                qty
                and continuation_row_can_define_quantity(nonempty)
                and not any(has_material_hint(cell) for cell in nonempty)
            ):
                current_item.quantity = current_item.quantity or qty
                current_item.unit = current_item.unit or unit

            continuation_chars = extract_row_characteristics(
                nonempty,
                raw,
                unit=current_item.unit or unit,
            )
            current_item.characteristics = dedupe_preserve_order(
                current_item.characteristics + continuation_chars
            )
            current_item.search_query = build_search_query(
                current_item.normalized_name,
                current_item.characteristics,
            )
            continue

        if not candidate_name:
            current_item = None
            continue

        candidate_name = enrich_candidate_name(candidate_name, nonempty)
        qty, unit = suppress_embedded_dimension_quantity(candidate_name, qty, unit, nonempty)
        chars = extract_row_characteristics(nonempty, raw, candidate_name=candidate_name, unit=unit)
        chars = dedupe_preserve_order(mixed_name_chars + chars)
        mode = "parametric"

        item = RequirementItem(
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
        items.append(item)
        current_item = item

    return items


def parse_eis_product_cards(text: str) -> List[RequirementItem]:
    items: List[RequirementItem] = []
    lines = split_lines(text)
    idx = 0

    while idx < len(lines):
        line = lines[idx]
        next_line = lines[idx + 1] if idx + 1 < len(lines) else ""

        if (
            "|" in line
            or not looks_like_material_name(line)
            or looks_like_service_row(line)
            or looks_like_non_product_line(line)
            or not looks_like_eis_card_metadata_row(next_line)
        ):
            idx += 1
            continue

        name = normalize_material_name(line)
        meta_cells = [clean_cell(part) for part in next_line.split("|")]
        qty, unit = extract_eis_card_quantity_and_unit(meta_cells)
        chars: List[str] = []
        fragment_lines = [line, next_line]

        cursor = idx + 2
        if cursor < len(lines) and re.match(r"^\s*характеристики\s+товара", lines[cursor], re.I):
            fragment_lines.append(lines[cursor])
            cursor += 1

        if cursor < len(lines) and looks_like_table_header_row(lines[cursor]):
            cursor += 1

        while cursor < len(lines):
            current = lines[cursor]
            upcoming = lines[cursor + 1] if cursor + 1 < len(lines) else ""

            if re.search(r"\bнаименование\s+товара(?:,\s*работы,\s*услуги)?\b", current, re.I):
                break
            if "|" not in current and looks_like_material_name(current) and looks_like_eis_card_metadata_row(upcoming):
                break
            if "|" not in current and looks_like_service_row(current) and not looks_like_characteristic_cell(current):
                break

            if "|" in current:
                fragment_lines.append(current)
                char_text = characteristic_from_eis_row([clean_cell(part) for part in current.split("|")])
                if char_text:
                    chars.append(char_text)

            cursor += 1

        chars = dedupe_preserve_order(chars)
        item = RequirementItem(
            position_name=name,
            normalized_name=name,
            quantity=qty,
            unit=unit,
            characteristics=chars,
            notes=None,
            search_query=build_search_query(name, chars),
            source_fragment=" | ".join(fragment_lines),
            extraction_mode="mixed",
            confidence=0.92 if qty else 0.82,
        )
        items.append(item)
        idx = max(cursor, idx + 2)

    return items


def parse_inline_positions(text: str) -> List[RequirementItem]:
    """
    Режим для документов, где позиции заданы строками без нормальной таблицы:
    1. РУБЕМАСТ РНК 350 — 900 м2
    2. Смесь сухая штукатурная ... 120 мешков
    """
    items: List[RequirementItem] = []
    lines = split_lines(text)
    current_item: Optional[RequirementItem] = None

    for line in lines:
        low = line.lower()
        if looks_like_excluded_section(low):
            current_item = None
            continue
        if "|" in line:
            continue
        if looks_like_service_row(line):
            current_item = None
            continue
        if looks_like_non_product_line(line):
            continue

        qty, unit = detect_quantity_and_unit(line)
        if current_item and (
            looks_like_characteristic_cell(line)
            or (qty and not looks_like_material_name(line))
        ):
            if qty:
                current_item.quantity = current_item.quantity or qty
                current_item.unit = current_item.unit or unit

            continuation_chars = extract_characteristics_from_text(line)
            if looks_like_characteristic_cell(line):
                continuation_chars.append(line)

            current_item.characteristics = dedupe_preserve_order(
                current_item.characteristics + continuation_chars
            )
            current_item.search_query = build_search_query(
                current_item.normalized_name,
                current_item.characteristics,
            )
            continue

        if current_item and looks_like_inline_context_header_line(line):
            continue

        if not looks_like_material_name(line):
            current_item = None
            continue

        chars = extract_characteristics_from_text(line)
        if (
            not qty
            and not has_material_hint(line)
            and not contains_any(line, EXACT_NAME_HINTS)
            and not re.search(r"\b(материал|товар|продукц)\b", line.lower())
        ):
            current_item = None
            continue

        if qty or len(line) < 180:
            name = line
            if qty and unit:
                name = re.sub(rf"(\d+(?:[ .]\d+)?(?:[.,]\d+)?)\s*{re.escape(unit)}\b.*$", "", name, flags=re.I)
            name = normalize_material_name(name)

            if len(name) >= 5:
                mode = "exact_name" if contains_any(name, EXACT_NAME_HINTS) else "mixed"
                item = RequirementItem(
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
                items.append(item)
                current_item = item

    return items


def merge_similar_items(items: List[RequirementItem]) -> List[RequirementItem]:
    def canonical_merge_key(name: str) -> str:
        key = re.sub(r"\s+", " ", name.lower()).strip()
        key = re.sub(
            r"(шнур\s*\(жгут\)\s*для\s*заделки\s*швов)\s*0[.,]([68])\s*мм\.?\b",
            r"\1 \2 мм",
            key,
            flags=re.I,
        )
        return key

    def should_prefer_item_identity(base: RequirementItem, item: RequirementItem) -> bool:
        base_low = normalize_spaces(base.normalized_name or base.position_name).lower()
        item_low = normalize_spaces(item.normalized_name or item.position_name).lower()
        if (
            re.search(r"шнур\s*\(жгут\)\s*для\s*заделки\s*швов\s*0[.,][68]\s*мм", base_low)
            and re.search(r"шнур\s*\(жгут\)\s*для\s*заделки\s*швов\s*[68]\s*мм", item_low)
        ):
            return True
        if len(item.characteristics) > len(base.characteristics) + 1 and len(item.position_name) >= len(base.position_name):
            return True
        return False

    merged: Dict[str, RequirementItem] = {}

    for item in items:
        key = canonical_merge_key(item.normalized_name)
        if not key:
            continue

        if key in merged:
            base = merged[key]
            base_unit = normalize_unit_label(base.unit or "") or (base.unit or "")
            item_unit = normalize_unit_label(item.unit or "") or (item.unit or "")
            if (
                base.quantity
                and item.quantity
                and base.quantity != item.quantity
                and base_unit == item_unit
            ):
                key = f"{key}__{item.quantity}__{item_unit}"

        if key not in merged:
            merged[key] = item
            continue

        base = merged[key]
        if should_prefer_item_identity(base, item):
            base.position_name = item.position_name
            base.normalized_name = item.normalized_name
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
        if looks_like_procurement_instruction_line(req):
            continue
        if len(req) > 220:
            continue
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


def sanitize_items_characteristics(items: List[RequirementItem]) -> List[RequirementItem]:
    sanitized_items = []
    for item in items:
        cleaned_chars = []
        for char in item.characteristics:
            split_chars = split_characteristic_blob(char)
            if not split_chars and (
                looks_like_characteristic_cell(char)
                or re.search(r"\b(не\s+менее|гост|ту|мм|м2|кг|л|цвет|тип|основа|покрытие)\b", char.lower())
            ):
                split_chars = [char]

            for part in split_chars:
                clean_char = sanitize_characteristic_text(part)
                if not clean_char:
                    continue
                if looks_like_inline_context_header_line(clean_char):
                    continue
                if normalize_material_name(clean_char).lower() == normalize_material_name(item.normalized_name).lower():
                    continue
                cleaned_chars.append(clean_char)
        item.characteristics = dedupe_preserve_order(cleaned_chars)
        item.search_query = build_search_query(item.normalized_name, item.characteristics)
        sanitized_items.append(item)
    return sanitized_items


def validate_item(item: RequirementItem) -> bool:
    if not item.position_name or len(item.position_name) < 4:
        return False
    if "://" in item.position_name.lower():
        return False
    if "|" in item.position_name:
        return False
    if looks_like_excluded_section(item.position_name.lower()):
        return False
    if looks_like_service_row(item.position_name):
        return False
    if looks_like_non_product_line(item.position_name):
        return False
    if looks_like_characteristic_cell(item.position_name):
        return False
    if len(item.position_name.split()) == 1 and not item.quantity:
        if re.search(r"(ый|ий|ая|ое|ого|ного|ционный|ционная|ционное)$", item.position_name.lower()):
            return False
    if len(item.position_name) > 220:
        return False
    if not item.quantity and len(item.position_name) > 160:
        return False
    if not item.quantity and item.position_name and item.position_name[:1].islower():
        return False
    if ":" in item.position_name:
        left = normalize_spaces(item.position_name.split(":", 1)[0])
        if (
            looks_like_characteristic_cell(left)
            or looks_like_non_product_line(left)
            or re.search(r"\b(грузополучател\w*|маркиров\w*|место|срок|качеств\w*|упаков\w*|поставк\w*)\b", left.lower())
        ):
            return False
    if not looks_like_material_name(item.position_name):
        low = item.position_name.lower()
        if (
            item.quantity
            and 1 <= len(item.position_name.split()) <= 8
            and len(item.position_name) <= 120
            and not looks_like_non_product_line(item.position_name)
            and not looks_like_characteristic_cell(item.position_name)
        ):
            return True
        if not (item.quantity and re.search(r"\b(материал|товар|продукц)\b", low)):
            return False
    return True


def extract_tz_from_relevant_text(relevant_text: str, debug_info: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    debug_info = debug_info or {"mode": "preselected_relevant_text", "blocks": 1}
    if not relevant_text:
        return asdict(ExtractionResult(
            items=[],
            general_requirements=[],
            warnings=["Не найден релевантный фрагмент ТЗ/спецификации"],
            debug=debug_info
        ))

    general_requirements = detect_general_requirements(relevant_text)

    eis_card_items = parse_eis_product_cards(relevant_text)
    table_items = parse_parametric_rows(relevant_text)
    inline_items = parse_inline_positions(relevant_text)

    items = merge_similar_items(eis_card_items + table_items + inline_items)
    items = attach_global_characteristics(items, general_requirements)
    items = sanitize_items_characteristics(items)
    items = [item for item in items if validate_item(item)]

    warnings = []
    if not items:
        warnings.append("Позиции не выделены автоматически. Требуется ручная проверка блока ТЗ.")
    generic_items = sum(
        1
        for item in items
        if looks_like_generic_position_label(item.position_name)
        or item.position_name.lower().startswith("материал ")
    )
    if len(items) > 80 or (len(items) > 50 and items and generic_items / len(items) > 0.45):
        warnings.append("Найдено аномально много позиций. Возможна ошибка OCR или захват служебных таблиц.")

    result = ExtractionResult(
        items=items,
        general_requirements=general_requirements,
        warnings=warnings,
        debug=debug_info
    )
    return asdict(result)


def extract_tz_from_text(text: str) -> Dict[str, Any]:
    relevant_text, debug_info = extract_relevant_text(text)
    return extract_tz_from_relevant_text(relevant_text, debug_info=debug_info)


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
