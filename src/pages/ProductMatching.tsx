import { useState, useEffect, useCallback } from "react";
import { logger } from "../services/loggerService";
import {
  Search, Sparkles, Database, RefreshCw, Plus, Trash2,
  ExternalLink, ChevronDown, ChevronUp, CheckCircle,
  AlertCircle, Loader2, Package, X, Filter, Star,
  ArrowRight, BookOpen, Globe, FileText, ListChecks, Briefcase
} from "lucide-react";
import {
  TenderRequirementItem,
  TenderRequirementGroup,
  Tender,
  RequirementCharacteristic,
} from "../types";
import {
  getSelectedTendersForMatching,
  extractTenderRequirementsFromText,
  extractTenderRequirementsFromCrm,
  exportParsedTenderTzWord
} from "../services/geminiService";

// ── Типы ─────────────────────────────────────────────────────────────────────
type MatchingSearchMode = "local" | "ai" | "both";
type SearchStatus = "idle" | "searching" | "refining" | "done" | "error";

interface ProductSpec {
  [key: string]: string;
}

interface Product {
  id?: number | null;
  title: string;
  manufacturer?: string;
  category?: string;
  material_type?: string;
  price?: number | null;
  price_unit?: string;
  specs: ProductSpec;
  url?: string;
  url_status?: "verified" | "not_found";
  url_note?: string;
  source_title?: string;
  source_url?: string;
  source_url_status?: "verified" | "not_found";
  source_url_note?: string;
  description?: string;
  match_score?: number;
  validation_status?: "APPROVED" | "REJECTED" | "UNCERTAIN" | "UNKNOWN";
  validation_score?: number;
  validation_comment?: string;
  validation_matched_parameters?: string[];
  validation_conflicting_parameters?: string[];
  validation_missing_parameters?: string[];
  source?: "local_db" | "ai_search" | "manual";
  matched_requirement_id?: string;
  matched_requirement_name?: string;
  matched_tender_id?: string;
  matched_tender_title?: string;
}

interface ReferenceProfile {
  title?: string;
  manufacturer?: string;
  price?: number | null;
  price_unit?: string;
  specs?: ProductSpec;
  metrics?: Record<string, string | number | null | undefined>;
  source?: string;
  strict_price?: boolean;
}

interface SearchResult {
  query: string;
  local_results: Product[];
  ai_results: Product[];
  total: number;
  ai_error?: string;
  validation_error?: string;
  validation_summary?: string;
  reference_profile?: ReferenceProfile;
}

interface ExtractedTenderGroup extends TenderRequirementGroup {
  group_key: string;
}

interface StoredSearchResult extends SearchResult {
  item_id: string;
  requirements: string;
  mode: MatchingSearchMode;
  ai_error?: string;
  saved_at: string;
}

interface SearchRefinementJob {
  job_id: string;
  status: "queued" | "running" | "waiting_retry" | "completed" | "error";
  stage?: string;
  query?: string;
  mode?: MatchingSearchMode;
  result?: SearchResult;
  aiError?: string | null;
  error?: string;
  retry_count?: number;
  next_retry_at?: string | null;
  created_at?: string;
  updated_at?: string;
}

interface ProductMatchingPersistedState {
  version?: number;
  extractedTenderGroups: ExtractedTenderGroup[];
  selectedExtractedItemIds: string[];
  searchQueue: TenderRequirementItem[];
  activeSearchQueueItemId: string | null;
  searchResultsByItemId: Record<string, StoredSearchResult>;
  selectedProducts: Product[];
}

// ── Вспомогательные ───────────────────────────────────────────────────────────
const PRODUCT_MATCHING_STORAGE_KEY = "TENDER_SMART_PRODUCT_MATCHING_STATE";
const PRODUCT_MATCHING_STORAGE_VERSION = 2;
const TIME_OF_DAY_FORMATTER = new Intl.DateTimeFormat("ru-RU", {
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
});

const formatPrice = (price?: number | null, unit?: string): string => {
  if (!price) return "—";
  const formatted = new Intl.NumberFormat("ru-RU").format(price);
  return unit ? `${formatted} ${unit}` : `${formatted} ₽`;
};

const formatRetryCountdown = (
  job: SearchRefinementJob | null | undefined,
  nowMs: number
): string => {
  if (!job || job.status !== "waiting_retry" || !job.next_retry_at) {
    return "";
  }

  const retryAtMs = Date.parse(job.next_retry_at);
  if (!Number.isFinite(retryAtMs)) {
    return "";
  }

  const remainingSeconds = Math.max(0, Math.ceil((retryAtMs - nowMs) / 1000));
  const retryAtLabel = TIME_OF_DAY_FORMATTER.format(new Date(retryAtMs));

  if (remainingSeconds <= 0) {
    return `повтор запускается сейчас · до ${retryAtLabel}`;
  }

  if (remainingSeconds >= 60) {
    const minutes = Math.floor(remainingSeconds / 60);
    const seconds = remainingSeconds % 60;
    return `повтор через ${minutes}:${String(seconds).padStart(2, "0")} · до ${retryAtLabel}`;
  }

  return `повтор через ${remainingSeconds} сек · до ${retryAtLabel}`;
};

const normalizeExternalUrl = (value: unknown): string => {
  let raw = String(value || "").replace(/\\\//g, "/").trim();
  if (!raw) {
    return "";
  }

  const markdownMatch = raw.match(/\((https?:\/\/[^)\s]+)\)/i);
  if (markdownMatch) {
    raw = markdownMatch[1];
  } else {
    const angleMatch = raw.match(/<(https?:\/\/[^>\s]+)>/i);
    if (angleMatch) {
      raw = angleMatch[1];
    }
  }

  raw = raw.replace(/^[\s"'`<>()\[\]]+|[\s"'`<>()\[\].,;:!?]+$/g, "");
  const urlMatch = raw.match(/((?:https?:\/\/|www\.)[^\s<>"'`]+|(?:[a-z0-9-]+\.)+[a-z]{2,}(?:\/[^\s<>"'`]+)?)/i);
  if (urlMatch) {
    raw = urlMatch[1];
  }

  if (!/^[a-z][a-z0-9+.-]*:\/\//i.test(raw)) {
    if (/^(?:www\.)?(?:[a-z0-9-]+\.)+[a-z]{2,}(?:\/|$)/i.test(raw)) {
      raw = `https://${raw.replace(/^\/+/, "")}`;
    } else {
      return "";
    }
  }

  try {
    const parsed = new URL(raw);
    const host = parsed.hostname.toLowerCase();
    const path = parsed.pathname.toLowerCase();
    if (!["http:", "https:"].includes(parsed.protocol)) {
      return "";
    }
    if (
      /^(?:www\.)?google\./i.test(host) ||
      /^(?:www\.)?yandex\./i.test(host) ||
      ["bing.com", "www.bing.com", "go.mail.ru", "search.yahoo.com", "search.yahoo.co.jp"].includes(host) ||
      host.endsWith("googleusercontent.com") ||
      path.startsWith("/search") ||
      path.startsWith("/url")
    ) {
      return "";
    }
    return parsed.toString();
  } catch {
    return "";
  }
};

interface ComparisonRow {
  key: string;
  label: string;
  requiredValue: string;
  referenceValue: string;
  referenceOnly: boolean;
  productCells: Array<{
    value: string;
    mismatch: boolean;
    missing: boolean;
  }>;
}

const normalizeCharacteristicKey = (label: string): string => {
  const normalized = String(label || "")
    .toLowerCase()
    .replace(/\([^)]*\)/g, " ")
    .replace(/[^a-zа-я0-9]+/gi, " ")
    .replace(/\s+/g, " ")
    .trim();

  if (!normalized) {
    return "";
  }

  const aliases: Array<[RegExp, string]> = [
    [/(тип материал|гидроизоляц.*рулон|рулонн.*наплавля)/, "тип материала"],
    [/(состав|структур.*материал|структура)/, "состав"],
    [/(материал.*основ|основа материала|армирующ.*основ)/, "основа"],
    [/армирующ.*слой/, "основа"],
    [/(внешн.*вид|неоднород|посторонн.*включ|вид товара)/, "вид"],
    [/(время.*высых|высыхани)/, "время высыхания"],
    [/(способ.*монтаж|метод.*укладк|способ.*нанесени|метод.*нанесени)/, "способ монтажа"],
    [/(температур.*размягч|размягчени)/, "температура размягчения"],
    [/(температур.*применен|диапазон.*применен|допустимая температура.*примен)/, "температура применения"],
    [/(температур.*хрупк|хрупк.*покровн|хрупк.*вяжущ)/, "температура хрупкости"],
    [/толщ/, "толщина"],
    [/(масса|вес|вес 1 м2|масса 1 м2|масса материала)/, "масса"],
    [/(масс.*покровн.*состав|масс.*вяжущ)/, "масса покровного состава"],
    [/(массов.*дол.*нелетуч|нелетуч.*веществ)/, "массовая доля нелетучих веществ"],
    [/(вязк|вискозиметр)/, "условная вязкость"],
    [/гибк/, "гибкость"],
    [/теплостой/, "теплостойкость"],
    [/(разрыв|прочност.*растяж|сила при растяж)/, "разрывная сила"],
    [/(прочност.*сцеплен.*бетон|с бетоном)/, "прочность сцепления с бетоном"],
    [/(прочност.*сцеплен.*металл|с металлом)/, "прочность сцепления с металлом"],
    [/(условн.*прочност)/, "условная прочность"],
    [/(относительн.*удлинен|удлинени.*растяж)/, "относительное удлинение"],
    [/(водонепроница|водоупор)/, "водонепроницаемость"],
    [/водопоглощ/, "водопоглощение"],
    [/основа/, "основа"],
    [/маркир/, "марка"],
    [/(тип.*защитн.*покрыт.*верх.*ниж|защитн.*покрыт.*верх.*ниж|покрытие.*верх.*ниж)/, "тип защитного покрытия"],
    [/(тип.*покрыт.*верхн.*сторон|материал.*покрыт.*верхн.*сторон)/, "верхнее покрытие"],
    [/(тип.*покрыт.*нижн.*сторон|материал.*покрыт.*нижн.*сторон|тип.*покрыт.*наплавля.*сторон|наплавля.*сторон)/, "нижнее покрытие"],
    [/(верхн.*сторон|верхн.*покрыт|посыпк)/, "верхнее покрытие"],
    [/(нижн.*сторон|нижн.*покрыт|пленк)/, "нижнее покрытие"],
    [/марка/, "марка"],
    [/(размер|размер.*рулон|длина.*ширина|ширина.*длина|рул.*м2)/, "размер"],
    [/площадь.*рулон/, "площадь"],
    [/ширин/, "ширина"],
    [/длин/, "длина"],
    [/(тара|ведро|канистр)/, "тара"],
    [/(тип материал|тип продукц|тип изоляц|тип покрытия)/, "тип материала"],
    [/группа.*горюч/, "группа горючести"],
    [/класс.*пожар/, "класс пожарной опасности"],
    [/расход/, "расход"],
    [/(стандарт|нормативн.*документ|соответств.*норматив|соответств.*гост)/, "стандарт"],
    [/гост/, "гост"],
    [/ту/, "ту"],
  ];

  for (const [pattern, key] of aliases) {
    if (pattern.test(normalized)) {
      return key;
    }
  }

  return normalized;
};

const parseStructuredCharacteristics = (
  item?: Pick<TenderRequirementItem, "structured_characteristics" | "characteristics"> | null
): RequirementCharacteristic[] => {
  if (!item) {
    return [];
  }

  if (Array.isArray(item.structured_characteristics) && item.structured_characteristics.length > 0) {
    return item.structured_characteristics
      .map(char => ({
        name: String(char?.name || "").trim(),
        value: String(char?.value || "").trim(),
      }))
      .filter(char => Boolean(char.name || char.value));
  }

  return Array.isArray(item.characteristics)
    ? item.characteristics
        .map((raw): RequirementCharacteristic | null => {
          const text = String(raw || "").trim();
          if (!text) {
            return null;
          }
          if (text.includes(":")) {
            const [name, ...valueParts] = text.split(":");
            return {
              name: name.trim(),
              value: valueParts.join(":").trim(),
            };
          }
          return { name: "", value: text };
        })
        .filter((char): char is RequirementCharacteristic => Boolean(char))
    : [];
};

const formatRequirementCharacteristics = (characteristics: RequirementCharacteristic[]): string[] =>
  characteristics
    .map(char => {
      if (char.name && char.value) {
        return `${char.name}: ${char.value}`;
      }
      return char.value || char.name;
    })
    .filter(Boolean);

const parseRequirementTextToCharacteristics = (text: string): RequirementCharacteristic[] => {
  if (!text.trim()) {
    return [];
  }

  return text
    .split(/\n|;/)
    .map(line => line.trim())
    .filter(Boolean)
    .map((line): RequirementCharacteristic => {
      if (line.includes(":")) {
        const [name, ...valueParts] = line.split(":");
        return {
          name: name.trim(),
          value: valueParts.join(":").trim(),
        };
      }
      return { name: "", value: line };
    })
    .filter(char => Boolean(char.name || char.value));
};

const buildRequirementText = (item: TenderRequirementItem): string => {
  const structured = parseStructuredCharacteristics(item);
  const charLines = structured.length > 0
    ? formatRequirementCharacteristics(structured)
    : item.characteristics;
  return charLines.join("\n") + (item.notes ? `\nДоп: ${item.notes}` : "");
};

const buildCharacteristicMap = (
  characteristics: RequirementCharacteristic[]
): Map<string, { label: string; value: string }> => {
  const result = new Map<string, { label: string; value: string }>();

  characteristics.forEach((characteristic, index) => {
    const label = (characteristic.name || characteristic.value || `Характеристика ${index + 1}`).trim();
    const value = (characteristic.value || characteristic.name || "").trim();
    const key = normalizeCharacteristicKey(characteristic.name || characteristic.value || label);

    if (!key || !value) {
      return;
    }

    if (!result.has(key)) {
      result.set(key, { label, value });
    }
  });

  return result;
};

const metricToRequirementCharacteristic = (
  label: string,
  rawValue: string | number | null | undefined,
  suffix = ""
): RequirementCharacteristic | null => {
  if (rawValue === null || rawValue === undefined || rawValue === "") {
    return null;
  }
  const value = typeof rawValue === "number"
    ? `${String(rawValue).replace(".", ",")}${suffix}`
    : `${String(rawValue).trim()}${suffix}`;
  return {
    name: label,
    value,
  };
};

const buildProductCharacteristicMap = (product: Product): Map<string, string> => {
  const result = new Map<string, string>();
  const basePattern = /(стеклоткан|стеклохолст|полиэстер|полиэф|каркасн|холст|ткан)/i;
  const coverPattern = /(сланец|гранулят|пленк|фольг|посып|песок)/i;
  const extractMassFromMarkSuffix = (value: string): string => {
    const match = String(value || "").match(/(?:ткп|тпп|хкп|хпп|экп|эпп)\s*[- ]\s*(\d+(?:[.,]\d+)?)/i);
    return match ? `${match[1]} кг/м2` : "";
  };
  const extractMarkingFromText = (value: string): string => {
    const tokens = String(value || "")
      .toLowerCase()
      .split(/[^a-zа-я0-9]+/i)
      .filter(Boolean);
    const mark = tokens.find(token => ["ткп", "тпп", "хкп", "хпп", "экп", "эпп", "тпэ", "ткэ", "эпм", "экм"].includes(token));
    return mark ? mark.toUpperCase() : "";
  };
  const normalizeBaseFromShortCode = (value: string): string => {
    const normalized = String(value || "").trim().toLowerCase();
    if (normalized === "с/т") {
      return "стеклоткань";
    }
    if (normalized === "с/х") {
      return "стеклохолст";
    }
    if (normalized === "п/э") {
      return "полиэстер";
    }
    return value;
  };
  const deriveRollMaterialFromMark = (marking: string) => {
    const normalized = String(marking || "").trim().toUpperCase();
    if (normalized.length < 3) {
      return null;
    }

    const baseLetter = normalized[0];
    const upperLetter = normalized[1];
    const lowerLetter = normalized[2];

    const baseMap: Record<string, string> = {
      "Т": "стеклоткань",
      "Х": "стеклохолст",
      "Э": "полиэстер",
    };
    const coverMap: Record<string, string> = {
      "К": "крупнозернистая посыпка",
      "П": "пленка",
      "М": "мелкозернистая посыпка",
    };

    return {
      base: baseMap[baseLetter] || "",
      upper: coverMap[upperLetter] || "",
      lower: coverMap[lowerLetter] || "",
    };
  };
  const addValue = (key: string, value: string) => {
    const normalizedKey = normalizeCharacteristicKey(key);
    const normalizedValue = String(value || "").trim();
    if (!normalizedKey || !normalizedValue) {
      return;
    }
    if (["показатель", "наименование показателя", "значение", "норма"].includes(normalizedKey)) {
      return;
    }
    if (!result.has(normalizedKey)) {
      result.set(normalizedKey, normalizedValue);
    }
  };

  const addRollDimensionsFromText = (rawValue: string) => {
    const text = String(rawValue || "").trim();
    if (!text) {
      return;
    }

    const lengthMatch = text.match(/длин(?:а|ой)?[^0-9-]*?(?:не\s+менее\s+|не\s+более\s+)?(-?\d+(?:[.,]\d+)?)\s*м(?![a-zа-я0-9])/i);
    const widthMatch = text.match(/ширин(?:а|ой)?[^0-9-]*?(?:не\s+менее\s+|не\s+более\s+)?(-?\d+(?:[.,]\d+)?)\s*м(?![a-zа-я0-9])/i);
    const areaMatch = text.match(/площад[ьи][^0-9-]*?(?:не\s+менее\s+|не\s+более\s+)?(-?\d+(?:[.,]\d+)?)\s*м2(?![a-zа-я0-9])/i);

    if (lengthMatch) {
      addValue("длина", `${lengthMatch[1]} м`);
    }
    if (widthMatch) {
      addValue("ширина", `${widthMatch[1]} м`);
    }
    if (areaMatch) {
      addValue("площадь", `${areaMatch[1]} м2`);
    }

    const dimensionTokens = Array.from(
      text.matchAll(/(-?\d+(?:[.,]\d+)?)\s*(м2|м²|мм|см|м)(?![a-zа-я0-9])/gi)
    ).map(match => ({
      value: match[1],
      unit: match[2].toLowerCase(),
    }));
    if (dimensionTokens.length >= 2) {
      const linearTokens = dimensionTokens.filter(token => token.unit === "м");
      if (!result.has("длина") && linearTokens[0]) {
        addValue("длина", `${linearTokens[0].value} м`);
      }
      if (!result.has("ширина") && linearTokens[1]) {
        addValue("ширина", `${linearTokens[1].value} м`);
      }
      if (!result.has("площадь")) {
        const areaToken = dimensionTokens.find(token => token.unit === "м2" || token.unit === "м²");
        if (areaToken) {
          addValue("площадь", `${areaToken.value} м2`);
        } else if (linearTokens[0] && linearTokens[1]) {
          const lengthValue = Number(linearTokens[0].value.replace(",", "."));
          const widthValue = Number(linearTokens[1].value.replace(",", "."));
          if (!Number.isNaN(lengthValue) && !Number.isNaN(widthValue)) {
            addValue("площадь", `${String(lengthValue * widthValue).replace(".", ",")} м2`);
          }
        }
      }
    }

    if (
      (!result.has("длина") || !result.has("ширина") || !result.has("площадь"))
      && /^\d+(?:[.,]\d+)?\s*[xх*\/]\s*\d+(?:[.,]\d+)?(?:\s*[xх*\/]\s*\d+(?:[.,]\d+)?)?$/.test(text)
    ) {
      const plainNumbers = text.match(/\d+(?:[.,]\d+)?/g) || [];
      if (plainNumbers[0] && !result.has("длина")) {
        addValue("длина", `${plainNumbers[0]} м`);
      }
      if (plainNumbers[1] && !result.has("ширина")) {
        addValue("ширина", `${plainNumbers[1]} м`);
      }
      if (!result.has("площадь")) {
        if (plainNumbers[2]) {
          addValue("площадь", `${plainNumbers[2]} м2`);
        } else if (plainNumbers[0] && plainNumbers[1]) {
          const lengthValue = Number(plainNumbers[0].replace(",", "."));
          const widthValue = Number(plainNumbers[1].replace(",", "."));
          if (!Number.isNaN(lengthValue) && !Number.isNaN(widthValue)) {
            addValue("площадь", `${String(lengthValue * widthValue).replace(".", ",")} м2`);
          }
        }
      }
    }
  };

  const addCompositeSurfaceBase = (rawValue: string) => {
    const parts = rawValue
      .split("/")
      .map(part => part.trim())
      .filter(Boolean);

    if (parts.length < 2) {
      return;
    }

    parts.forEach(part => {
      if (basePattern.test(part)) {
        addValue("основа", part);
        addValue("армирующий слой", part);
      } else if (coverPattern.test(part)) {
        if (/пленк/i.test(part)) {
          addValue("нижнее покрытие", part);
        } else {
          addValue("верхнее покрытие", part);
        }
      }
    });
  };

  Object.entries(product.specs || {}).forEach(([key, value]) => {
    const text = String(value || "").trim();
    if (!text) {
      return;
    }

    addValue(key, text);

    const normalizedLabel = String(key || "").toLowerCase();
    const normalizedKey = normalizeCharacteristicKey(key);
    if (normalizedLabel.includes("покрытие верхнее/нижнее") || normalizedKey === "тип защитного покрытия") {
      const [upper, lower] = text.split("/").map(part => part.trim()).filter(Boolean);
      if (upper) {
        addValue("верхнее покрытие", upper);
      }
      if (lower) {
        addValue("нижнее покрытие", lower);
      }
      if (upper || lower) {
        addValue("тип защитного покрытия", [upper, lower].filter(Boolean).join(" / "));
      }
    }
    if (normalizedLabel.includes("поверхность/основа")) {
      addCompositeSurfaceBase(text);
    }
    if (normalizedLabel.includes("основа") && text.includes("/")) {
      addCompositeSurfaceBase(text);
    }
    if (normalizedLabel.includes("соответствует нормативным документам")) {
      addValue("стандарт", text);
    }
    if (normalizedLabel.includes("метод укладки") && /наплав/i.test(text)) {
      addValue("тип материала", "рулонный наплавляемый материал");
      addValue("способ монтажа", "наплавление");
    }
    if (
      normalizedKey === "размер"
      || normalizedKey === "длина"
      || normalizedKey === "ширина"
      || normalizedKey === "площадь"
      || normalizedLabel.includes("длина/ширина/площадь рулона")
      || normalizedLabel.includes("размер рулона")
    ) {
      addRollDimensionsFromText(text);
    }
  });

  const identityText = [
    product.title || "",
    product.category || "",
    product.material_type || "",
  ].join(" ");
  const normalizedIdentity = identityText.toLowerCase();
  if (/(рулон|гидроизоляц|кровель|ткп|тпп|хкп|хпп|эпп|экп)/i.test(identityText)) {
    addValue(
      "тип материала",
      /наплав|ткп|тпп|хкп|хпп|эпп|экп/i.test(identityText)
        ? "рулонный наплавляемый материал"
        : "рулонный гидроизоляционный материал"
    );
  } else if (product.material_type || product.category) {
    addValue("тип материала", String(product.material_type || product.category || "").trim());
  }
  if (!result.has("способ монтажа") && /наплав|ткп|тпп|хкп|хпп|эпп|экп/i.test(identityText)) {
    addValue("способ монтажа", "наплавление");
  }

  if (!result.has("основа")) {
    const titleBaseMatch = identityText.match(/(стеклоткан[ьи]?|стеклохолст|полиэстер|полиэфир|с\/т|с\/х|п\/э)/i);
    if (titleBaseMatch) {
      const baseValue = normalizeBaseFromShortCode(titleBaseMatch[1]);
      addValue("основа", baseValue);
      addValue("армирующий слой", baseValue);
    }
  }

  if (!result.has("верхнее покрытие")) {
    const titleCoverMatch = normalizedIdentity.match(/(сланец|гранулят|пленка|пленк)/i);
    if (titleCoverMatch) {
      addValue("верхнее покрытие", titleCoverMatch[1]);
    }
  }

  if (!result.has("нижнее покрытие") && /пленк/i.test(normalizedIdentity)) {
    addValue("нижнее покрытие", "пленка");
  }

  if (!result.has("тип защитного покрытия")) {
    const upperCover = result.get("верхнее покрытие");
    const lowerCover = result.get("нижнее покрытие");
    if (upperCover || lowerCover) {
      addValue("тип защитного покрытия", [upperCover, lowerCover].filter(Boolean).join(" / "));
    }
  }

  if (!result.has("марка")) {
    const marking = extractMarkingFromText(identityText);
    if (marking) {
      addValue("марка", marking);
    }
  }

  const derivedFromMark = deriveRollMaterialFromMark(result.get("марка") || extractMarkingFromText(identityText));
  if (derivedFromMark?.base && !result.has("основа")) {
    addValue("основа", derivedFromMark.base);
    addValue("армирующий слой", derivedFromMark.base);
  }
  if (derivedFromMark?.upper && !result.has("верхнее покрытие")) {
    addValue("верхнее покрытие", derivedFromMark.upper);
  }
  if (derivedFromMark?.lower && !result.has("нижнее покрытие")) {
    addValue("нижнее покрытие", derivedFromMark.lower);
  }
  if (!result.has("масса")) {
    const derivedMass = extractMassFromMarkSuffix(identityText);
    if (derivedMass) {
      addValue("масса", derivedMass);
    }
  }
  if (!result.has("тип защитного покрытия")) {
    const upperCover = result.get("верхнее покрытие");
    const lowerCover = result.get("нижнее покрытие");
    if (upperCover || lowerCover) {
      addValue("тип защитного покрытия", [upperCover, lowerCover].filter(Boolean).join(" / "));
    }
  }

  if (!result.has("состав")) {
    const compositionParts = [
      result.get("тип материала"),
      result.get("основа") ? `основа: ${result.get("основа")}` : "",
      result.get("тип защитного покрытия") ? `покрытие: ${result.get("тип защитного покрытия")}` : "",
    ].filter(Boolean);
    if (compositionParts.length) {
      addValue("состав", compositionParts.join("; "));
    }
  }

  const sizeValue = result.get("размер");
  if (sizeValue) {
    addRollDimensionsFromText(sizeValue);
  }

  return result;
};

const normalizeStringArray = (value: unknown): string[] =>
  Array.isArray(value)
    ? value
        .map(item => String(item || "").trim())
        .filter(Boolean)
    : [];

const normalizeSearchProduct = (
  rawProduct: any,
  source: Product["source"]
): Product => {
  const specs = rawProduct?.specs && typeof rawProduct.specs === "object" && !Array.isArray(rawProduct.specs)
    ? Object.fromEntries(
        Object.entries(rawProduct.specs)
          .map(([key, value]) => [String(key), String(value ?? "").trim()])
          .filter(([key, value]) => Boolean(key.trim()) && Boolean(value))
      )
    : {};

  return {
    ...rawProduct,
    source,
    specs,
    url: normalizeExternalUrl(rawProduct?.url) || undefined,
    url_status: rawProduct?.url_status === "verified" ? "verified" : rawProduct?.url_status === "not_found" ? "not_found" : undefined,
    url_note: String(rawProduct?.url_note || "").trim() || undefined,
    source_title: String(rawProduct?.source_title || "").trim() || undefined,
    source_url: normalizeExternalUrl(rawProduct?.source_url) || undefined,
    source_url_status:
      rawProduct?.source_url_status === "verified"
        ? "verified"
        : rawProduct?.source_url_status === "not_found"
          ? "not_found"
          : undefined,
    source_url_note: String(rawProduct?.source_url_note || "").trim() || undefined,
    validation_matched_parameters: normalizeStringArray(rawProduct?.validation_matched_parameters),
    validation_conflicting_parameters: normalizeStringArray(rawProduct?.validation_conflicting_parameters),
    validation_missing_parameters: normalizeStringArray(rawProduct?.validation_missing_parameters),
  };
};

const normalizeTitleIdentity = (value: string): string =>
  String(value || "")
    .toLowerCase()
    .replace(/[^a-zа-я0-9]+/gi, " ")
    .replace(/\s+/g, " ")
    .trim();

const enrichProductFromCatalog = (product: Product, catalogProducts: Product[]): Product => {
  const productSpecsCount = Object.keys(product.specs || {}).length;
  const normalizedTitle = normalizeTitleIdentity(product.title);
  const catalogProduct = catalogProducts.find(candidate => {
    if (product.id != null && candidate.id != null && String(product.id) === String(candidate.id)) {
      return true;
    }
    return normalizeTitleIdentity(candidate.title) === normalizedTitle;
  });

  if (!catalogProduct) {
    return product;
  }

  const catalogSpecs = catalogProduct.specs || {};
  const mergedSpecs = productSpecsCount >= Object.keys(catalogSpecs).length
    ? product.specs || {}
    : { ...catalogSpecs, ...(product.specs || {}) };

  return {
    ...catalogProduct,
    ...product,
    specs: mergedSpecs,
    source: product.source || catalogProduct.source,
    validation_status: product.validation_status,
    validation_score: product.validation_score,
    validation_comment: product.validation_comment,
    validation_matched_parameters: product.validation_matched_parameters,
    validation_conflicting_parameters: product.validation_conflicting_parameters,
    validation_missing_parameters: product.validation_missing_parameters,
  };
};

const formatCharacteristicDisplayLabel = (key: string): string => {
  const labels: Record<string, string> = {
    "тип материала": "Тип материала",
    "состав": "Состав",
    "основа": "Материал основы",
    "армирующий слой": "Армирующий слой",
    "марка": "Маркировка",
    "тип защитного покрытия": "Тип защитного покрытия верх/низ",
    "верхнее покрытие": "Верхнее покрытие",
    "нижнее покрытие": "Нижнее покрытие",
    "способ монтажа": "Способ монтажа",
    "толщина": "Толщина",
    "масса": "Масса 1 м2",
    "масса покровного состава": "Масса покровного состава",
    "гибкость": "Температура гибкости на брусе",
    "температура хрупкости": "Температура хрупкости",
    "теплостойкость": "Теплостойкость",
    "размер": "Размер рулона",
    "длина": "Длина рулона",
    "ширина": "Ширина рулона",
    "площадь": "Площадь рулона",
    "разрывная сила": "Разрывная сила при растяжении",
    "водонепроницаемость": "Водонепроницаемость",
    "водопоглощение": "Водопоглощение",
    "стандарт": "Стандарт",
  };
  return labels[key] || key;
};

const buildCanonicalReferenceProfileCharacteristics = (
  referenceProfile?: ReferenceProfile
): RequirementCharacteristic[] => {
  if (!referenceProfile) {
    return [];
  }

  const pseudoProduct: Product = {
    title: referenceProfile.title || "",
    manufacturer: referenceProfile.manufacturer || "",
    category: "",
    material_type: "",
    price: referenceProfile.price ?? null,
    price_unit: referenceProfile.price_unit,
    specs: referenceProfile.specs || {},
    source: "manual",
  };

  const canonicalMap = buildProductCharacteristicMap(pseudoProduct);
  const metrics = referenceProfile.metrics || {};
  const priorityKeys = [
    "тип материала",
    "состав",
    "основа",
    "марка",
    "тип защитного покрытия",
    "верхнее покрытие",
    "нижнее покрытие",
    "способ монтажа",
    "толщина",
    "масса",
    "гибкость",
    "температура хрупкости",
    "теплостойкость",
    "размер",
    "длина",
    "ширина",
    "площадь",
    "разрывная сила",
    "водонепроницаемость",
    "водопоглощение",
    "стандарт",
  ];

  const result: RequirementCharacteristic[] = [];
  const usedKeys = new Set<string>();
  const pushCharacteristic = (name: string, value: string | null | undefined) => {
    const text = String(value || "").trim();
    if (!name || !text) {
      return;
    }
    result.push({ name, value: text });
  };

  priorityKeys.forEach(key => {
    const value = canonicalMap.get(key);
    if (!value) {
      return;
    }
    usedKeys.add(key);
    pushCharacteristic(formatCharacteristicDisplayLabel(key), value);
  });

  const metricFallbacks = [
    metricToRequirementCharacteristic("Толщина", metrics.thickness, " мм"),
    metricToRequirementCharacteristic("Масса 1 м2", metrics.mass, " кг/м2"),
    metricToRequirementCharacteristic("Температура гибкости на брусе", metrics.flex, " °C"),
  ];
  metricFallbacks.forEach(characteristic => {
    if (!characteristic) {
      return;
    }
    const normalizedKey = normalizeCharacteristicKey(characteristic.name);
    if (!usedKeys.has(normalizedKey)) {
      usedKeys.add(normalizedKey);
      result.push(characteristic);
    }
  });

  canonicalMap.forEach((value, key) => {
    if (!value || usedKeys.has(key)) {
      return;
    }
    usedKeys.add(key);
    pushCharacteristic(formatCharacteristicDisplayLabel(key), value);
  });

  Object.entries(referenceProfile.specs || {}).forEach(([key, value]) => {
    const text = String(value || "").trim();
    const normalizedKey = normalizeCharacteristicKey(key);
    if (!text || usedKeys.has(normalizedKey)) {
      return;
    }
    usedKeys.add(normalizedKey);
    result.push({ name: key, value: text });
  });

  return result;
};

const buildValidationKeySet = (values?: string[]): Set<string> =>
  new Set(
    (values || [])
      .map(value => normalizeCharacteristicKey(value))
      .filter(Boolean)
  );

const extractNumbers = (text: string): number[] => {
  const matches = String(text || "")
    .replace(/\u2212/g, "-")
    .match(/-?\d+(?:[.,]\d+)?/g);

  if (!matches) {
    return [];
  }

  return matches
    .map(value => Number(value.replace(",", ".")))
    .filter(value => !Number.isNaN(value));
};

const hasCommonStandardNumber = (requiredValue: string, productValue: string): boolean => {
  const requiredNumbers = requiredValue.match(/\d{2,}/g) || [];
  const productNumbers = new Set(productValue.match(/\d{2,}/g) || []);
  return requiredNumbers.some(value => productNumbers.has(value));
};

const valueConflictsWithRequirement = (label: string, requiredValue: string, productValue: string): boolean => {
  const required = String(requiredValue || "").trim().toLowerCase();
  const product = String(productValue || "").trim().toLowerCase();
  if (!required || !product) {
    return false;
  }

  if (/гост|ту|норматив/.test(label)) {
    return !hasCommonStandardNumber(required, product);
  }

  if (/вид/.test(label) && /без видимых посторонних включений/.test(required)) {
    return /есть|допуска|налич/.test(product);
  }

  const requiredNumbers = extractNumbers(required);
  const productNumbers = extractNumbers(product);
  if (requiredNumbers.length && productNumbers.length) {
    const productMin = Math.min(...productNumbers);
    const productMax = Math.max(...productNumbers);

    if (required.includes("не менее")) {
      return productMax < requiredNumbers[0];
    }
    if (required.includes("не более")) {
      return productMin > requiredNumbers[0];
    }
    if (required.includes("от ") && required.includes(" до ") && requiredNumbers.length >= 2) {
      const requiredMin = Math.min(requiredNumbers[0], requiredNumbers[1]);
      const requiredMax = Math.max(requiredNumbers[0], requiredNumbers[1]);
      return productMax < requiredMin || productMin > requiredMax;
    }
    if (
      requiredNumbers.length >= 2 &&
      /[-–]/.test(required) &&
      !required.includes("не менее") &&
      !required.includes("не более")
    ) {
      const requiredMin = Math.min(requiredNumbers[0], requiredNumbers[1]);
      const requiredMax = Math.max(requiredNumbers[0], requiredNumbers[1]);
      return productMax < requiredMin || productMin > requiredMax;
    }
  }

  return false;
};

const buildComparisonMatrix = (
  item: TenderRequirementItem | null,
  searchResult: SearchResult | null,
  requirementsText: string,
  catalogProducts: Product[] = []
): { rows: ComparisonRow[]; products: Product[]; referenceOnlyCount: number; hasReferenceProfile: boolean } => {
  if (!searchResult) {
    return { rows: [], products: [], referenceOnlyCount: 0, hasReferenceProfile: false };
  }

  const requiredCharacteristics = item
    ? parseStructuredCharacteristics(item)
    : parseRequirementTextToCharacteristics(requirementsText);
  const referenceCharacteristics = buildCanonicalReferenceProfileCharacteristics(searchResult.reference_profile);
  const hasReferenceProfile = referenceCharacteristics.length > 0;
  const requiredMap = buildCharacteristicMap(requiredCharacteristics);
  const referenceMap = buildCharacteristicMap(referenceCharacteristics);
  const products = [...(searchResult.local_results || []), ...(searchResult.ai_results || [])]
    .map(product => enrichProductFromCatalog(product, catalogProducts));
  const productMaps = products.map(product => buildProductCharacteristicMap(product));
  const productConflictKeys = products.map(product => buildValidationKeySet(product.validation_conflicting_parameters));
  const productMissingKeys = products.map(product => buildValidationKeySet(product.validation_missing_parameters));

  const orderedKeys = hasReferenceProfile
    ? [
        ...Array.from(requiredMap.keys()),
        ...Array.from(referenceMap.keys()).filter(key => !requiredMap.has(key)),
      ]
    : Array.from(requiredMap.keys());

  const rows = orderedKeys.map((key): ComparisonRow => ({
    key,
    label: requiredMap.get(key)?.label || referenceMap.get(key)?.label || key,
    requiredValue: requiredMap.get(key)?.value || "",
    referenceValue: referenceMap.get(key)?.value || "",
    referenceOnly: !requiredMap.has(key) && referenceMap.has(key),
    productCells: productMaps.map((productMap, index) => {
      const value = productMap.get(key) || "";
      const explicitMismatch = productConflictKeys[index].has(key);
      const inferredMismatch = !explicitMismatch
        && Boolean(value)
        && Boolean(requiredMap.get(key)?.value)
        && valueConflictsWithRequirement(
          requiredMap.get(key)?.label || key,
          requiredMap.get(key)?.value || "",
          value
        );

      return {
        value,
        mismatch: explicitMismatch || inferredMismatch,
        missing: !value && productMissingKeys[index].has(key),
      };
    }),
  }));

  return {
    rows,
    products,
    referenceOnlyCount: rows.filter(row => row.referenceOnly).length,
    hasReferenceProfile,
  };
};

const buildExtractedTenderGroups = (
  groups: TenderRequirementGroup[] | undefined,
  mode: "manual" | "crm",
  fallbackItems: TenderRequirementItem[] = []
): ExtractedTenderGroup[] => {
  const sourceGroups = groups && groups.length > 0
    ? groups
    : fallbackItems.length > 0
      ? [{
          tender_id: mode === "manual" ? "manual" : "crm",
          tender_title: mode === "manual" ? "Ручной ввод" : "Тендер",
          source: mode,
          items: fallbackItems,
          warnings: [],
          general_requirements: [],
        }]
      : [];

  const requestSeed = Date.now();

  return sourceGroups.map((group, groupIndex) => {
    const groupKey = mode === "crm"
      ? `crm:${group.tender_id || groupIndex}`
      : `manual:${requestSeed}:${groupIndex}`;

    return {
      ...group,
      group_key: groupKey,
      items: (group.items || []).map((item, itemIndex) => ({
        ...item,
        id: `${groupKey}:${item.id || itemIndex}`,
        tender_id: item.tender_id || group.tender_id,
        tender_title: item.tender_title || group.tender_title,
        source: item.source || group.source,
        source_label: item.source_label || group.source_label,
      })),
    };
  });
};

const mergeExtractedTenderGroups = (
  current: ExtractedTenderGroup[],
  incoming: ExtractedTenderGroup[],
  mode: "manual" | "crm"
): ExtractedTenderGroup[] => {
  if (mode === "manual") {
    return [...current, ...incoming];
  }

  const incomingKeys = new Set(incoming.map(group => group.group_key));
  return [
    ...current.filter(group => !incomingKeys.has(group.group_key)),
    ...incoming,
  ];
};

const getProductSelectionKey = (product: Product): string =>
  `${product.title}::${product.matched_requirement_id || "global"}`;

const attachProductContext = (
  product: Product,
  item: TenderRequirementItem | null
): Product => {
  if (!item) {
    return product;
  }

  return {
    ...product,
    matched_requirement_id: item.id,
    matched_requirement_name: item.position_name,
    matched_tender_id: item.tender_id,
    matched_tender_title: item.tender_title,
  };
};

const normalizeSearchResult = (data: any, fallbackQuery: string): SearchResult => ({
  query: data?.query || fallbackQuery,
  local_results: (data?.local_results || data?.results || []).map((product: any) =>
    normalizeSearchProduct(product, "local_db")
  ),
  ai_results: (data?.ai_results || []).map((product: any) =>
    normalizeSearchProduct(product, "ai_search")
  ),
  total: data?.total || 0,
  ai_error: typeof data?.ai_error === "string" ? data.ai_error : undefined,
  validation_error: typeof data?.validation_error === "string" ? data.validation_error : undefined,
  validation_summary: typeof data?.validation_summary === "string" ? data.validation_summary : undefined,
  reference_profile: data?.reference_profile && typeof data.reference_profile === "object"
    ? {
        ...data.reference_profile,
        specs: data.reference_profile.specs && typeof data.reference_profile.specs === "object"
          ? data.reference_profile.specs
          : {},
        metrics: data.reference_profile.metrics && typeof data.reference_profile.metrics === "object"
          ? data.reference_profile.metrics
          : {},
      }
    : undefined,
});

const normalizeStoredSearchResult = (data: any): StoredSearchResult | null => {
  if (!data || typeof data !== "object" || !data.item_id) {
    return null;
  }

  const normalized = normalizeSearchResult(data, String(data.query || ""));
  return {
    ...normalized,
    item_id: String(data.item_id),
    requirements: typeof data.requirements === "string" ? data.requirements : "",
    mode: (data.mode as MatchingSearchMode) || "both",
    ai_error: typeof data.ai_error === "string" ? data.ai_error : normalized.ai_error,
    saved_at: typeof data.saved_at === "string" ? data.saved_at : new Date().toISOString(),
  };
};

const getDisplayAiError = (
  result: SearchResult | StoredSearchResult | null | undefined,
  aiErrorMessage: string | null | undefined
): string | null => {
  const message = typeof aiErrorMessage === "string" ? aiErrorMessage.trim() : "";
  if (!message) {
    return null;
  }

  const normalizedMessage = message.toLowerCase();
  const hasVisibleResults = (result?.total || 0) > 0;
  const isQuotaFallbackMessage =
    normalizedMessage.includes("quota_exhausted")
    || normalizedMessage.includes("ai search skipped")
    || normalizedMessage.includes("returning local db results only")
    || normalizedMessage.includes("validation skipped")
    || normalizedMessage.includes("validation unavailable");

  if (hasVisibleResults && isQuotaFallbackMessage) {
    return null;
  }

  return message;
};

const normalizeRefinementJob = (
  data: any,
  fallbackQuery: string,
  fallbackMode?: MatchingSearchMode
): SearchRefinementJob | null => {
  if (!data || typeof data !== "object" || !data.job_id) {
    return null;
  }

  return {
    job_id: String(data.job_id),
    status: (data.status || "queued") as SearchRefinementJob["status"],
    stage: typeof data.stage === "string" ? data.stage : undefined,
    query: typeof data.query === "string" ? data.query : fallbackQuery,
    mode: (data.mode as MatchingSearchMode | undefined) || fallbackMode,
    result: data.result ? normalizeSearchResult(data.result, fallbackQuery) : undefined,
    aiError: typeof data?.result?.ai_error === "string" ? data.result.ai_error : null,
    error: typeof data.error === "string" ? data.error : undefined,
    retry_count: typeof data.retry_count === "number" ? data.retry_count : undefined,
    next_retry_at: typeof data.next_retry_at === "string" ? data.next_retry_at : null,
    created_at: typeof data.created_at === "string" ? data.created_at : undefined,
    updated_at: typeof data.updated_at === "string" ? data.updated_at : undefined,
  };
};

const isRefinementJobPending = (job: SearchRefinementJob | null | undefined): boolean =>
  job?.status === "queued" || job?.status === "running" || job?.status === "waiting_retry";

const getMatchColor = (score?: number): string => {
  if (!score) return "bg-gray-100 text-gray-600";
  if (score >= 90) return "bg-green-100 text-green-700";
  if (score >= 70) return "bg-blue-100 text-blue-700";
  if (score >= 50) return "bg-yellow-100 text-yellow-700";
  return "bg-red-100 text-red-700";
};

const getValidationBadge = (status?: Product["validation_status"]) => {
  if (status === "APPROVED") {
    return { label: "AI: подтвержден", className: "bg-emerald-100 text-emerald-700" };
  }
  if (status === "UNCERTAIN") {
    return { label: "AI: спорно", className: "bg-yellow-100 text-yellow-800" };
  }
  if (status === "REJECTED") {
    return { label: "AI: отклонен", className: "bg-red-100 text-red-700" };
  }
  return null;
};

const SourceBadge = ({ source }: { source?: string }) => {
  if (source === "local_db") {
    return (
      <span className="flex items-center gap-1 px-2 py-0.5 text-xs rounded-full bg-emerald-100 text-emerald-700 font-medium">
        <Database className="w-3 h-3" /> База
      </span>
    );
  }
  if (source === "ai_search") {
    return (
      <span className="flex items-center gap-1 px-2 py-0.5 text-xs rounded-full bg-purple-100 text-purple-700 font-medium">
        <Sparkles className="w-3 h-3" /> ИИ-поиск
      </span>
    );
  }
  return (
    <span className="flex items-center gap-1 px-2 py-0.5 text-xs rounded-full bg-gray-100 text-gray-600 font-medium">
      <Plus className="w-3 h-3" /> Вручную
    </span>
  );
};

const RequirementCharacteristicsTable = ({
  characteristics,
  compact = false,
}: {
  characteristics: RequirementCharacteristic[];
  compact?: boolean;
}) => {
  if (!characteristics.length) {
    return (
      <div className="text-xs text-gray-400 italic">
        Структурированные характеристики не извлечены.
      </div>
    );
  }

  return (
    <div className="overflow-x-auto rounded-lg border border-gray-200 bg-white">
      <table className="min-w-full divide-y divide-gray-200">
        <thead className="bg-gray-50">
          <tr>
            <th className="px-3 py-2 text-left text-[11px] font-semibold uppercase tracking-wide text-gray-500">
              Характеристика
            </th>
            <th className="px-3 py-2 text-left text-[11px] font-semibold uppercase tracking-wide text-gray-500">
              Значение
            </th>
          </tr>
        </thead>
        <tbody className="divide-y divide-gray-100">
          {characteristics.map((characteristic, index) => (
            <tr key={`${characteristic.name}-${characteristic.value}-${index}`} className="align-top">
              <td className={`px-3 py-2 text-xs font-medium text-gray-700 ${compact ? "w-44" : "w-56"}`}>
                {characteristic.name || "Параметр"}
              </td>
              <td className="px-3 py-2 text-xs text-gray-800">
                {characteristic.value || "—"}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
};

const AnalogComparisonMatrix = ({
  item,
  requirementsText,
  searchResult,
  catalogProducts,
}: {
  item: TenderRequirementItem | null;
  requirementsText: string;
  searchResult: SearchResult | null;
  catalogProducts?: Product[];
}) => {
  const { rows, products, referenceOnlyCount, hasReferenceProfile } = buildComparisonMatrix(
    item,
    searchResult,
    requirementsText,
    catalogProducts || []
  );

  if (!searchResult || rows.length === 0) {
    return null;
  }

  const referenceTitle = searchResult.reference_profile?.title || "";

  return (
    <div className="space-y-3">
      <div className="flex items-start justify-between gap-3">
        <div>
          <h3 className="text-sm font-semibold text-gray-800">Таблица сравнения по позиции</h3>
          <p className="text-xs text-gray-500 mt-1">
            {item?.position_name || searchResult.query}
          </p>
        </div>
        {referenceOnlyCount > 0 && (
          <div className="px-3 py-2 rounded-lg bg-amber-50 border border-amber-200 text-xs text-amber-900 max-w-sm">
            Подсвечены характеристики, которые найдены по торговой марке исходного материала, но отсутствуют в тексте ТЗ.
          </div>
        )}
      </div>

      <div className="overflow-x-auto rounded-xl border border-gray-200 bg-white">
        <table className="min-w-[820px] w-full divide-y divide-gray-200">
          <thead className="bg-gray-50">
            <tr>
              <th className="px-3 py-3 text-left text-[11px] font-semibold uppercase tracking-wide text-gray-500 min-w-52">
                Параметр
              </th>
              <th className="px-3 py-3 text-left text-[11px] font-semibold uppercase tracking-wide text-gray-500 min-w-44">
                Требование ТЗ
              </th>
              {hasReferenceProfile && (
                <th className="px-3 py-3 text-left text-[11px] font-semibold uppercase tracking-wide text-gray-500 min-w-52">
                  Референс по марке
                  <div className="mt-1 text-[11px] normal-case font-medium text-gray-700">
                    {referenceTitle}
                  </div>
                  {searchResult.reference_profile?.manufacturer && (
                    <div className="text-[10px] normal-case font-normal text-gray-500 mt-0.5">
                      {searchResult.reference_profile.manufacturer}
                    </div>
                  )}
                </th>
              )}
              {products.map((product, index) => (
                <th
                  key={`${product.title}-${product.source || "candidate"}-${index}`}
                  className="px-3 py-3 text-left text-[11px] font-semibold uppercase tracking-wide text-gray-500 min-w-56"
                >
                  <div className="flex items-center gap-2 flex-wrap">
                    <span>{product.source === "ai_search" ? "Аналог из интернета" : "Аналог из базы"}</span>
                    <SourceBadge source={product.source} />
                  </div>
                  <div className="mt-1 text-[11px] normal-case font-medium text-gray-700">
                    {product.title}
                  </div>
                  {product.manufacturer && (
                    <div className="text-[10px] normal-case font-normal text-gray-500 mt-0.5">
                      {product.manufacturer}
                    </div>
                  )}
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100">
            {rows.map(row => (
              <tr
                key={row.key}
                className={row.referenceOnly ? "bg-amber-50/70" : "bg-white"}
              >
                <td className="px-3 py-2.5 text-xs font-medium text-gray-800 align-top">
                  <div className="flex flex-col gap-1">
                    <span>{row.label}</span>
                    {row.referenceOnly && (
                      <span className="inline-flex w-fit px-2 py-0.5 rounded-full bg-amber-100 text-amber-800 text-[10px] font-medium">
                        Найдено по торговой марке, не указано в ТЗ
                      </span>
                    )}
                  </div>
                </td>
                <td className="px-3 py-2.5 text-xs text-gray-800 align-top">
                  {row.requiredValue || <span className="text-gray-400">—</span>}
                </td>
                {hasReferenceProfile && (
                  <td className={`px-3 py-2.5 text-xs align-top ${row.referenceOnly ? "text-amber-900 font-medium" : "text-gray-800"}`}>
                    {row.referenceValue || <span className="text-gray-400">—</span>}
                  </td>
                )}
                {row.productCells.map((cell, index) => (
                  <td
                    key={`${row.key}-${index}`}
                    className={`px-3 py-2.5 text-xs align-top ${
                      cell.mismatch
                        ? "bg-red-50 text-red-800 font-medium"
                        : cell.missing
                          ? "bg-amber-50 text-amber-800"
                          : "text-gray-800"
                    }`}
                  >
                    {cell.value || (
                      <span className={cell.missing ? "text-amber-700" : "text-gray-400"}>
                        {cell.missing ? "не подтверждено" : "не указано"}
                      </span>
                    )}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
};

// ── Карточка товара ───────────────────────────────────────────────────────────
const ProductCard = ({
  product,
  onSelect,
  onDelete,
  selected,
}: {
  product: Product;
  onSelect?: (p: Product) => void;
  onDelete?: (id: number) => void;
  selected?: boolean;
}) => {
  const [expanded, setExpanded] = useState(false);
  const specEntries = Object.entries(product.specs || {}).slice(0, expanded ? 999 : 4);
  const validationBadge = getValidationBadge(product.validation_status);
  const externalUrl = normalizeExternalUrl(product.url);
  const sourceUrl = normalizeExternalUrl(product.source_url);
  const unresolvedInternetUrl = product.source === "ai_search" && !externalUrl && product.url_status === "not_found";
  const unresolvedInternetSource = product.source === "ai_search" && !sourceUrl && product.source_url_status === "not_found";

  return (
    <div
      className={`
        min-w-0 h-full overflow-hidden rounded-xl border transition-all duration-200 flex flex-col
        ${selected
          ? "border-blue-400 bg-blue-50 shadow-md"
          : "border-gray-200 bg-white hover:border-blue-200 hover:shadow-sm"
        }
      `}
    >
      {/* Заголовок карточки */}
      <div className="flex min-h-0 flex-1 flex-col p-4">
        <div className="mb-3 flex items-start justify-between gap-2">
          <div className="flex-1 min-w-0">
            <div className="mb-1.5 flex flex-wrap items-center gap-1.5">
              <SourceBadge source={product.source} />
              {product.match_score != null && (
                <span className={`max-w-full px-2 py-0.5 text-xs rounded-full font-semibold ${getMatchColor(product.match_score)}`}>
                  score {Math.max(0, Math.min(100, Math.round(product.match_score)))}
                </span>
              )}
              {validationBadge && (
                <span className={`max-w-full px-2 py-0.5 text-xs rounded-full font-semibold ${validationBadge.className}`}>
                  {validationBadge.label}
                </span>
              )}
              {product.category && (
                <span
                  className="max-w-full truncate px-2 py-0.5 text-xs rounded-full bg-gray-100 text-gray-600"
                  title={product.category}
                >
                  {product.category}
                </span>
              )}
            </div>
            <h3 className="text-sm font-semibold text-gray-900 leading-snug break-words line-clamp-3" title={product.title}>
              {product.title}
            </h3>
            {product.manufacturer && (
              <p className="mt-0.5 text-xs text-gray-500 break-words line-clamp-2" title={product.manufacturer}>
                {product.manufacturer}
              </p>
            )}
          </div>
          <div className="flex items-center gap-1 flex-shrink-0">
            {externalUrl && (
              <a
                href={externalUrl}
                target="_blank"
                rel="noopener noreferrer"
                className="p-1.5 text-gray-400 hover:text-blue-600 rounded-lg hover:bg-blue-50 transition-colors"
                title="Открыть на сайте"
              >
                <ExternalLink className="w-3.5 h-3.5" />
              </a>
            )}
            {sourceUrl && (
              <a
                href={sourceUrl}
                target="_blank"
                rel="noopener noreferrer"
                className="p-1.5 text-gray-400 hover:text-emerald-600 rounded-lg hover:bg-emerald-50 transition-colors"
                title="Открыть подтверждающий источник"
              >
                <Globe className="w-3.5 h-3.5" />
              </a>
            )}
            {onDelete && product.id && (
              <button
                onClick={() => onDelete(product.id!)}
                className="p-1.5 text-gray-400 hover:text-red-500 rounded-lg hover:bg-red-50 transition-colors"
                title="Удалить"
              >
                <Trash2 className="w-3.5 h-3.5" />
              </button>
            )}
          </div>
        </div>

        {/* Цена */}
        {product.price != null && (
          <div className="mb-2 text-base font-bold text-gray-900 whitespace-nowrap">
            {formatPrice(product.price, product.price_unit)}
          </div>
        )}

        {/* Описание / причина совпадения */}
            {product.description && (
          <p className="mb-2 rounded-lg bg-gray-50 p-2 text-xs leading-relaxed text-gray-600 break-words">
            {product.description}
          </p>
        )}

        {unresolvedInternetUrl && (
          <div className="mb-2 rounded-lg border border-amber-200 bg-amber-50 p-2 text-xs leading-relaxed text-amber-900">
            <div className="font-semibold">Ссылка на карточку не подтверждена</div>
            <div className="mt-1 break-words">
              {product.url_note || "Кандидат найден ИИ по интернет-источникам, но прямую страницу товара подтвердить не удалось."}
            </div>
          </div>
        )}

        {product.source === "ai_search" && (product.source_title || sourceUrl || unresolvedInternetSource) && (
          <div className="mb-2 rounded-lg border border-sky-200 bg-sky-50 p-2 text-xs leading-relaxed text-sky-950">
            <div className="flex items-center gap-1.5 font-semibold">
              <BookOpen className="h-3.5 w-3.5" />
              Подтверждающий источник
            </div>
            {product.source_title && (
              <div className="mt-1 break-words">
                {product.source_title}
              </div>
            )}
            {sourceUrl ? (
              <a
                href={sourceUrl}
                target="_blank"
                rel="noopener noreferrer"
                className="mt-1 inline-flex items-center gap-1 text-sky-700 hover:text-sky-900 break-all"
              >
                <Globe className="h-3.5 w-3.5" />
                {sourceUrl}
              </a>
            ) : (
              <div className="mt-1 text-sky-900 break-words">
                {product.source_url_note || "Подтверждающий источник не удалось открыть повторно."}
              </div>
            )}
          </div>
        )}

        {product.validation_comment && (
          <p
            className={`mb-2 rounded-lg p-2 text-xs leading-relaxed break-words ${
              product.validation_status === "REJECTED"
                ? "text-red-800 bg-red-50"
                : product.validation_status === "UNCERTAIN"
                  ? "text-amber-900 bg-amber-50"
                  : "text-emerald-800 bg-emerald-50"
            }`}
          >
            AI-проверка: {product.validation_comment}
          </p>
        )}

        {/* Характеристики */}
        {specEntries.length > 0 && (
          <div className="mt-2 min-w-0">
            <div className="grid grid-cols-1 gap-2">
              {specEntries.map(([key, value]) => (
                <div key={key} className="rounded-lg bg-gray-50 px-2.5 py-2">
                  <span className="block text-[10px] leading-4 text-gray-500 break-words" title={key}>
                    {key}
                  </span>
                  <span className="mt-1 block text-xs font-medium leading-5 text-gray-800 break-words" title={String(value)}>
                    {String(value)}
                  </span>
                </div>
              ))}
            </div>
            {Object.keys(product.specs || {}).length > 4 && (
              <button
                onClick={() => setExpanded(e => !e)}
                className="mt-2 flex items-center gap-1 text-xs text-blue-600 hover:text-blue-800"
              >
                {expanded ? (
                  <><ChevronUp className="w-3 h-3" /> Свернуть</>
                ) : (
                  <><ChevronDown className="w-3 h-3" /> Ещё {Object.keys(product.specs).length - 4} характеристик</>
                )}
              </button>
            )}
          </div>
        )}
      </div>

      {/* Кнопка выбрать */}
      {onSelect && (
        <div className="px-4 pb-4">
          <button
            onClick={() => onSelect(product)}
            className={`
              w-full py-2 px-3 text-sm font-medium rounded-lg transition-colors
              ${selected
                ? "bg-blue-600 text-white hover:bg-blue-700"
                : "border border-blue-300 text-blue-700 hover:bg-blue-50"
              }
            `}
          >
            {selected ? (
              <span className="flex items-center justify-center gap-1.5">
                <CheckCircle className="w-4 h-4" /> Выбрано в КП
              </span>
            ) : (
              <span className="flex items-center justify-center gap-1.5">
                <Plus className="w-4 h-4" /> Добавить в КП
              </span>
            )}
          </button>
        </div>
      )}
    </div>
  );
};

// ── Форма добавления товара вручную ──────────────────────────────────────────
const AddManualForm = ({
  onAdd,
  onClose,
}: {
  onAdd: (p: Partial<Product>) => void;
  onClose: () => void;
}) => {
  const [form, setForm] = useState({
    title: "", category: "", material_type: "",
    price: "", url: "", description: "",
  });

  const handleSubmit = () => {
    if (!form.title.trim()) return;
    onAdd({
      ...form,
      price: form.price ? parseFloat(form.price) : undefined,
      url: normalizeExternalUrl(form.url) || undefined,
      specs: {},
      source: "manual",
    });
    onClose();
  };

  return (
    <div className="bg-white rounded-xl border border-gray-200 p-5">
      <div className="flex items-center justify-between mb-4">
        <h3 className="text-sm font-semibold text-gray-800">Добавить материал вручную</h3>
        <button onClick={onClose} className="text-gray-400 hover:text-gray-600">
          <X className="w-4 h-4" />
        </button>
      </div>
      <div className="grid grid-cols-2 gap-3">
        {[
          { key: "title", label: "Название *", placeholder: "Техноэласт ЭПП 4,0" },
          { key: "category", label: "Категория", placeholder: "Рулонная гидроизоляция" },
          { key: "material_type", label: "Тип материала", placeholder: "Наплавляемый" },
          { key: "price", label: "Цена (₽)", placeholder: "3200" },
          { key: "url", label: "Ссылка", placeholder: "https://..." },
        ].map(field => (
          <div key={field.key} className={field.key === "url" ? "col-span-2" : ""}>
            <label className="block text-xs text-gray-600 mb-1">{field.label}</label>
            <input
              type={field.key === "price" ? "number" : "text"}
              value={(form as any)[field.key]}
              onChange={e => setForm(prev => ({ ...prev, [field.key]: e.target.value }))}
              placeholder={field.placeholder}
              className="w-full px-3 py-2 text-sm border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
            />
          </div>
        ))}
        <div className="col-span-2">
          <label className="block text-xs text-gray-600 mb-1">Описание</label>
          <textarea
            value={form.description}
            onChange={e => setForm(prev => ({ ...prev, description: e.target.value }))}
            placeholder="Краткое описание материала..."
            rows={2}
            className="w-full px-3 py-2 text-sm border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 resize-none"
          />
        </div>
      </div>
      <div className="flex gap-2 mt-4">
        <button
          onClick={handleSubmit}
          className="flex-1 py-2 bg-blue-600 text-white text-sm font-medium rounded-lg hover:bg-blue-700"
        >
          Добавить
        </button>
        <button
          onClick={onClose}
          className="px-4 py-2 border border-gray-200 text-gray-600 text-sm rounded-lg hover:bg-gray-50"
        >
          Отмена
        </button>
      </div>
    </div>
  );
};

// ── ГЛАВНЫЙ КОМПОНЕНТ ─────────────────────────────────────────────────────────
export default function ProductMatching() {
  const [searchQuery, setSearchQuery]         = useState("");
  const [requirements, setRequirements]       = useState("");
  const [showRequirements, setShowRequirements] = useState(false);
  const [searchResult, setSearchResult]       = useState<SearchResult | null>(null);
  const [isSearching, setIsSearching]         = useState(false);
  const [searchMode, setSearchMode]           = useState<MatchingSearchMode>("both");
  const [selectedProducts, setSelectedProducts] = useState<Product[]>([]);
  const [allProducts, setAllProducts]         = useState<Product[]>([]);
  const [isLoadingAll, setIsLoadingAll]       = useState(false);
  const [isRefreshing, setIsRefreshing]       = useState(false);
  const [showAddForm, setShowAddForm]         = useState(false);
  const [activeTab, setActiveTab]             = useState<"search"|"extraction"|"catalog"|"selected">("search");
  const [error, setError]                     = useState<string | null>(null);
  const [aiError, setAiError]                 = useState<string | null>(null);

  // Состояние для извлечения ТЗ
  const [manualText, setManualText] = useState("");
  const [extractedTenderGroups, setExtractedTenderGroups] = useState<ExtractedTenderGroup[]>([]);
  const [selectedExtractedItemIds, setSelectedExtractedItemIds] = useState<string[]>([]);
  const [isExportingParsedTz, setIsExportingParsedTz] = useState(false);
  const [searchQueue, setSearchQueue] = useState<TenderRequirementItem[]>([]);
  const [activeSearchQueueItemId, setActiveSearchQueueItemId] = useState<string | null>(null);
  const [searchResultsByItemId, setSearchResultsByItemId] = useState<Record<string, StoredSearchResult>>({});
  const [searchStatusesByItemId, setSearchStatusesByItemId] = useState<Record<string, SearchStatus>>({});
  const [searchErrorsByItemId, setSearchErrorsByItemId] = useState<Record<string, string>>({});
  const [searchRefinementJobsByItemId, setSearchRefinementJobsByItemId] = useState<Record<string, SearchRefinementJob>>({});
  const [standaloneRefinementJob, setStandaloneRefinementJob] = useState<SearchRefinementJob | null>(null);
  const [isBatchSearching, setIsBatchSearching] = useState(false);
  const [hasRestoredState, setHasRestoredState] = useState(false);
  const [isExtracting, setIsExtracting] = useState(false);
  const [refinementClockMs, setRefinementClockMs] = useState(() => Date.now());
  const [selectedTendersForExtraction, setSelectedTendersForExtraction] = useState<Tender[]>([]);

  // Загрузка всего каталога
  const loadAllProducts = useCallback(async () => {
    setIsLoadingAll(true);
    try {
      const res = await fetch("/api/products?limit=100");
      const data = await res.json();
      setAllProducts((data.products || []).map((p: Product) => normalizeSearchProduct(p, "local_db")));
    } catch {
      setError("Не удалось загрузить каталог");
    } finally {
      setIsLoadingAll(false);
    }
  }, []);

  useEffect(() => {
    loadAllProducts();
  }, [loadAllProducts]);

  useEffect(() => {
    try {
      const raw = localStorage.getItem(PRODUCT_MATCHING_STORAGE_KEY);
      if (!raw) {
        setHasRestoredState(true);
        return;
      }

      const saved = JSON.parse(raw) as Partial<ProductMatchingPersistedState>;
      const savedVersion = typeof saved.version === "number" ? saved.version : 0;
      if (Array.isArray(saved.extractedTenderGroups)) {
        setExtractedTenderGroups(saved.extractedTenderGroups);
      }
      if (Array.isArray(saved.selectedExtractedItemIds)) {
        setSelectedExtractedItemIds(saved.selectedExtractedItemIds);
      }
      if (Array.isArray(saved.searchQueue)) {
        setSearchQueue(saved.searchQueue);
      }
      if (typeof saved.activeSearchQueueItemId === "string" || saved.activeSearchQueueItemId === null) {
        setActiveSearchQueueItemId(saved.activeSearchQueueItemId ?? null);
      }
      if (saved.searchResultsByItemId && typeof saved.searchResultsByItemId === "object") {
        const normalizedEntries = Object.entries(saved.searchResultsByItemId)
          .map(([key, value]) => [key, normalizeStoredSearchResult(value)] as const)
          .filter((entry): entry is readonly [string, StoredSearchResult] => Boolean(entry[1]));
        setSearchResultsByItemId(Object.fromEntries(normalizedEntries));
      }
      if (Array.isArray(saved.selectedProducts)) {
        setSelectedProducts(
          saved.selectedProducts.map(product => normalizeSearchProduct(product, product?.source || "local_db"))
        );
      }
      if (savedVersion < PRODUCT_MATCHING_STORAGE_VERSION) {
        logger.info("Product matching state version is outdated; cached search results were migrated.");
      }
    } catch (err) {
      logger.error("Failed to restore product matching state", err);
    } finally {
      setHasRestoredState(true);
    }
  }, []);

  useEffect(() => {
    if (!hasRestoredState) {
      return;
    }

    const stateToPersist: ProductMatchingPersistedState = {
      version: PRODUCT_MATCHING_STORAGE_VERSION,
      extractedTenderGroups,
      selectedExtractedItemIds,
      searchQueue,
      activeSearchQueueItemId,
      searchResultsByItemId,
      selectedProducts,
    };

    try {
      localStorage.setItem(PRODUCT_MATCHING_STORAGE_KEY, JSON.stringify(stateToPersist));
    } catch (err) {
      logger.error("Failed to persist product matching state", err);
    }
  }, [
    activeSearchQueueItemId,
    extractedTenderGroups,
    hasRestoredState,
    searchQueue,
    searchResultsByItemId,
    selectedExtractedItemIds,
    selectedProducts,
  ]);

  // Поиск аналогов
  const requestSearchResult = async (
    query: string,
    requirementsText: string,
    mode: MatchingSearchMode
  ): Promise<{
    result: SearchResult;
    aiErrorMessage: string | null;
    refinementJob: SearchRefinementJob | null;
    refinementPending: boolean;
  }> => {
    if (mode === "local") {
      const res = await fetch("/api/products/search", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          query,
          requirements: requirementsText || undefined,
          limit: 10,
        }),
      });
      const data = await res.json();

      if (!res.ok) {
        throw new Error(data.detail || "Ошибка поиска. Проверьте подключение к серверу.");
      }

      return {
        result: normalizeSearchResult(data, query),
        aiErrorMessage: data.ai_error || null,
        refinementJob: null,
        refinementPending: false,
      };
    }

    const res = await fetch("/api/products/search-ai", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        query,
        requirements: requirementsText || undefined,
        max_results: 5,
        mode,
        background_refine: true,
      }),
    });

    const data = await res.json();

    if (!res.ok) {
      throw new Error(data.detail || "Ошибка поиска. Проверьте подключение к серверу.");
    }

    const refinementJob = normalizeRefinementJob(data.refinement_job, data.query || query, mode);

    return {
      result: normalizeSearchResult(data, query),
      aiErrorMessage: data.ai_error || null,
      refinementJob,
      refinementPending: Boolean(data.refinement_pending && refinementJob),
    };
  };

  const fetchRefinementJob = async (
    jobId: string,
    fallbackQuery: string,
    fallbackMode?: MatchingSearchMode
  ): Promise<SearchRefinementJob> => {
    const res = await fetch(`/api/products/search-ai/jobs/${jobId}`);
    const data = await res.json();

    if (!res.ok) {
      if (res.status === 404) {
        return normalizeRefinementJob(
          {
            job_id: jobId,
            status: "error",
            stage: "Фоновая дообработка недоступна",
            error:
              typeof data?.detail === "string" && data.detail.trim()
                ? data.detail
                : "Фоновая задача не найдена. Вероятно, сервер был перезапущен; сохранен предварительный результат.",
          },
          fallbackQuery,
          fallbackMode
        ) as SearchRefinementJob;
      }
      throw new Error(data.detail || "Не удалось получить статус фонового поиска.");
    }

    const job = normalizeRefinementJob(data, fallbackQuery, fallbackMode);
    if (!job) {
      throw new Error("Сервер вернул некорректный статус фонового поиска.");
    }

    return job;
  };

  const saveSearchResultForItem = (
    item: TenderRequirementItem,
    result: SearchResult,
    requirementsText: string,
    mode: MatchingSearchMode,
    aiErrorMessage: string | null
  ): StoredSearchResult => ({
    ...result,
    item_id: item.id,
    requirements: requirementsText,
    mode,
    ai_error: getDisplayAiError(result, aiErrorMessage) || undefined,
    saved_at: new Date().toISOString(),
  });

  const runSearchForItem = async (
    item: TenderRequirementItem,
    options?: {
      activate?: boolean;
      queryOverride?: string;
      requirementsOverride?: string;
    }
  ): Promise<StoredSearchResult> => {
    const query = (options?.queryOverride ?? item.search_query).trim();
    const requirementsText = options?.requirementsOverride ?? buildRequirementText(item);

    if (!query) {
      throw new Error("Поисковый запрос пуст");
    }

    setIsSearching(true);
    setStandaloneRefinementJob(null);
    setSearchStatusesByItemId(prev => ({ ...prev, [item.id]: "searching" }));
    setSearchErrorsByItemId(prev => {
      const next = { ...prev };
      delete next[item.id];
      return next;
    });

    if (options?.activate) {
      setError(null);
      setAiError(null);
      setSearchResult(null);
      setSearchQuery(query);
      setRequirements(requirementsText);
      setShowRequirements(true);
      setActiveTab("search");
      setActiveSearchQueueItemId(item.id);
    }

    try {
      const { result, aiErrorMessage, refinementJob, refinementPending } = await requestSearchResult(
        query,
        requirementsText,
        searchMode
      );
      const storedResult = saveSearchResultForItem(item, result, requirementsText, searchMode, aiErrorMessage);

      setSearchResultsByItemId(prev => ({ ...prev, [item.id]: storedResult }));
      setSearchStatusesByItemId(prev => ({ ...prev, [item.id]: refinementPending ? "refining" : "done" }));
      setSearchRefinementJobsByItemId(prev => {
        const next = { ...prev };
        if (refinementPending && refinementJob) {
          next[item.id] = refinementJob;
        } else {
          delete next[item.id];
        }
        return next;
      });

      if (options?.activate || activeSearchQueueItemId === item.id) {
        setSearchResult(storedResult);
        setAiError(getDisplayAiError(storedResult, aiErrorMessage));
      }

      return storedResult;
    } catch (err: any) {
      const message = err?.message || "Ошибка поиска. Проверьте подключение к серверу.";
      setSearchRefinementJobsByItemId(prev => {
        const next = { ...prev };
        delete next[item.id];
        return next;
      });
      setSearchStatusesByItemId(prev => ({ ...prev, [item.id]: "error" }));
      setSearchErrorsByItemId(prev => ({ ...prev, [item.id]: message }));

      if (options?.activate || activeSearchQueueItemId === item.id) {
        setError(message);
      }

      throw err;
    } finally {
      setIsSearching(false);
    }
  };

  const runBatchSearch = async (items: TenderRequirementItem[]) => {
    if (items.length === 0) {
      return;
    }

    setIsBatchSearching(true);
    setError(null);
    setAiError(null);

    const [firstItem] = items;
    setSearchQueue(items);
    setActiveSearchQueueItemId(firstItem.id);
    setActiveTab("search");
    setSearchQuery(firstItem.search_query);
    setRequirements(buildRequirementText(firstItem));
    setShowRequirements(true);

    try {
      let firstStoredResult: StoredSearchResult | null = null;

      for (const item of items) {
        try {
          const storedResult = await runSearchForItem(item, { activate: true });
          if (!firstStoredResult) {
            firstStoredResult = storedResult;
          }
        } catch (err) {
          logger.error("Batch analog search failed for item", { itemId: item.id, error: err });
        }
      }

      setActiveSearchQueueItemId(firstItem.id);
      if (firstStoredResult) {
        setSearchResult(firstStoredResult);
        setAiError(getDisplayAiError(firstStoredResult, firstStoredResult.ai_error || null));
      } else {
        setSearchResult(null);
      }
    } finally {
      setIsBatchSearching(false);
    }
  };

  const handleSearch = async () => {
    if (!searchQuery.trim()) return;

    if (selectedSearchQueueItem) {
      try {
        await runSearchForItem(selectedSearchQueueItem, {
          activate: true,
          queryOverride: searchQuery,
          requirementsOverride: requirements,
        });
      } catch {
        // Error state is already reflected in component state.
      }
      return;
    }

    setIsSearching(true);
    setError(null);
    setAiError(null);
    setSearchResult(null);
    setStandaloneRefinementJob(null);

    try {
      const { result, aiErrorMessage, refinementJob, refinementPending } = await requestSearchResult(
        searchQuery.trim(),
        requirements,
        searchMode
      );
      setSearchResult(result);
      setAiError(getDisplayAiError(result, aiErrorMessage));
      setStandaloneRefinementJob(refinementPending ? refinementJob : null);
    } catch (err: any) {
      setStandaloneRefinementJob(null);
      setError(err?.message || "Ошибка поиска. Проверьте подключение к серверу.");
    } finally {
      setIsSearching(false);
    }
  };

  // Обновление каталога с сайта
  const handleRefreshCatalog = async () => {
    setIsRefreshing(true);
    try {
      await fetch("/api/products/refresh-catalog", { method: "POST" });
      setTimeout(() => {
        loadAllProducts();
        setIsRefreshing(false);
      }, 3000);
    } catch {
      setIsRefreshing(false);
    }
  };

  // Добавить материал вручную
  const handleAddManual = async (product: Partial<Product>) => {
    try {
      await fetch("/api/products", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(product),
      });
      await loadAllProducts();
    } catch {
      setError("Не удалось добавить материал");
    }
  };

  // Удалить из каталога
  const handleDelete = async (id: number) => {
    try {
      await fetch(`/api/products/${id}`, { method: "DELETE" });
      setAllProducts(prev => prev.filter(p => p.id !== id));
    } catch {
      setError("Не удалось удалить");
    }
  };

  // Выбрать / снять выбор товара в КП
  const toggleSelect = (product: Product) => {
    setSelectedProducts(prev => {
      const selectionKey = getProductSelectionKey(product);
      const exists = prev.some(p => getProductSelectionKey(p) === selectionKey);
      return exists
        ? prev.filter(p => getProductSelectionKey(p) !== selectionKey)
        : [...prev, product];
    });
  };

  const isSelected = (product: Product) =>
    selectedProducts.some(p => getProductSelectionKey(p) === getProductSelectionKey(product));

  const totalResults =
    (searchResult?.local_results?.length || 0) +
    (searchResult?.ai_results?.length || 0);
  const totalExtractedItems = extractedTenderGroups.reduce((sum, group) => sum + group.items.length, 0);
  const selectedSearchQueueItem =
    searchQueue.find(item => item.id === activeSearchQueueItemId) ||
    searchQueue[0] ||
    null;
  const activeRefinementJob = selectedSearchQueueItem
    ? searchRefinementJobsByItemId[selectedSearchQueueItem.id] || null
    : standaloneRefinementJob;
  const refinementPending = isRefinementJobPending(activeRefinementJob);
  const hasWaitingRetryJobs =
    standaloneRefinementJob?.status === "waiting_retry" ||
    Object.values(searchRefinementJobsByItemId).some(job => job?.status === "waiting_retry");
  const activeRetryCountdown = formatRetryCountdown(activeRefinementJob, refinementClockMs);

  useEffect(() => {
    if (!hasWaitingRetryJobs) {
      return;
    }

    setRefinementClockMs(Date.now());
    const timer = window.setInterval(() => {
      setRefinementClockMs(Date.now());
    }, 1000);

    return () => {
      window.clearInterval(timer);
    };
  }, [hasWaitingRetryJobs]);

  // Загрузка тендеров для извлечения
  const loadTendersForExtraction = useCallback(async () => {
    try {
      const tenders = await getSelectedTendersForMatching();
      setSelectedTendersForExtraction(tenders);
    } catch (err) {
      logger.error("Failed to load tenders for matching", err);
    }
  }, []);

  useEffect(() => {
    if (activeTab === "extraction") {
      loadTendersForExtraction();
    }
  }, [activeTab, loadTendersForExtraction]);

  useEffect(() => {
    if (!hasRestoredState) {
      return;
    }

    const validItemIds = new Set(
      extractedTenderGroups.flatMap(group => group.items.map(item => item.id))
    );

    setSelectedExtractedItemIds(prev => prev.filter(id => validItemIds.has(id)));
    setSearchQueue(prev => prev.filter(item => validItemIds.has(item.id)));
    setActiveSearchQueueItemId(prev => (prev && validItemIds.has(prev) ? prev : null));
    setSearchResultsByItemId(prev =>
      Object.fromEntries(
        Object.entries(prev).filter(([itemId]) => validItemIds.has(itemId))
      )
    );
    setSearchStatusesByItemId(prev =>
      Object.fromEntries(
        Object.entries(prev).filter(([itemId]) => validItemIds.has(itemId))
      ) as Record<string, SearchStatus>
    );
    setSearchErrorsByItemId(prev =>
      Object.fromEntries(
        Object.entries(prev).filter(([itemId]) => validItemIds.has(itemId))
      )
    );
    setSearchRefinementJobsByItemId(prev =>
      Object.fromEntries(
        Object.entries(prev).filter(([itemId]) => validItemIds.has(itemId))
      )
    );
  }, [extractedTenderGroups, hasRestoredState]);

  useEffect(() => {
    if (!selectedSearchQueueItem) {
      return;
    }

    const storedResult = searchResultsByItemId[selectedSearchQueueItem.id];
    setSearchQuery(selectedSearchQueueItem.search_query);
    setRequirements(buildRequirementText(selectedSearchQueueItem));
    setShowRequirements(true);

    if (storedResult) {
      setSearchResult(storedResult);
      setAiError(getDisplayAiError(storedResult, storedResult.ai_error || null));
      return;
    }

    setSearchResult(null);
    setAiError(null);
  }, [selectedSearchQueueItem, searchResultsByItemId]);

  useEffect(() => {
    const pendingItemJobs = Object.entries(searchRefinementJobsByItemId).filter(([, job]) =>
      isRefinementJobPending(job)
    );
    const hasStandalonePending = isRefinementJobPending(standaloneRefinementJob);

    if (!hasStandalonePending && pendingItemJobs.length === 0) {
      return;
    }

    let cancelled = false;

    const applyCompletedItemJob = (itemId: string, job: SearchRefinementJob) => {
      const nowIso = new Date().toISOString();

      setSearchRefinementJobsByItemId(prev => {
        const current = prev[itemId];
        if (!current || current.job_id !== job.job_id) {
          return prev;
        }
        const next = { ...prev };
        delete next[itemId];
        return next;
      });

      if (job.status === "completed" && job.result) {
        const completedResult = job.result;
        setSearchResultsByItemId(prev => {
          const previous = prev[itemId];
          const mode = previous?.mode || job.mode || "both";
          return {
            ...prev,
            [itemId]: {
              ...(previous || {
                item_id: itemId,
                requirements: "",
                mode,
                saved_at: nowIso,
                query: completedResult.query,
                local_results: [],
                ai_results: [],
                total: 0,
              }),
              ...completedResult,
              item_id: itemId,
              requirements: previous?.requirements || "",
              mode,
              ai_error: job.aiError || undefined,
              saved_at: nowIso,
            },
          };
        });
        setSearchStatusesByItemId(prev => ({ ...prev, [itemId]: "done" }));

        if (activeSearchQueueItemId === itemId) {
          setSearchResult(completedResult);
          setAiError(getDisplayAiError(completedResult, job.aiError || null));
        }
        return;
      }

      const message = job.error || "Фоновая дообработка не выполнена.";
      const hasPreview = Boolean(searchResultsByItemId[itemId]);
      setSearchStatusesByItemId(prev => ({ ...prev, [itemId]: hasPreview ? "done" : "error" }));
      if (hasPreview) {
        setSearchResultsByItemId(prev => {
          const previous = prev[itemId];
          if (!previous) {
            return prev;
          }
          return {
            ...prev,
            [itemId]: {
              ...previous,
              ai_error: getDisplayAiError(previous, message) || undefined,
              saved_at: nowIso,
            },
          };
        });
      } else {
        setSearchErrorsByItemId(prev => ({ ...prev, [itemId]: message }));
      }

      if (activeSearchQueueItemId === itemId) {
        if (hasPreview) {
          setAiError(getDisplayAiError(searchResultsByItemId[itemId], message));
        } else {
          setError(message);
        }
      }
    };

    const pollOnce = async () => {
      if (hasStandalonePending && standaloneRefinementJob) {
        try {
          const nextJob = await fetchRefinementJob(
            standaloneRefinementJob.job_id,
            standaloneRefinementJob.query || searchQuery.trim(),
            standaloneRefinementJob.mode
          );
          if (!cancelled) {
            if (isRefinementJobPending(nextJob)) {
              setStandaloneRefinementJob(prev =>
                prev?.job_id === nextJob.job_id ? nextJob : prev
              );
            } else if (nextJob.status === "completed" && nextJob.result) {
              setStandaloneRefinementJob(null);
              setSearchResult(nextJob.result);
              setAiError(getDisplayAiError(nextJob.result, nextJob.aiError || null));
            } else {
              setStandaloneRefinementJob(null);
              setAiError(getDisplayAiError(searchResult, nextJob.error || "Фоновая дообработка не выполнена."));
            }
          }
        } catch (err) {
          if (!cancelled) {
            logger.error("Failed to poll standalone refinement job", err);
            setStandaloneRefinementJob(null);
            setAiError(
              getDisplayAiError(
                searchResult,
                err instanceof Error ? err.message : "Фоновая дообработка не выполнена."
              )
            );
          }
        }
      }

      await Promise.all(
        pendingItemJobs.map(async ([itemId, job]) => {
          try {
            const nextJob = await fetchRefinementJob(
              job.job_id,
              job.query || searchResultsByItemId[itemId]?.query || "",
              job.mode
            );
            if (cancelled) {
              return;
            }
            if (isRefinementJobPending(nextJob)) {
              setSearchRefinementJobsByItemId(prev => {
                const current = prev[itemId];
                if (!current || current.job_id !== nextJob.job_id) {
                  return prev;
                }
                return {
                  ...prev,
                  [itemId]: nextJob,
                };
              });
              return;
            }
            applyCompletedItemJob(itemId, nextJob);
          } catch (err) {
            if (!cancelled) {
              logger.error("Failed to poll refinement job for item", { itemId, error: err });
              applyCompletedItemJob(itemId, {
                ...job,
                status: "error",
                error: err instanceof Error ? err.message : "Фоновая дообработка не выполнена.",
              });
            }
          }
        })
      );
    };

    void pollOnce();
    const timer = window.setInterval(() => {
      void pollOnce();
    }, 4000);

    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [
    activeSearchQueueItemId,
    searchQuery,
    searchRefinementJobsByItemId,
    searchResultsByItemId,
    searchResult,
    standaloneRefinementJob,
  ]);

  const applyItemToSearch = (item: TenderRequirementItem) => {
    setSearchQuery(item.search_query);
    setRequirements(buildRequirementText(item));
    setShowRequirements(true);
    setActiveTab("search");
  };

  const getSelectedItemsForGroup = (group: ExtractedTenderGroup) =>
    group.items.filter(item => selectedExtractedItemIds.includes(item.id));

  const isGroupFullySelected = (group: ExtractedTenderGroup) =>
    group.items.length > 0 && group.items.every(item => selectedExtractedItemIds.includes(item.id));

  const toggleExtractedItemSelection = (itemId: string) => {
    setSelectedExtractedItemIds(prev =>
      prev.includes(itemId)
        ? prev.filter(id => id !== itemId)
        : [...prev, itemId]
    );
  };

  const toggleGroupSelection = (group: ExtractedTenderGroup) => {
    const groupItemIds = group.items.map(item => item.id);
    const allSelected = groupItemIds.every(id => selectedExtractedItemIds.includes(id));

    setSelectedExtractedItemIds(prev => {
      if (allSelected) {
        return prev.filter(id => !groupItemIds.includes(id));
      }

      return [...new Set([...prev, ...groupItemIds])];
    });
  };

  const handleExtractFromText = async () => {
    if (!manualText.trim()) return;
    setIsExtracting(true);
    setError(null);
    try {
      const res = await extractTenderRequirementsFromText(manualText);
      const nextGroups = buildExtractedTenderGroups(res.tenders, "manual", res.items);
      setExtractedTenderGroups(prev => mergeExtractedTenderGroups(prev, nextGroups, "manual"));
      setManualText("");
    } catch (err: any) {
      setError(err.message || "Ошибка извлечения");
    } finally {
      setIsExtracting(false);
    }
  };

  const handleExtractFromCrm = async () => {
    if (selectedTendersForExtraction.length === 0) return;
    setIsExtracting(true);
    setError(null);
    try {
      const ids = selectedTendersForExtraction.map(t => t.id);
      const res = await extractTenderRequirementsFromCrm(ids);
      const nextGroups = buildExtractedTenderGroups(res.tenders, "crm", res.items);
      setExtractedTenderGroups(prev => mergeExtractedTenderGroups(prev, nextGroups, "crm"));
    } catch (err: any) {
      setError(err.message || "Ошибка извлечения из CRM");
    } finally {
      setIsExtracting(false);
    }
  };

  const handleSearchForItem = async (item: TenderRequirementItem) => {
    setSearchQueue([item]);
    setActiveSearchQueueItemId(item.id);
    try {
      await runSearchForItem(item, { activate: true });
    } catch {
      // Error state is already reflected in component state.
    }
  };

  const handleSearchForGroup = async (group: ExtractedTenderGroup) => {
    const selectedItems = getSelectedItemsForGroup(group);
    const queueItems = selectedItems.length > 0 ? selectedItems : group.items;

    if (queueItems.length === 0) {
      return;
    }

    await runBatchSearch(queueItems);
  };

  const clearExtractedResults = () => {
    setExtractedTenderGroups([]);
    setSelectedExtractedItemIds([]);
    setSearchQueue([]);
    setActiveSearchQueueItemId(null);
    setSearchResultsByItemId({});
    setSearchStatusesByItemId({});
    setSearchErrorsByItemId({});
    setSearchRefinementJobsByItemId({});
    setStandaloneRefinementJob(null);
    setSearchResult(null);
    setAiError(null);
    setError(null);
  };

  const handleExportAllParsedTz = async () => {
    if (extractedTenderGroups.length === 0 || isExportingParsedTz) {
      return;
    }

    setIsExportingParsedTz(true);
    try {
      await exportParsedTenderTzWord(extractedTenderGroups);
    } catch (err: any) {
      setError(err.message || "Ошибка экспорта ТЗ");
    } finally {
      setIsExportingParsedTz(false);
    }
  };

  const handleExportSelectedParsedTz = async () => {
    if (selectedExtractedItemIds.length === 0 || isExportingParsedTz) {
      return;
    }

    setIsExportingParsedTz(true);
    try {
      await exportParsedTenderTzWord(extractedTenderGroups, selectedExtractedItemIds);
    } catch (err: any) {
      setError(err.message || "Ошибка экспорта выбранных позиций");
    } finally {
      setIsExportingParsedTz(false);
    }
  };

  const handleExportGroupParsedTz = async (group: ExtractedTenderGroup) => {
    if (isExportingParsedTz) {
      return;
    }

    setIsExportingParsedTz(true);
    try {
      await exportParsedTenderTzWord([group]);
    } catch (err: any) {
      setError(err.message || "Ошибка экспорта ТЗ по тендеру");
    } finally {
      setIsExportingParsedTz(false);
    }
  };

  return (
    <div className="flex flex-col h-full bg-gray-50">

      {/* ── ШАПКА ────────────────────────────────────────────────────── */}
      <div className="bg-white border-b border-gray-200 px-6 py-3 flex-shrink-0">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="p-2 bg-blue-50 rounded-lg">
              <Package className="w-5 h-5 text-blue-600" />
            </div>
            <div>
              <h1 className="text-lg font-semibold text-gray-900">Подбор аналогов</h1>
              <p className="text-xs text-gray-500">
                Поиск в базе gidroizol.ru и через ИИ
              </p>
            </div>
          </div>

          <div className="flex items-center gap-3">
            {/* Счётчики */}
            <div className="flex items-center gap-4 text-sm">
              <div className="text-center">
                <div className="font-semibold text-gray-900">{allProducts.length}</div>
                <div className="text-xs text-gray-500">в базе</div>
              </div>
              {selectedProducts.length > 0 && (
                <div className="text-center">
                  <div className="font-semibold text-blue-600">{selectedProducts.length}</div>
                  <div className="text-xs text-gray-500">в КП</div>
                </div>
              )}
            </div>

            {/* Обновить каталог */}
            <button
              onClick={handleRefreshCatalog}
              disabled={isRefreshing}
              className="flex items-center gap-1.5 px-3 py-2 text-sm border border-gray-200 rounded-lg hover:bg-gray-50 text-gray-600 disabled:opacity-50"
              title="Обновить каталог с gidroizol.ru"
            >
              <RefreshCw className={`w-4 h-4 ${isRefreshing ? "animate-spin" : ""}`} />
              {isRefreshing ? "Обновляю..." : "Обновить каталог"}
            </button>

            {/* Добавить вручную */}
            <button
              onClick={() => setShowAddForm(v => !v)}
              className="flex items-center gap-1.5 px-3 py-2 text-sm bg-blue-600 text-white rounded-lg hover:bg-blue-700"
            >
              <Plus className="w-4 h-4" />
              Добавить
            </button>
          </div>
        </div>
      </div>

      {/* ── ФОРМА ДОБАВЛЕНИЯ ─────────────────────────────────────────── */}
      {showAddForm && (
        <div className="px-6 py-3 bg-white border-b border-gray-200 flex-shrink-0">
          <AddManualForm onAdd={handleAddManual} onClose={() => setShowAddForm(false)} />
        </div>
      )}

      {/* ── ВКЛАДКИ ──────────────────────────────────────────────────── */}
      <div className="bg-white border-b border-gray-200 flex-shrink-0">
        <div className="flex px-6">
          {([
            { key: "search",   label: "Поиск аналогов",  icon: Search },
            { key: "extraction", label: "Извлечение ТЗ", icon: FileText },
            { key: "catalog",  label: `Каталог (${allProducts.length})`, icon: BookOpen },
            { key: "selected", label: `КП (${selectedProducts.length})`, icon: CheckCircle },
          ] as const).map(tab => (
            <button
              key={tab.key}
              onClick={() => setActiveTab(tab.key)}
              className={`
                flex items-center gap-2 px-4 py-3 text-sm font-medium border-b-2 transition-colors
                ${activeTab === tab.key
                  ? "border-blue-600 text-blue-700"
                  : "border-transparent text-gray-500 hover:text-gray-700"
                }
              `}
            >
              <tab.icon className="w-4 h-4" />
              {tab.label}
            </button>
          ))}
        </div>
      </div>

      {/* ── КОНТЕНТ ──────────────────────────────────────────────────── */}
      <div className="flex-1 overflow-y-auto p-6">
        {error && (
          <div className="mb-4 flex items-center gap-2 p-3 bg-red-50 border border-red-200 rounded-lg text-red-700 text-sm">
            <AlertCircle className="w-4 h-4 flex-shrink-0" />
            {error}
            <button onClick={() => setError(null)} className="ml-auto"><X className="w-4 h-4" /></button>
          </div>
        )}
        
        {aiError && (
          <div className="mb-4 flex items-center gap-2 p-3 bg-yellow-50 border border-yellow-200 rounded-lg text-yellow-800 text-sm">
            <AlertCircle className="w-4 h-4 flex-shrink-0" />
            <span className="font-medium">Ошибка ИИ-поиска:</span> {aiError}
            <button onClick={() => setAiError(null)} className="ml-auto"><X className="w-4 h-4" /></button>
          </div>
        )}

        {/* ── ВКЛАДКА ПОИСК ─────────────────────────────────────────── */}
        {activeTab === "search" && (
          <div className="max-w-4xl mx-auto space-y-4">

            {/* Поисковая строка */}
            <div className="bg-white rounded-xl border border-gray-200 p-5">
              <div className="flex gap-3 mb-3">
                <div className="flex-1 relative">
                  <Search className="absolute left-3 top-2.5 w-4 h-4 text-gray-400" />
                  <input
                    type="text"
                    value={searchQuery}
                    onChange={e => setSearchQuery(e.target.value)}
                    onKeyDown={e => e.key === "Enter" && handleSearch()}
                    placeholder="Введите название материала... (напр: Гидроизол ТПП, мастика битумная)"
                    className="w-full pl-9 pr-3 py-2.5 text-sm border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                  />
                </div>
                <button
                  onClick={handleSearch}
                  disabled={isSearching || !searchQuery.trim()}
                  className="flex items-center gap-2 px-5 py-2.5 bg-blue-600 text-white text-sm font-medium rounded-lg hover:bg-blue-700 disabled:opacity-50"
                >
                  {isSearching ? (
                    <Loader2 className="w-4 h-4 animate-spin" />
                  ) : (
                    <Search className="w-4 h-4" />
                  )}
                  {isSearching ? "Ищу..." : "Найти"}
                </button>
              </div>

              {/* Режим поиска */}
              <div className="flex items-center gap-2 text-xs">
                <span className="text-gray-500">Режим:</span>
                {([
                  { key: "both",  label: "БД + ИИ",     icon: Sparkles },
                  { key: "local", label: "Только БД",   icon: Database },
                  { key: "ai",    label: "Только ИИ",   icon: Globe },
                ] as const).map(m => (
                  <button
                    key={m.key}
                    onClick={() => setSearchMode(m.key)}
                    className={`flex items-center gap-1 px-3 py-1 rounded-full border transition-colors ${
                      searchMode === m.key
                        ? "bg-blue-600 text-white border-blue-600"
                        : "border-gray-200 text-gray-600 hover:bg-gray-50"
                    }`}
                  >
                    <m.icon className="w-3 h-3" />
                    {m.label}
                  </button>
                ))}

                {/* Требования из ТЗ */}
                <button
                  onClick={() => setShowRequirements(v => !v)}
                  className="ml-auto flex items-center gap-1 px-3 py-1 rounded-full border border-gray-200 text-gray-600 hover:bg-gray-50"
                >
                  <Filter className="w-3 h-3" />
                  {showRequirements ? "Скрыть ТЗ" : "Добавить ТЗ"}
                </button>
              </div>

              {/* Поле ТЗ */}
              {showRequirements && (
                <div className="mt-3">
                  <label className="block text-xs text-gray-600 mb-1">
                    Технические требования из ТЗ тендера (для точного подбора):
                  </label>
                  <textarea
                    value={requirements}
                    onChange={e => setRequirements(e.target.value)}
                    placeholder="Вставьте технические требования к материалу из документации тендера..."
                    rows={4}
                    className="w-full px-3 py-2 text-sm border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 resize-none"
                  />
                </div>
              )}
            </div>

            {searchQueue.length > 0 && (
              <div className="bg-white rounded-xl border border-gray-200 p-4 space-y-3">
                <div className="flex items-start justify-between gap-3">
                  <div>
                    <h3 className="text-sm font-semibold text-gray-800">
                      Очередь поиска по позициям ({searchQueue.length})
                    </h3>
                    <p className="text-xs text-gray-500 mt-1">
                      {selectedSearchQueueItem?.tender_title || "Выбранный тендер"}
                    </p>
                    {isBatchSearching && (
                      <p className="text-xs text-blue-600 mt-1">
                        Выполняется поиск по всем выбранным позициям и сохранение результатов.
                      </p>
                    )}
                  </div>
                  <button
                    onClick={() => {
                      setSearchQueue([]);
                      setActiveSearchQueueItemId(null);
                    }}
                    className="text-xs text-gray-500 hover:text-gray-700"
                  >
                    Очистить очередь
                  </button>
                </div>

                <div className="flex flex-wrap gap-2">
                  {searchQueue.map((item, index) => {
                    const isActive = selectedSearchQueueItem?.id === item.id;
                    const storedResult = searchResultsByItemId[item.id];
                    const status = searchStatusesByItemId[item.id];

                    return (
                      <button
                        key={item.id}
                        onClick={() => {
                          setActiveSearchQueueItemId(item.id);
                          applyItemToSearch(item);
                        }}
                        className={`px-3 py-1.5 text-xs rounded-full border transition-colors ${
                          isActive
                            ? "border-blue-600 bg-blue-50 text-blue-700"
                            : "border-gray-200 text-gray-600 hover:bg-gray-50"
                        }`}
                      >
                        {index + 1}. {item.position_name}
                        {status === "searching" && " · поиск..."}
                        {status === "refining" &&
                          ` · ${
                            formatRetryCountdown(searchRefinementJobsByItemId[item.id], refinementClockMs) || "уточнение..."
                          }`}
                        {status === "error" && " · ошибка"}
                        {storedResult && status !== "searching" && ` · найдено ${storedResult.total}`}
                      </button>
                    );
                  })}
                </div>
              </div>
            )}

            {/* Результаты поиска */}
            {isSearching && (
              <div className="flex flex-col items-center justify-center py-16 text-gray-400">
                <Loader2 className="w-10 h-10 animate-spin mb-3 text-blue-500" />
                <p className="text-sm font-medium text-gray-600">Ищу аналоги...</p>
                <p className="text-xs mt-1">
                  {searchMode === "ai" || searchMode === "both"
                    ? "ИИ анализирует рынок, это займёт 10-20 секунд"
                    : "Поиск в локальной базе"
                  }
                </p>
              </div>
            )}

            {searchResult && !isSearching && (
              <div className="space-y-4">
                <div className="flex items-center justify-between">
                  <h2 className="text-sm font-semibold text-gray-700">
                    Найдено аналогов: {totalResults}
                    <span className="ml-2 font-normal text-gray-400">
                      по запросу «{searchResult.query}»
                    </span>
                  </h2>
                </div>

                {refinementPending && (
                  <div className="flex items-start gap-3 p-3 bg-indigo-50 border border-indigo-200 rounded-lg text-sm text-indigo-900">
                    <Loader2 className="w-4 h-4 mt-0.5 animate-spin text-indigo-600" />
                    <div>
                      <p className="font-medium">
                        Показан предварительный результат. ИИ продолжает internet-поиск и уточнение в фоне.
                      </p>
                      {activeRefinementJob?.stage && (
                        <p className="text-xs text-indigo-700 mt-1">{activeRefinementJob.stage}</p>
                      )}
                      {activeRetryCountdown && (
                        <p className="text-xs text-indigo-800 mt-1 font-medium">
                          Автоповтор: {activeRetryCountdown}
                        </p>
                      )}
                    </div>
                  </div>
                )}

                {selectedSearchQueueItem && (
                  <div className="p-3 bg-blue-50 border border-blue-200 rounded-lg text-xs text-blue-800">
                    Результат сохранен для позиции: <span className="font-semibold">{selectedSearchQueueItem.position_name}</span>
                    {selectedSearchQueueItem.tender_title && (
                      <span className="text-blue-600"> · {selectedSearchQueueItem.tender_title}</span>
                    )}
                  </div>
                )}

                {searchResult.validation_summary && (
                  <div className="flex items-start gap-3 p-3 bg-emerald-50 border border-emerald-200 rounded-lg text-sm text-emerald-900">
                    <CheckCircle className="w-4 h-4 mt-0.5 text-emerald-600" />
                    <div>
                      <p className="font-medium">Краткий ИИ-анализ соответствия</p>
                      <p className="text-xs text-emerald-800 mt-1">{searchResult.validation_summary}</p>
                    </div>
                  </div>
                )}

                      <AnalogComparisonMatrix
                        item={selectedSearchQueueItem}
                        requirementsText={selectedSearchQueueItem ? buildRequirementText(selectedSearchQueueItem) : requirements}
                        searchResult={searchResult}
                        catalogProducts={allProducts}
                      />

                {/* Из локальной БД */}
                {searchResult.local_results.length > 0 && (
                  <div>
                    <div className="flex items-center gap-2 mb-3">
                      <Database className="w-4 h-4 text-emerald-600" />
                      <h3 className="text-sm font-medium text-gray-700">
                        Из каталога gidroizol.ru ({searchResult.local_results.length})
                      </h3>
                    </div>
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                      {searchResult.local_results.map((product, idx) => {
                        const contextualProduct = attachProductContext(product, selectedSearchQueueItem);
                        return (
                        <ProductCard
                          key={`local-${idx}`}
                          product={contextualProduct}
                          onSelect={toggleSelect}
                          selected={isSelected(contextualProduct)}
                        />
                        );
                      })}
                    </div>
                  </div>
                )}

                {/* От ИИ */}
                {searchResult.ai_results.length > 0 && (
                  <div>
                    <div className="flex items-center gap-2 mb-3">
                      <Sparkles className="w-4 h-4 text-purple-600" />
                      <h3 className="text-sm font-medium text-gray-700">
                        Найдено ИИ в интернете ({searchResult.ai_results.length})
                      </h3>
                    </div>
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                      {searchResult.ai_results.map((product, idx) => {
                        const contextualProduct = attachProductContext(product, selectedSearchQueueItem);
                        return (
                        <ProductCard
                          key={`ai-${idx}`}
                          product={contextualProduct}
                          onSelect={toggleSelect}
                          selected={isSelected(contextualProduct)}
                        />
                        );
                      })}
                    </div>
                  </div>
                )}

                {totalResults === 0 && refinementPending && (
                  <div className="flex flex-col items-center justify-center py-12 text-indigo-500">
                    <Loader2 className="w-8 h-8 mb-2 animate-spin opacity-80" />
                    <p className="text-sm">Предварительных совпадений пока нет</p>
                    <p className="text-xs mt-1">Продолжаю internet-поиск и AI-уточнение характеристик</p>
                  </div>
                )}

                {totalResults === 0 && !refinementPending && (
                  <div className="flex flex-col items-center justify-center py-12 text-gray-400">
                    <AlertCircle className="w-10 h-10 mb-2 opacity-50" />
                    <p className="text-sm">Аналоги не найдены</p>
                    <p className="text-xs mt-1">
                      Попробуйте изменить запрос или переключиться на режим «ИИ»
                    </p>
                  </div>
                )}
              </div>
            )}

            {!searchResult && !isSearching && (
              <div className="flex flex-col items-center justify-center py-16 text-gray-400">
                <Package className="w-16 h-16 mb-4 opacity-20" />
                <p className="text-base font-medium text-gray-500">Введите название материала</p>
                <p className="text-sm mt-1 text-center max-w-sm">
                  Система найдёт аналоги в каталоге gidroizol.ru и в интернете через ИИ
                </p>
              </div>
            )}
          </div>
        )}

        {/* ── ВКЛАДКА ИЗВЛЕЧЕНИЕ ТЗ ────────────────────────────────────── */}
        {activeTab === "extraction" && (
          <div className="max-w-5xl mx-auto space-y-6">
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
              
              {/* Извлечение из CRM */}
              <div className="bg-white rounded-xl border border-gray-200 p-5">
                <h3 className="text-sm font-semibold text-gray-800 mb-2 flex items-center gap-2">
                  <Briefcase className="w-4 h-4 text-blue-600" />
                  Извлечение из тендеров CRM
                </h3>
                <p className="text-xs text-gray-500 mb-4">
                  Выберите тендеры в CRM (поставив галочку "В подбор аналогов"), чтобы извлечь из них требования.
                </p>
                
                <div className="space-y-2 mb-4 max-h-40 overflow-y-auto">
                  {selectedTendersForExtraction.length === 0 ? (
                    <div className="text-xs text-gray-400 italic">Нет выбранных тендеров</div>
                  ) : (
                    selectedTendersForExtraction.map(t => (
                      <div key={t.id} className="text-xs p-2 bg-gray-50 rounded border border-gray-100 flex items-center gap-2">
                        <CheckCircle className="w-3 h-3 text-emerald-500" />
                        <span className="truncate">{t.eis_number} - {t.title}</span>
                      </div>
                    ))
                  )}
                </div>

                <button
                  onClick={handleExtractFromCrm}
                  disabled={isExtracting || selectedTendersForExtraction.length === 0}
                  className="w-full flex items-center justify-center gap-2 px-4 py-2 bg-blue-600 text-white text-sm font-medium rounded-lg hover:bg-blue-700 disabled:opacity-50"
                >
                  {isExtracting ? <Loader2 className="w-4 h-4 animate-spin" /> : <ListChecks className="w-4 h-4" />}
                  Извлечь из CRM
                </button>
              </div>

              {/* Извлечение из текста */}
              <div className="bg-white rounded-xl border border-gray-200 p-5">
                <h3 className="text-sm font-semibold text-gray-800 mb-2 flex items-center gap-2">
                  <FileText className="w-4 h-4 text-purple-600" />
                  Извлечение из текста
                </h3>
                <p className="text-xs text-gray-500 mb-4">
                  Вставьте текст технического задания, сметы или спецификации.
                </p>
                
                <textarea
                  value={manualText}
                  onChange={e => setManualText(e.target.value)}
                  placeholder="Вставьте текст сюда..."
                  rows={5}
                  className="w-full px-3 py-2 text-sm border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-purple-500 resize-none mb-4"
                />

                <button
                  onClick={handleExtractFromText}
                  disabled={isExtracting || !manualText.trim()}
                  className="w-full flex items-center justify-center gap-2 px-4 py-2 bg-purple-600 text-white text-sm font-medium rounded-lg hover:bg-purple-700 disabled:opacity-50"
                >
                  {isExtracting ? <Loader2 className="w-4 h-4 animate-spin" /> : <Sparkles className="w-4 h-4" />}
                  Извлечь из текста
                </button>
              </div>
            </div>

            {/* Результаты извлечения */}
            {extractedTenderGroups.length > 0 && (
              <div className="bg-white rounded-xl border border-gray-200 p-5">
                <div className="flex items-center justify-between mb-4">
                  <h3 className="text-sm font-semibold text-gray-800">
                    Извлеченные позиции ({totalExtractedItems})
                  </h3>
                  <div className="flex flex-wrap items-center gap-3">
                    <button
                      onClick={handleExportAllParsedTz}
                      disabled={isExportingParsedTz}
                      className="text-xs text-blue-600 hover:text-blue-800 flex items-center gap-1 disabled:opacity-50"
                    >
                      {isExportingParsedTz ? <Loader2 className="w-3 h-3 animate-spin" /> : <FileText className="w-3 h-3" />}
                      Выгрузить все ТЗ
                    </button>
                    <button
                      onClick={handleExportSelectedParsedTz}
                      disabled={isExportingParsedTz || selectedExtractedItemIds.length === 0}
                      className="text-xs text-emerald-600 hover:text-emerald-800 flex items-center gap-1 disabled:opacity-50"
                    >
                      {isExportingParsedTz ? <Loader2 className="w-3 h-3 animate-spin" /> : <ListChecks className="w-3 h-3" />}
                      Выгрузить выбранные
                    </button>
                    <button 
                      onClick={clearExtractedResults}
                      className="text-xs text-red-500 hover:text-red-700 flex items-center gap-1"
                    >
                      <Trash2 className="w-3 h-3" /> Очистить
                    </button>
                  </div>
                </div>

                <div className="space-y-4">
                  {extractedTenderGroups.map(group => {
                    const selectedItems = getSelectedItemsForGroup(group);
                    const allSelected = isGroupFullySelected(group);

                    return (
                      <div key={group.group_key} className="rounded-xl border border-gray-200 overflow-hidden">
                        <div className="px-4 py-3 bg-gray-50 border-b border-gray-200">
                          <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
                            <div className="min-w-0">
                              <div className="flex items-center gap-2 flex-wrap">
                                <h4 className="text-sm font-semibold text-gray-900">
                                  {group.tender_title}
                                </h4>
                                <span className="text-[10px] px-1.5 py-0.5 bg-white border border-gray-200 text-gray-600 rounded">
                                  {group.source_label || group.source}
                                </span>
                                <span className="text-[10px] px-1.5 py-0.5 bg-blue-50 text-blue-700 rounded">
                                  позиций: {group.items.length}
                                </span>
                                {selectedItems.length > 0 && (
                                  <span className="text-[10px] px-1.5 py-0.5 bg-emerald-50 text-emerald-700 rounded">
                                    выбрано: {selectedItems.length}
                                  </span>
                                )}
                              </div>
                              <div className="text-xs text-gray-500 mt-1 truncate">
                                {group.tender_id}
                              </div>
                            </div>

                            <div className="flex flex-wrap items-center gap-2">
                              <label className="flex items-center gap-2 text-xs text-gray-600 px-3 py-1.5 bg-white border border-gray-200 rounded-lg cursor-pointer">
                                <input
                                  type="checkbox"
                                  checked={allSelected}
                                  onChange={() => toggleGroupSelection(group)}
                                  className="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                                />
                                {allSelected ? "Снять выбор" : "Выбрать все"}
                              </label>

                              <button
                                onClick={() => handleExportGroupParsedTz(group)}
                                disabled={isExportingParsedTz}
                                className="flex items-center gap-1.5 px-3 py-1.5 bg-white border border-emerald-200 text-emerald-700 text-xs font-medium rounded-lg hover:bg-emerald-50 disabled:opacity-50"
                              >
                                {isExportingParsedTz ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <FileText className="w-3.5 h-3.5" />}
                                Word ТЗ
                              </button>

                              <button
                                onClick={() => handleSearchForGroup(group)}
                                disabled={group.items.length === 0 || isBatchSearching}
                                className="flex items-center gap-1.5 px-3 py-1.5 bg-white border border-blue-200 text-blue-600 text-xs font-medium rounded-lg hover:bg-blue-50 disabled:opacity-50"
                              >
                                <Search className="w-3.5 h-3.5" />
                                {isBatchSearching
                                  ? "Идет поиск..."
                                  : selectedItems.length > 0
                                  ? `Искать выбранные (${selectedItems.length})`
                                  : `Искать все (${group.items.length})`}
                              </button>
                            </div>
                          </div>
                        </div>

                        {group.warnings.length > 0 && (
                          <div className="px-4 pt-4">
                            <div className="p-3 bg-yellow-50 border border-yellow-200 rounded-lg space-y-1">
                              <h5 className="text-xs font-semibold text-yellow-800 flex items-center gap-1">
                                <AlertCircle className="w-3 h-3" /> Предупреждения
                              </h5>
                              {group.warnings.map((warning, index) => (
                                <div key={`${group.group_key}-warning-${index}`} className="text-xs text-yellow-700">
                                  • {warning.message}
                                </div>
                              ))}
                            </div>
                          </div>
                        )}

                        {(group.general_requirements || []).length > 0 && (
                          <div className="px-4 pt-4">
                            <div className="p-3 bg-blue-50 border border-blue-200 rounded-lg space-y-1">
                              <h5 className="text-xs font-semibold text-blue-800 flex items-center gap-1">
                                <ListChecks className="w-3 h-3" /> Общие требования к продукции
                              </h5>
                              <ul className="list-disc list-inside space-y-1">
                                {(group.general_requirements || []).map((requirement, index) => (
                                  <li key={`${group.group_key}-req-${index}`} className="text-xs text-blue-700 ml-1">
                                    {requirement}
                                  </li>
                                ))}
                              </ul>
                            </div>
                          </div>
                        )}

                        <div className="p-4 space-y-3">
                          {group.items.length === 0 ? (
                            <div className="text-xs text-gray-400 italic">
                              Позиции в этом тендере не найдены. Проверьте предупреждения выше.
                            </div>
                          ) : (
                            group.items.map(item => {
                              const storedResult = searchResultsByItemId[item.id];
                              const status = searchStatusesByItemId[item.id];
                              const searchError = searchErrorsByItemId[item.id];

                              return (
                              <div key={item.id} className="p-3 border border-gray-100 bg-gray-50 rounded-lg flex flex-col gap-4 sm:flex-row sm:items-start">
                                <label className="flex items-start pt-0.5">
                                  <input
                                    type="checkbox"
                                    checked={selectedExtractedItemIds.includes(item.id)}
                                    onChange={() => toggleExtractedItemSelection(item.id)}
                                    className="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                                  />
                                </label>

                                <div className="flex-1 min-w-0">
                                  <div className="flex items-center gap-2 mb-1 flex-wrap">
                                    <span className="font-medium text-sm text-gray-900">{item.position_name}</span>
                                    <span className="text-[10px] px-1.5 py-0.5 bg-gray-200 text-gray-600 rounded">
                                      {item.source_label || item.source}
                                    </span>
                                    {item.quality_status === "degraded" && (
                                      <span className="text-[10px] px-1.5 py-0.5 bg-yellow-100 text-yellow-800 rounded">
                                        неполные данные
                                      </span>
                                    )}
                                    {status === "searching" && (
                                      <span className="text-[10px] px-1.5 py-0.5 bg-blue-100 text-blue-700 rounded">
                                        поиск...
                                      </span>
                                    )}
                                    {status === "refining" && (
                                      <span className="text-[10px] px-1.5 py-0.5 bg-indigo-100 text-indigo-700 rounded">
                                        {formatRetryCountdown(searchRefinementJobsByItemId[item.id], refinementClockMs)
                                          ? `автоповтор: ${formatRetryCountdown(searchRefinementJobsByItemId[item.id], refinementClockMs)}`
                                          : "internet-уточнение..."}
                                      </span>
                                    )}
                                    {storedResult && status !== "searching" && (
                                      <span className="text-[10px] px-1.5 py-0.5 bg-emerald-100 text-emerald-700 rounded">
                                        сохранено аналогов: {storedResult.total}
                                      </span>
                                    )}
                                    {status === "error" && (
                                      <span className="text-[10px] px-1.5 py-0.5 bg-red-100 text-red-700 rounded">
                                        ошибка поиска
                                      </span>
                                    )}
                                  </div>
                                  <div className="space-y-3">
                                    <div className="text-xs text-gray-500 space-y-0.5">
                                      {item.quantity && <div>Количество: {item.quantity} {item.unit}</div>}
                                      {item.notes && <div>Примечание: {item.notes}</div>}
                                      {searchError && <div className="text-red-500">Ошибка поиска: {searchError}</div>}
                                    </div>
                                    <RequirementCharacteristicsTable
                                      characteristics={parseStructuredCharacteristics(item)}
                                      compact
                                    />
                                  </div>
                                </div>

                                <button
                                  onClick={() => handleSearchForItem(item)}
                                  disabled={isBatchSearching}
                                  className="flex-shrink-0 flex items-center gap-1.5 px-3 py-1.5 bg-white border border-blue-200 text-blue-600 text-xs font-medium rounded hover:bg-blue-50 disabled:opacity-50"
                                >
                                  <Search className="w-3.5 h-3.5" />
                                  {storedResult ? "Обновить поиск" : "Искать позицию"}
                                </button>
                              </div>
                            );
                          })
                          )}
                        </div>
                      </div>
                    );
                  })}
                </div>
              </div>
            )}
          </div>
        )}

        {/* ── ВКЛАДКА КАТАЛОГ ───────────────────────────────────────── */}
        {activeTab === "catalog" && (
          <div className="max-w-4xl mx-auto">
            {isLoadingAll ? (
              <div className="flex items-center justify-center py-16">
                <Loader2 className="w-8 h-8 animate-spin text-blue-500" />
              </div>
            ) : allProducts.length === 0 ? (
              <div className="flex flex-col items-center justify-center py-16 text-gray-400">
                <Database className="w-16 h-16 mb-4 opacity-20" />
                <p className="text-base font-medium text-gray-500">Каталог пуст</p>
                <p className="text-sm mt-1">Нажмите «Обновить каталог» чтобы загрузить данные с gidroizol.ru</p>
                <button
                  onClick={handleRefreshCatalog}
                  className="mt-4 flex items-center gap-2 px-4 py-2 bg-blue-600 text-white text-sm rounded-lg hover:bg-blue-700"
                >
                  <RefreshCw className="w-4 h-4" /> Загрузить каталог
                </button>
              </div>
            ) : (
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
                {allProducts.map(product => (
                  <ProductCard
                    key={product.id}
                    product={product}
                    onSelect={toggleSelect}
                    onDelete={handleDelete}
                    selected={isSelected(product)}
                  />
                ))}
              </div>
            )}
          </div>
        )}

        {/* ── ВКЛАДКА КП ────────────────────────────────────────────── */}
        {activeTab === "selected" && (
          <div className="max-w-3xl mx-auto">
            {selectedProducts.length === 0 ? (
              <div className="flex flex-col items-center justify-center py-16 text-gray-400">
                <CheckCircle className="w-16 h-16 mb-4 opacity-20" />
                <p className="text-base font-medium text-gray-500">КП пусто</p>
                <p className="text-sm mt-1">
                  Добавьте аналоги из поиска или каталога
                </p>
                <button
                  onClick={() => setActiveTab("search")}
                  className="mt-4 flex items-center gap-2 px-4 py-2 bg-blue-600 text-white text-sm rounded-lg hover:bg-blue-700"
                >
                  <ArrowRight className="w-4 h-4" /> Перейти к поиску
                </button>
              </div>
            ) : (
              <div className="space-y-4">
                <div className="flex items-center justify-between">
                  <h2 className="text-sm font-semibold text-gray-700">
                    Выбрано для КП: {selectedProducts.length} позиций
                  </h2>
                  <button
                    onClick={() => setSelectedProducts([])}
                    className="flex items-center gap-1 text-xs text-red-500 hover:text-red-700"
                  >
                    <X className="w-3.5 h-3.5" /> Очистить всё
                  </button>
                </div>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                  {selectedProducts.map((product, idx) => (
                    <div key={`${getProductSelectionKey(product)}-${idx}`} className="space-y-2">
                      {(product.matched_requirement_name || product.matched_tender_title) && (
                        <div className="px-3 py-2 bg-blue-50 border border-blue-200 rounded-lg text-xs text-blue-800">
                          {product.matched_requirement_name && (
                            <div>
                              Подобрано для позиции: <span className="font-semibold">{product.matched_requirement_name}</span>
                            </div>
                          )}
                          {product.matched_tender_title && (
                            <div className="mt-1 text-blue-600">{product.matched_tender_title}</div>
                          )}
                        </div>
                      )}
                      <ProductCard
                        product={product}
                        onSelect={toggleSelect}
                        selected={true}
                      />
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
