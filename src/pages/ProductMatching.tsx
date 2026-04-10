import { useState, useEffect, useCallback } from "react";
import {
  Search, Sparkles, Database, RefreshCw, Plus, Trash2,
  ExternalLink, ChevronDown, ChevronUp, CheckCircle,
  AlertCircle, Loader2, Package, X, Filter, Star,
  ArrowRight, BookOpen, Globe
} from "lucide-react";

// ── Типы ─────────────────────────────────────────────────────────────────────
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
  description?: string;
  match_score?: number;
  source?: "local_db" | "ai_search" | "manual";
}

interface SearchResult {
  query: string;
  local_results: Product[];
  ai_results: Product[];
  total: number;
}

// ── Вспомогательные ───────────────────────────────────────────────────────────
const formatPrice = (price?: number | null, unit?: string): string => {
  if (!price) return "—";
  const formatted = new Intl.NumberFormat("ru-RU").format(price);
  return unit ? `${formatted} ${unit}` : `${formatted} ₽`;
};

const getMatchColor = (score?: number): string => {
  if (!score) return "bg-gray-100 text-gray-600";
  if (score >= 90) return "bg-green-100 text-green-700";
  if (score >= 70) return "bg-blue-100 text-blue-700";
  if (score >= 50) return "bg-yellow-100 text-yellow-700";
  return "bg-red-100 text-red-700";
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

  return (
    <div
      className={`
        rounded-xl border transition-all duration-200
        ${selected
          ? "border-blue-400 bg-blue-50 shadow-md"
          : "border-gray-200 bg-white hover:border-blue-200 hover:shadow-sm"
        }
      `}
    >
      {/* Заголовок карточки */}
      <div className="p-4">
        <div className="flex items-start justify-between gap-2 mb-2">
          <div className="flex-1 min-w-0">
            <div className="flex items-center gap-2 mb-1 flex-wrap">
              <SourceBadge source={product.source} />
              {product.match_score != null && (
                <span className={`px-2 py-0.5 text-xs rounded-full font-semibold ${getMatchColor(product.match_score)}`}>
                  {product.match_score}% совпадение
                </span>
              )}
              {product.category && (
                <span className="px-2 py-0.5 text-xs rounded-full bg-gray-100 text-gray-600">
                  {product.category}
                </span>
              )}
            </div>
            <h3 className="text-sm font-semibold text-gray-900 leading-snug">
              {product.title}
            </h3>
            {product.manufacturer && (
              <p className="text-xs text-gray-500 mt-0.5">{product.manufacturer}</p>
            )}
          </div>
          <div className="flex items-center gap-1 flex-shrink-0">
            {product.url && (
              <a
                href={product.url}
                target="_blank"
                rel="noopener noreferrer"
                className="p-1.5 text-gray-400 hover:text-blue-600 rounded-lg hover:bg-blue-50 transition-colors"
                title="Открыть на сайте"
              >
                <ExternalLink className="w-3.5 h-3.5" />
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
          <div className="text-base font-bold text-gray-900 mb-2">
            {formatPrice(product.price, product.price_unit)}
          </div>
        )}

        {/* Описание / причина совпадения */}
        {product.description && (
          <p className="text-xs text-gray-600 mb-2 leading-relaxed bg-gray-50 rounded-lg p-2">
            {product.description}
          </p>
        )}

        {/* Характеристики */}
        {specEntries.length > 0 && (
          <div className="mt-2">
            <div className="grid grid-cols-2 gap-1">
              {specEntries.map(([key, value]) => (
                <div key={key} className="flex gap-1 text-xs">
                  <span className="text-gray-500 shrink-0">{key}:</span>
                  <span className="text-gray-800 font-medium truncate">{value}</span>
                </div>
              ))}
            </div>
            {Object.keys(product.specs || {}).length > 4 && (
              <button
                onClick={() => setExpanded(e => !e)}
                className="mt-1 flex items-center gap-1 text-xs text-blue-600 hover:text-blue-800"
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
  const [searchMode, setSearchMode]           = useState<"local"|"ai"|"both">("both");
  const [selectedProducts, setSelectedProducts] = useState<Product[]>([]);
  const [allProducts, setAllProducts]         = useState<Product[]>([]);
  const [isLoadingAll, setIsLoadingAll]       = useState(false);
  const [isRefreshing, setIsRefreshing]       = useState(false);
  const [showAddForm, setShowAddForm]         = useState(false);
  const [activeTab, setActiveTab]             = useState<"search"|"catalog"|"selected">("search");
  const [error, setError]                     = useState<string | null>(null);

  // Загрузка всего каталога
  const loadAllProducts = useCallback(async () => {
    setIsLoadingAll(true);
    try {
      const res = await fetch("/api/products?limit=100");
      const data = await res.json();
      setAllProducts((data.products || []).map((p: Product) => ({ ...p, source: "local_db" })));
    } catch {
      setError("Не удалось загрузить каталог");
    } finally {
      setIsLoadingAll(false);
    }
  }, []);

  useEffect(() => {
    loadAllProducts();
  }, [loadAllProducts]);

  // Поиск аналогов
  const handleSearch = async () => {
    if (!searchQuery.trim()) return;
    setIsSearching(true);
    setError(null);
    setSearchResult(null);

    try {
      if (searchMode === "local") {
        const res = await fetch("/api/products/search", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ query: searchQuery, limit: 10 }),
        });
        const data = await res.json();
        setSearchResult({
          query: searchQuery,
          local_results: (data.results || []).map((p: Product) => ({ ...p, source: "local_db" })),
          ai_results: [],
          total: data.total || 0,
        });
      } else {
        const res = await fetch("/api/products/search-ai", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            query: searchQuery,
            requirements: requirements || undefined,
            max_results: 5,
          }),
        });
        const data = await res.json();
        setSearchResult({
          query: data.query || searchQuery,
          local_results: (data.local_results || []).map((p: Product) => ({ ...p, source: "local_db" })),
          ai_results: (data.ai_results || []).map((p: Product) => ({ ...p, source: "ai_search" })),
          total: data.total || 0,
        });
      }
    } catch (err) {
      setError("Ошибка поиска. Проверьте подключение к серверу.");
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
      const exists = prev.some(p => p.title === product.title);
      return exists
        ? prev.filter(p => p.title !== product.title)
        : [...prev, product];
    });
  };

  const isSelected = (product: Product) =>
    selectedProducts.some(p => p.title === product.title);

  const totalResults =
    (searchResult?.local_results?.length || 0) +
    (searchResult?.ai_results?.length || 0);

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
                      {searchResult.local_results.map((product, idx) => (
                        <ProductCard
                          key={`local-${idx}`}
                          product={product}
                          onSelect={toggleSelect}
                          selected={isSelected(product)}
                        />
                      ))}
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
                      {searchResult.ai_results.map((product, idx) => (
                        <ProductCard
                          key={`ai-${idx}`}
                          product={product}
                          onSelect={toggleSelect}
                          selected={isSelected(product)}
                        />
                      ))}
                    </div>
                  </div>
                )}

                {totalResults === 0 && (
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
                    <ProductCard
                      key={idx}
                      product={product}
                      onSelect={toggleSelect}
                      selected={true}
                    />
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
