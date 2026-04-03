import { useState, useEffect, useCallback } from "react";
import {
  FileText, RefreshCw, Download, CheckSquare, Square,
  ChevronRight, AlertCircle, Search, Filter, ExternalLink,
  Loader2, FileIcon, File, ArrowUpDown, CheckCheck, X
} from "lucide-react";

// ── Типы ─────────────────────────────────────────────────────────────────────
interface TenderFile {
  name: string;
  size: number;
  url?: string;
  path?: string;
}

interface Tender {
  id: string;
  reg_number?: string;
  title: string;
  price?: number;
  law_type?: string;   // "fz44" | "fz223"
  publish_date?: string;
  deadline?: string;
  customer?: string;
  href?: string;
  files?: TenderFile[];
  status?: string;
}

// ── Вспомогательные функции ───────────────────────────────────────────────────
const formatPrice = (price?: number): string => {
  if (!price) return "—";
  return new Intl.NumberFormat("ru-RU", {
    style: "currency", currency: "RUB", maximumFractionDigits: 2
  }).format(price);
};

const formatSize = (bytes: number): string => {
  if (bytes < 1024) return `${bytes} Б`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(0)} КБ`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} МБ`;
};

const getFileIcon = (name: string) => {
  const ext = name.split(".").pop()?.toLowerCase();
  const colors: Record<string, string> = {
    pdf: "text-red-500", docx: "text-blue-500", doc: "text-blue-500",
    xlsx: "text-green-500", xls: "text-green-500", zip: "text-yellow-500",
    rar: "text-yellow-500",
  };
  return colors[ext || ""] || "text-gray-400";
};

const getLawBadge = (id: string) => {
  if (id.startsWith("223-")) {
    return <span className="px-1.5 py-0.5 text-xs rounded bg-purple-100 text-purple-700 font-medium">ФЗ-223</span>;
  }
  return <span className="px-1.5 py-0.5 text-xs rounded bg-blue-100 text-blue-700 font-medium">ФЗ-44</span>;
};

// ── Главный компонент ─────────────────────────────────────────────────────────
export default function LegalAnalysis() {
  const [tenders, setTenders]           = useState<Tender[]>([]);
  const [selectedId, setSelectedId]     = useState<string | null>(null);
  const [tenderFiles, setTenderFiles]   = useState<Record<string, TenderFile[]>>({});
  const [checkedFiles, setCheckedFiles] = useState<Record<string, Set<string>>>({});
  const [refreshingId, setRefreshingId] = useState<string | null>(null);
  const [loadingFiles, setLoadingFiles] = useState<string | null>(null);
  const [searchQuery, setSearchQuery]   = useState("");
  const [filterLaw, setFilterLaw]       = useState<"all"|"fz44"|"fz223">("all");
  const [filterDocs, setFilterDocs]     = useState<"all"|"has"|"empty">("all");
  const [sortBy, setSortBy]             = useState<"date"|"price">("date");
  const [analyzing, setAnalyzing]       = useState(false);
  const [analysisResult, setAnalysisResult] = useState<string | null>(null);

  const selected = tenders.find(t => t.id === selectedId) || null;

  // ── Загрузка тендеров ─────────────────────────────────────────────────────
  useEffect(() => {
    fetch("/api/crm/tenders")
      .then(r => r.json())
      .then(data => {
        const list: Tender[] = Array.isArray(data) ? data : (data.tenders || []);
        setTenders(list);
        if (list.length > 0) setSelectedId(list[0].id);
      })
      .catch(err => console.error("Failed to load tenders:", err));
  }, []);

  // ── Загрузка файлов при смене выбранного тендера ─────────────────────────
  useEffect(() => {
    if (!selectedId) return;
    if (tenderFiles[selectedId] !== undefined) return;  // уже загружены
    setLoadingFiles(selectedId);
    fetch(`/api/tenders/${selectedId}/files`)
      .then(r => r.json())
      .then(data => {
        const files: TenderFile[] = Array.isArray(data) ? data : (data.files || []);
        setTenderFiles(prev => ({ ...prev, [selectedId]: files }));
      })
      .catch(() => {
        setTenderFiles(prev => ({ ...prev, [selectedId]: [] }));
      })
      .finally(() => setLoadingFiles(null));
  }, [selectedId]);

  // ── Принудительное обновление документов ─────────────────────────────────
  const handleRefresh = useCallback(async (tenderId: string) => {
    setRefreshingId(tenderId);
    try {
      const response = await fetch(
        `/api/tenders/${tenderId}/refresh-files`,
        { method: "POST" }
      );
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const data = await response.json();
      const files: TenderFile[] = data.files || [];
      setTenderFiles(prev => ({ ...prev, [tenderId]: files }));
    } catch (err) {
      console.error("Refresh failed:", err);
    } finally {
      setRefreshingId(null);
    }
  }, []);

  // ── Чекбоксы файлов ───────────────────────────────────────────────────────
  const toggleFile = (tenderId: string, fileName: string) => {
    setCheckedFiles(prev => {
      const set = new Set(prev[tenderId] || []);
      set.has(fileName) ? set.delete(fileName) : set.add(fileName);
      return { ...prev, [tenderId]: set };
    });
  };

  const selectAllFiles = (tenderId: string) => {
    const files = tenderFiles[tenderId] || [];
    setCheckedFiles(prev => ({
      ...prev,
      [tenderId]: new Set(files.map(f => f.name))
    }));
  };

  const clearAllFiles = (tenderId: string) => {
    setCheckedFiles(prev => ({ ...prev, [tenderId]: new Set() }));
  };

  const handleFileUpload = async (tenderId: string, files: FileList | null) => {
    if (!files || files.length === 0) return;
    const file = files[0];
    const formData = new FormData();
    formData.append("file", file);

    try {
      const res = await fetch(`/api/tenders/${tenderId}/upload`, {
        method: "POST",
        body: formData
      });
      if (!res.ok) throw new Error("Upload failed");
      const data = await res.json();
      
      // Обновляем список файлов
      setTenderFiles(prev => ({
        ...prev,
        [tenderId]: [...(prev[tenderId] || []), data.file]
      }));
    } catch (err) {
      console.error("Upload error:", err);
    }
  };

  // ── ИИ Анализ ─────────────────────────────────────────────────────────────
  const handleAnalyze = async () => {
    const checkedTenders = tenders.filter(t => {
      const checked = checkedFiles[t.id];
      return checked && checked.size > 0;
    });
    if (checkedTenders.length === 0) {
      alert("Выберите хотя бы один файл для анализа");
      return;
    }
    setAnalyzing(true);
    setAnalysisResult(null);
    try {
      const payload = checkedTenders.map(t => ({
        tender_id: t.id,
        files: Array.from(checkedFiles[t.id] || [])
      }));
      const response = await fetch("/api/ai/analyze-tenders-batch", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ tender_ids: payload.map(p => p.tender_id), selected_files: Object.fromEntries(payload.map(p => [p.tender_id, p.files])) })
      });
      const data = await response.json();
      setAnalysisResult(`Задача на анализ создана. ID: ${data.job_id}\n\nОжидайте завершения...`);
    } catch (err) {
      setAnalysisResult(`Ошибка анализа: ${err}`);
    } finally {
      setAnalyzing(false);
    }
  };

  // ── Фильтрация и сортировка ───────────────────────────────────────────────
  const filtered = tenders
    .filter(t => {
      if (searchQuery) {
        const q = searchQuery.toLowerCase();
        const match = (t.title || "").toLowerCase().includes(q) ||
                      (t.id || "").toLowerCase().includes(q) ||
                      (t.customer || "").toLowerCase().includes(q);
        if (!match) return false;
      }
      if (filterLaw !== "all") {
        const isFz223 = t.id.startsWith("223-");
        if (filterLaw === "fz223" && !isFz223) return false;
        if (filterLaw === "fz44"  &&  isFz223) return false;
      }
      if (filterDocs !== "all") {
        const count = (tenderFiles[t.id] || []).length;
        if (filterDocs === "has"   && count === 0) return false;
        if (filterDocs === "empty" && count > 0)  return false;
      }
      return true;
    })
    .sort((a, b) => {
      if (sortBy === "price") return (b.price || 0) - (a.price || 0);
      return 0;  // date — порядок из API
    });

  // Итоговая сумма выбранных (с документами)
  const totalSelected = tenders
    .filter(t => (checkedFiles[t.id]?.size || 0) > 0)
    .reduce((sum, t) => sum + (t.price || 0), 0);

  const totalCheckedFiles = Object.values(checkedFiles)
    .reduce((sum, set) => sum + set.size, 0);

  // ── РЕНДЕР ────────────────────────────────────────────────────────────────
  return (
    <div className="flex flex-col h-full bg-gray-50">

      {/* ── ВЕРХНЯЯ ПАНЕЛЬ ─────────────────────────────────────────────── */}
      <div className="flex items-center justify-between px-6 py-3 bg-white border-b border-gray-200 shadow-sm flex-shrink-0">
        <div className="flex items-center gap-3">
          <div className="p-2 bg-blue-50 rounded-lg">
            <FileText className="w-5 h-5 text-blue-600" />
          </div>
          <div>
            <h1 className="text-lg font-semibold text-gray-900">ИИ Юрист</h1>
            <p className="text-xs text-gray-500">Анализ тендерной документации</p>
          </div>
        </div>

        <div className="flex items-center gap-4">
          {/* Итоговая статистика */}
          <div className="flex items-center gap-6 text-sm">
            <div className="text-center">
              <div className="font-semibold text-gray-900">{tenders.length}</div>
              <div className="text-xs text-gray-500">тендеров</div>
            </div>
            <div className="text-center">
              <div className="font-semibold text-blue-600">{totalCheckedFiles}</div>
              <div className="text-xs text-gray-500">файлов выбрано</div>
            </div>
            {totalSelected > 0 && (
              <div className="text-center">
                <div className="font-semibold text-green-600">{formatPrice(totalSelected)}</div>
                <div className="text-xs text-gray-500">сумма выбранных</div>
              </div>
            )}
          </div>

          {/* Кнопка анализа */}
          <button
            onClick={handleAnalyze}
            disabled={analyzing || totalCheckedFiles === 0}
            className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white text-sm font-medium rounded-lg
                       hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
          >
            {analyzing ? (
              <><Loader2 className="w-4 h-4 animate-spin" /> Анализирую...</>
            ) : (
              <><FileText className="w-4 h-4" /> Анализировать ({totalCheckedFiles})</>
            )}
          </button>
        </div>
      </div>

      {/* ── ОСНОВНАЯ ОБЛАСТЬ ───────────────────────────────────────────────── */}
      <div className="flex flex-1 overflow-hidden">

        {/* ── ЛЕВАЯ ПАНЕЛЬ: СПИСОК ТЕНДЕРОВ ──────────────────────────────── */}
        <div className="w-96 flex-shrink-0 flex flex-col border-r border-gray-200 bg-white">

          {/* Поиск */}
          <div className="p-3 border-b border-gray-100">
            <div className="relative">
              <Search className="absolute left-2.5 top-2.5 w-4 h-4 text-gray-400" />
              <input
                type="text"
                value={searchQuery}
                onChange={e => setSearchQuery(e.target.value)}
                placeholder="Поиск по номеру, названию..."
                className="w-full pl-8 pr-3 py-2 text-sm border border-gray-200 rounded-lg
                           focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
            </div>
          </div>

          {/* Фильтры */}
          <div className="px-3 py-2 border-b border-gray-100 flex items-center gap-2 flex-wrap">
            <div className="flex rounded-md border border-gray-200 overflow-hidden text-xs">
              {(["all","fz44","fz223"] as const).map(v => (
                <button key={v}
                  onClick={() => setFilterLaw(v)}
                  className={`px-2 py-1 transition-colors ${
                    filterLaw === v ? "bg-blue-600 text-white" : "bg-white text-gray-600 hover:bg-gray-50"
                  }`}
                >
                  {v === "all" ? "Все" : v === "fz44" ? "ФЗ-44" : "ФЗ-223"}
                </button>
              ))}
            </div>
            <div className="flex rounded-md border border-gray-200 overflow-hidden text-xs">
              {(["all","has","empty"] as const).map(v => (
                <button key={v}
                  onClick={() => setFilterDocs(v)}
                  className={`px-2 py-1 transition-colors ${
                    filterDocs === v ? "bg-blue-600 text-white" : "bg-white text-gray-600 hover:bg-gray-50"
                  }`}
                >
                  {v === "all" ? "Все" : v === "has" ? "С файлами" : "Без файлов"}
                </button>
              ))}
            </div>
            <button
              onClick={() => setSortBy(s => s === "date" ? "price" : "date")}
              className="ml-auto flex items-center gap-1 text-xs text-gray-500 hover:text-gray-700"
            >
              <ArrowUpDown className="w-3 h-3" />
              {sortBy === "date" ? "По дате" : "По сумме"}
            </button>
          </div>

          {/* Счётчик */}
          <div className="px-3 py-1.5 bg-gray-50 border-b border-gray-100 text-xs text-gray-500">
            Показано: {filtered.length} из {tenders.length}
          </div>

          {/* Список тендеров */}
          <div className="flex-1 overflow-y-auto">
            {filtered.length === 0 ? (
              <div className="flex flex-col items-center justify-center h-40 text-gray-400">
                <Search className="w-8 h-8 mb-2 opacity-50" />
                <p className="text-sm">Тендеры не найдены</p>
              </div>
            ) : (
              filtered.map(tender => {
                const files = tenderFiles[tender.id] || [];
                const checked = checkedFiles[tender.id] || new Set();
                const isSelected = selectedId === tender.id;
                const isRefreshing = refreshingId === tender.id;
                const isFz223 = tender.id.startsWith("223-");

                return (
                  <div
                    key={tender.id}
                    onClick={() => setSelectedId(tender.id)}
                    className={`
                      p-3 border-b border-gray-100 cursor-pointer transition-colors group
                      ${isSelected
                        ? "bg-blue-50 border-l-4 border-l-blue-500"
                        : "hover:bg-gray-50 border-l-4 border-l-transparent"
                      }
                    `}
                  >
                    {/* Строка 1: Бейдж + цена */}
                    <div className="flex items-center justify-between mb-1">
                      <div className="flex items-center gap-1.5">
                        {getLawBadge(tender.id)}
                        {files.length > 0 && (
                          <span className="px-1.5 py-0.5 text-xs rounded bg-green-100 text-green-700">
                            {files.length} файл{files.length > 4 ? "ов" : files.length > 1 ? "а" : ""}
                          </span>
                        )}
                        {files.length === 0 && (
                          <span className="px-1.5 py-0.5 text-xs rounded bg-gray-100 text-gray-500">
                            нет файлов
                          </span>
                        )}
                        {checked.size > 0 && (
                          <span className="px-1.5 py-0.5 text-xs rounded bg-blue-100 text-blue-600">
                            ✓{checked.size}
                          </span>
                        )}
                      </div>
                      {tender.price != null && (
                        <span className="text-xs font-semibold text-gray-800">
                          {formatPrice(tender.price)}
                        </span>
                      )}
                    </div>

                    {/* Строка 2: Номер */}
                    <div className="text-xs font-mono text-gray-500 mb-0.5">
                      № {tender.id.replace("223-", "")}
                    </div>

                    {/* Строка 3: Название */}
                    <div className="text-sm text-gray-800 leading-tight line-clamp-2 mb-1">
                      {tender.title || "Без названия"}
                    </div>

                    {/* Строка 4: Заказчик */}
                    {tender.customer && (
                      <div className="text-xs text-gray-400 truncate">
                        {tender.customer}
                      </div>
                    )}

                    {/* Кнопка обновить (показывается при наведении или выборе) */}
                    <div className="flex items-center justify-between mt-1.5">
                      <button
                        onClick={e => { e.stopPropagation(); handleRefresh(tender.id); }}
                        disabled={isRefreshing}
                        className="flex items-center gap-1 text-xs text-gray-400 hover:text-blue-600 transition-colors disabled:opacity-50"
                      >
                        <RefreshCw className={`w-3 h-3 ${isRefreshing ? "animate-spin" : ""}`} />
                        {isRefreshing ? "Загрузка..." : "Обновить"}
                      </button>
                      {isSelected && (
                        <ChevronRight className="w-3.5 h-3.5 text-blue-500" />
                      )}
                    </div>
                  </div>
                );
              })
            )}
          </div>
        </div>

        {/* ── ПРАВАЯ ПАНЕЛЬ: ДЕТАЛИ ТЕНДЕРА ──────────────────────────────── */}
        <div className="flex-1 flex flex-col overflow-hidden">
          {!selected ? (
            <div className="flex-1 flex flex-col items-center justify-center text-gray-400">
              <FileText className="w-16 h-16 mb-4 opacity-30" />
              <p className="text-lg">Выберите тендер из списка</p>
              <p className="text-sm mt-1">Нажмите на любой тендер слева</p>
            </div>
          ) : (
            <div className="flex-1 flex flex-col overflow-hidden">

              {/* Заголовок тендера */}
              <div className="px-6 py-4 bg-white border-b border-gray-200 flex-shrink-0">
                <div className="flex items-start justify-between gap-4">
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2 mb-1">
                      {getLawBadge(selected.id)}
                      <span className="text-sm font-mono text-gray-500">
                        № {selected.id.replace("223-", "")}
                      </span>
                      {selected.href && (
                        <a
                          href={selected.href}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="text-blue-500 hover:text-blue-700"
                          onClick={e => e.stopPropagation()}
                        >
                          <ExternalLink className="w-3.5 h-3.5" />
                        </a>
                      )}
                    </div>
                    <h2 className="text-base font-semibold text-gray-900 leading-snug">
                      {selected.title}
                    </h2>
                  </div>
                  {selected.price != null && (
                    <div className="text-right flex-shrink-0">
                      <div className="text-xl font-bold text-gray-900">
                        {formatPrice(selected.price)}
                      </div>
                      <div className="text-xs text-gray-500">Начальная цена</div>
                    </div>
                  )}
                </div>

                {/* Мета-данные тендера */}
                <div className="flex items-center gap-4 mt-2 text-xs text-gray-500">
                  {selected.customer && (
                    <span>🏛 {selected.customer}</span>
                  )}
                  {selected.publish_date && (
                    <span>📅 Опубликован: {selected.publish_date}</span>
                  )}
                  {selected.deadline && (
                    <span>⏰ Дедлайн: {selected.deadline}</span>
                  )}
                </div>
              </div>

              {/* Документы */}
              <div className="flex-1 overflow-y-auto p-6">
                <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">

                  {/* Заголовок секции документов */}
                  <div className="flex items-center justify-between px-4 py-3 border-b border-gray-100 bg-gray-50">
                    <div className="flex items-center gap-2">
                      <FileText className="w-4 h-4 text-gray-500" />
                      <span className="text-sm font-medium text-gray-700">
                        Документы
                        {tenderFiles[selected.id] && (
                          <span className="ml-1 text-gray-400">
                            ({tenderFiles[selected.id].length})
                          </span>
                        )}
                      </span>
                    </div>
                    <div className="flex items-center gap-2">
                      <button
                        onClick={() => selectAllFiles(selected.id)}
                        className="flex items-center gap-1 text-xs text-blue-600 hover:text-blue-800"
                      >
                        <CheckCheck className="w-3.5 h-3.5" /> Все
                      </button>
                      <span className="text-gray-300">|</span>
                      <button
                        onClick={() => clearAllFiles(selected.id)}
                        className="flex items-center gap-1 text-xs text-gray-500 hover:text-gray-700"
                      >
                        <X className="w-3.5 h-3.5" /> Снять
                      </button>
                      <span className="text-gray-300">|</span>
                      <button
                        onClick={() => handleRefresh(selected.id)}
                        disabled={refreshingId === selected.id}
                        className="flex items-center gap-1 text-xs text-blue-600 hover:text-blue-800 disabled:opacity-50"
                      >
                        <RefreshCw className={`w-3.5 h-3.5 ${refreshingId === selected.id ? "animate-spin" : ""}`} />
                        {refreshingId === selected.id ? "Загрузка..." : "Обновить"}
                      </button>
                      <span className="text-gray-300">|</span>
                      <label className="flex items-center gap-1 text-xs text-blue-600 hover:text-blue-800 cursor-pointer">
                        <Download className="w-3.5 h-3.5 rotate-180" />
                        Загрузить
                        <input 
                          type="file" 
                          className="hidden" 
                          onChange={(e) => handleFileUpload(selected.id, e.target.files)} 
                        />
                      </label>
                    </div>
                  </div>

                  {/* Список файлов */}
                  {loadingFiles === selected.id ? (
                    <div className="flex items-center justify-center py-12 text-gray-400">
                      <Loader2 className="w-6 h-6 animate-spin mr-2" />
                      Загрузка файлов...
                    </div>
                  ) : !tenderFiles[selected.id] || tenderFiles[selected.id].length === 0 ? (
                    <div className="flex flex-col items-center justify-center py-12 text-gray-400">
                      <AlertCircle className="w-8 h-8 mb-2 opacity-50" />
                      <p className="text-sm">Документы не найдены</p>
                      <button
                        onClick={() => handleRefresh(selected.id)}
                        disabled={refreshingId === selected.id}
                        className="mt-3 flex items-center gap-1.5 px-3 py-1.5 text-sm text-blue-600
                                   border border-blue-300 rounded-lg hover:bg-blue-50 disabled:opacity-50"
                      >
                        <RefreshCw className={`w-4 h-4 ${refreshingId === selected.id ? "animate-spin" : ""}`} />
                        Загрузить документы
                      </button>
                    </div>
                  ) : (
                    <div className="divide-y divide-gray-50">
                      {tenderFiles[selected.id].map((file, idx) => {
                        const isChecked = (checkedFiles[selected.id] || new Set()).has(file.name);
                        return (
                          <div
                            key={idx}
                            onClick={() => toggleFile(selected.id, file.name)}
                            className={`
                              flex items-center gap-3 px-4 py-3 cursor-pointer transition-colors
                              ${isChecked ? "bg-blue-50" : "hover:bg-gray-50"}
                            `}
                          >
                            {/* Чекбокс */}
                            <div className={`flex-shrink-0 w-4 h-4 rounded border-2 flex items-center justify-center
                              ${isChecked ? "bg-blue-600 border-blue-600" : "border-gray-300"}`}
                            >
                              {isChecked && <svg className="w-2.5 h-2.5 text-white" fill="currentColor" viewBox="0 0 12 12"><path d="M10 3L5 8.5 2 5.5" stroke="white" strokeWidth="2" fill="none" strokeLinecap="round"/></svg>}
                            </div>

                            {/* Иконка файла */}
                            <FileIcon className={`w-4 h-4 flex-shrink-0 ${getFileIcon(file.name)}`} />

                            {/* Имя файла */}
                            <div className="flex-1 min-w-0">
                              <div className="text-sm text-gray-800 truncate" title={file.name}>
                                {file.name}
                              </div>
                              <div className="text-xs text-gray-400">
                                {formatSize(file.size)}
                              </div>
                            </div>

                            {/* Кнопка скачать */}
                            {file.url && (
                              <a
                                href={file.url}
                                download={file.name}
                                onClick={e => e.stopPropagation()}
                                className="flex-shrink-0 p-1.5 text-gray-400 hover:text-blue-600 hover:bg-blue-50 rounded transition-colors"
                                title="Скачать"
                              >
                                <Download className="w-3.5 h-3.5" />
                              </a>
                            )}
                          </div>
                        );
                      })}
                    </div>
                  )}

                  {/* Итог выбранных файлов */}
                  {(checkedFiles[selected.id]?.size || 0) > 0 && (
                    <div className="px-4 py-2 bg-blue-50 border-t border-blue-100 flex items-center justify-between">
                      <span className="text-sm text-blue-700">
                        Выбрано: {checkedFiles[selected.id].size} файл(ов) для анализа
                      </span>
                      <button
                        onClick={handleAnalyze}
                        className="px-3 py-1 text-sm bg-blue-600 text-white rounded-lg hover:bg-blue-700"
                      >
                        Анализировать
                      </button>
                    </div>
                  )}
                </div>

                {/* Блок результата анализа */}
                {analysisResult && (
                  <div className="mt-4 bg-white rounded-xl border border-gray-200 p-5">
                    <div className="flex items-center gap-2 mb-3">
                      <FileText className="w-4 h-4 text-blue-600" />
                      <h3 className="text-sm font-semibold text-gray-800">Результат анализа</h3>
                    </div>
                    <div className="text-sm text-gray-700 whitespace-pre-wrap leading-relaxed">
                      {analysisResult}
                    </div>
                  </div>
                )}
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
