import { useState, useEffect, useMemo, useCallback } from "react";
import { logger } from "../services/loggerService";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import {
  FileText,
  RefreshCw,
  Download,
  ChevronRight,
  AlertCircle,
  Search,
  Loader2,
  FileIcon,
  ArrowUpDown,
  CheckCheck,
  X,
  ExternalLink,
  FolderSync,
  Scale,
  ClipboardList,
  CheckCircle2,
  Clock3,
} from "lucide-react";

interface TenderFile {
  name: string;
  size: number;
  url?: string;
  path?: string;
}

interface Tender {
  id: string;
  title: string;
  description: string;
  initial_price: number;
  deadline: string;
  law_type: string;
  url: string;
  customer?: string;
}

interface FileStatus {
  filename: string;
  status: string;
  message?: string;
}

interface JobTenderResult {
  stage?: string;
  progress?: number;
  status?: string;
  final_report_markdown?: string;
  summary_notes?: string;
  error_message?: string;
  file_statuses?: FileStatus[];
  export_available?: boolean;
  report_path?: string;
}

interface JobResponse {
  status: string;
  tenders: Record<string, JobTenderResult>;
}

interface RefreshErrorItem {
  url?: string;
  title?: string;
  error?: string;
  reason?: string;
  message?: string;
  docs_url?: string;
}

const parsePrice = (value: unknown): number => {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value !== "string") return 0;

  const cleaned = value
    .replace(/\s+/g, "")
    .replace(/\u00A0/g, "")
    .replace(/[^\d,.-]/g, "")
    .replace(",", ".");

  const parsed = Number(cleaned);
  return Number.isFinite(parsed) ? parsed : 0;
};

const formatPrice = (value?: number | null): string => {
  if (!value) return "Сумма не указана";
  return new Intl.NumberFormat("ru-RU", {
    style: "currency",
    currency: "RUB",
    maximumFractionDigits: 2,
  }).format(value);
};

const formatSize = (bytes: number): string => {
  if (bytes < 1024) return `${bytes} Б`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(0)} КБ`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} МБ`;
};

const getFileIconColor = (name: string): string => {
  const ext = name.split(".").pop()?.toLowerCase();
  const colors: Record<string, string> = {
    pdf: "text-red-500",
    docx: "text-blue-500",
    doc: "text-blue-500",
    xlsx: "text-green-500",
    xls: "text-green-500",
    zip: "text-yellow-500",
    rar: "text-yellow-500",
  };
  return colors[ext || ""] || "text-gray-400";
};

const normalizeTender = (raw: any): Tender => ({
  id: String(raw?.id ?? ""),
  title: String(raw?.title ?? "Без названия"),
  description: String(raw?.description ?? ""),
  initial_price: parsePrice(raw?.initial_price ?? raw?.price ?? 0),
  deadline: String(raw?.deadline ?? ""),
  law_type: String(raw?.law_type ?? ""),
  url: String(raw?.url ?? raw?.href ?? ""),
  customer: raw?.customer ? String(raw.customer) : "",
});

const getLawBadge = (tender: Tender) => {
  const is223 =
    tender.id.startsWith("223-") ||
    tender.law_type.toLowerCase().includes("223");

  if (is223) {
    return (
      <span className="px-2 py-0.5 text-xs rounded-full bg-purple-100 text-purple-700 font-medium">
        223-ФЗ
      </span>
    );
  }

  return (
    <span className="px-2 py-0.5 text-xs rounded-full bg-blue-100 text-blue-700 font-medium">
      44-ФЗ
    </span>
  );
};

const getResultBadge = (status?: string) => {
  switch (status) {
    case "success":
      return (
        <span className="inline-flex items-center gap-1 px-2.5 py-1 text-xs rounded-full bg-green-100 text-green-700 font-medium">
          <CheckCircle2 className="w-3.5 h-3.5" />
          Готово
        </span>
      );
    case "partial":
      return (
        <span className="inline-flex items-center gap-1 px-2.5 py-1 text-xs rounded-full bg-amber-100 text-amber-700 font-medium">
          <AlertCircle className="w-3.5 h-3.5" />
          Частично
        </span>
      );
    case "error":
      return (
        <span className="inline-flex items-center gap-1 px-2.5 py-1 text-xs rounded-full bg-red-100 text-red-700 font-medium">
          <AlertCircle className="w-3.5 h-3.5" />
          Ошибка
        </span>
      );
    case "running":
      return (
        <span className="inline-flex items-center gap-1 px-2.5 py-1 text-xs rounded-full bg-blue-100 text-blue-700 font-medium">
          <Loader2 className="w-3.5 h-3.5 animate-spin" />
          В работе
        </span>
      );
    default:
      return (
        <span className="inline-flex items-center gap-1 px-2.5 py-1 text-xs rounded-full bg-gray-100 text-gray-600 font-medium">
          <Clock3 className="w-3.5 h-3.5" />
          Нет результата
        </span>
      );
  }
};

const markdownComponents = {
  h1: (props: any) => (
    <h1 className="text-2xl font-bold text-slate-900 mt-1 mb-5" {...props} />
  ),
  h2: (props: any) => (
    <h2 className="text-xl font-semibold text-slate-900 mt-8 mb-4 pb-2 border-b border-slate-200" {...props} />
  ),
  h3: (props: any) => (
    <h3 className="text-lg font-semibold text-slate-800 mt-6 mb-3" {...props} />
  ),
  p: (props: any) => (
    <p className="text-sm leading-7 text-slate-700 mb-4" {...props} />
  ),
  ul: (props: any) => (
    <ul className="list-disc pl-5 text-sm text-slate-700 space-y-2 mb-4" {...props} />
  ),
  ol: (props: any) => (
    <ol className="list-decimal pl-5 text-sm text-slate-700 space-y-2 mb-4" {...props} />
  ),
  li: (props: any) => <li className="leading-6" {...props} />,
  table: (props: any) => (
    <div className="overflow-x-auto rounded-xl border border-slate-200 mb-6">
      <table className="min-w-full text-sm" {...props} />
    </div>
  ),
  thead: (props: any) => <thead className="bg-slate-50" {...props} />,
  th: (props: any) => (
    <th
      className="px-3 py-2 text-left font-semibold text-slate-700 border-b border-slate-200 align-top"
      {...props}
    />
  ),
  td: (props: any) => (
    <td
      className="px-3 py-2 text-slate-700 border-b border-slate-100 align-top whitespace-pre-wrap"
      {...props}
    />
  ),
  blockquote: (props: any) => (
    <blockquote className="border-l-4 border-blue-200 bg-blue-50 px-4 py-3 rounded-r-lg text-sm text-slate-700 mb-4" {...props} />
  ),
  code: (props: any) => (
    <code className="bg-slate-100 text-slate-800 px-1.5 py-0.5 rounded text-[13px]" {...props} />
  ),
};

export default function AnalysisPage() {
  const [tenders, setTenders] = useState<Tender[]>([]);
  const [selectedTenderId, setSelectedTenderId] = useState<string | null>(null);

  const [tenderFiles, setTenderFiles] = useState<Record<string, TenderFile[]>>({});
  const [checkedFiles, setCheckedFiles] = useState<Record<string, Set<string>>>({});
  const [refreshErrors, setRefreshErrors] = useState<Record<string, RefreshErrorItem[]>>({});

  const [loadingFilesId, setLoadingFilesId] = useState<string | null>(null);
  const [refreshingId, setRefreshingId] = useState<string | null>(null);

  const [searchQuery, setSearchQuery] = useState("");
  const [filterLaw, setFilterLaw] = useState<"all" | "fz44" | "fz223">("all");
  const [filterDocs, setFilterDocs] = useState<"all" | "has" | "empty">("all");
  const [sortBy, setSortBy] = useState<"date" | "price">("date");

  const [launchingAnalysis, setLaunchingAnalysis] = useState(false);
  const [currentJobId, setCurrentJobId] = useState<string | null>(null);
  const [jobData, setJobData] = useState<JobResponse | null>(null);
  const [pollError, setPollError] = useState<string>("");
  const [exportingReport, setExportingReport] = useState(false);

  const selectedTender = tenders.find((item) => item.id === selectedTenderId) || null;
  const selectedTenderResult = selectedTenderId ? jobData?.tenders?.[selectedTenderId] : undefined;

  useEffect(() => {
    const loadTenders = async () => {
      try {
        const response = await fetch("/api/crm/tenders");
        const data = await response.json();
        const list = (Array.isArray(data) ? data : []).map(normalizeTender);
        setTenders(list);
        if (list.length > 0) {
          setSelectedTenderId((prev) => prev ?? list[0].id);
        }
      } catch (error) {
        logger.error("Failed to load tenders:", error);
      }
    };

    loadTenders();
  }, []);

  useEffect(() => {
    if (!selectedTenderId) return;
    if (tenderFiles[selectedTenderId] !== undefined) return;

    const loadFiles = async () => {
      setLoadingFilesId(selectedTenderId);
      try {
        const response = await fetch(`/api/tenders/${selectedTenderId}/files`);
        const data = await response.json();
        const files = Array.isArray(data) ? data : data.files || [];
        setTenderFiles((prev) => ({ ...prev, [selectedTenderId]: files }));
      } catch (error) {
        logger.error("Failed to load files:", error);
        setTenderFiles((prev) => ({ ...prev, [selectedTenderId]: [] }));
      } finally {
        setLoadingFilesId(null);
      }
    };

    loadFiles();
  }, [selectedTenderId, tenderFiles]);

  useEffect(() => {
    if (!currentJobId) return;

    let isCancelled = false;

    const poll = async () => {
      try {
        const response = await fetch(`/api/ai/jobs/${currentJobId}`);
        if (!response.ok) {
          throw new Error(`HTTP ${response.status}`);
        }

        const data: JobResponse = await response.json();
        if (isCancelled) return;

        setJobData(data);
        setPollError("");

        if (data.status === "completed") {
          return;
        }

        window.setTimeout(poll, 2000);
      } catch (error) {
        if (isCancelled) return;
        logger.error("Polling error:", error);
        setPollError(String(error));
        window.setTimeout(poll, 3000);
      }
    };

    poll();

    return () => {
      isCancelled = true;
    };
  }, [currentJobId]);

  const filteredTenders = useMemo(() => {
    const source = [...tenders];

    const filtered = source.filter((tender) => {
      const query = searchQuery.trim().toLowerCase();
      if (query) {
        const haystack = [
          tender.id,
          tender.title,
          tender.description,
          tender.customer || "",
        ]
          .join(" ")
          .toLowerCase();

        if (!haystack.includes(query)) {
          return false;
        }
      }

      const is223 =
        tender.id.startsWith("223-") ||
        tender.law_type.toLowerCase().includes("223");

      if (filterLaw === "fz223" && !is223) return false;
      if (filterLaw === "fz44" && is223) return false;

      const fileCount = (tenderFiles[tender.id] || []).length;
      if (filterDocs === "has" && fileCount === 0) return false;
      if (filterDocs === "empty" && fileCount > 0) return false;

      return true;
    });

    if (sortBy === "price") {
      filtered.sort((a, b) => b.initial_price - a.initial_price);
    }

    return filtered;
  }, [tenders, searchQuery, filterLaw, filterDocs, sortBy, tenderFiles]);

  const selectedTenderCount = useMemo(() => {
    return tenders.filter((t) => (checkedFiles[t.id]?.size || 0) > 0).length;
  }, [tenders, checkedFiles]);

  const totalCheckedFiles = useMemo(() => {
    return Object.values(checkedFiles).reduce((sum, set) => sum + set.size, 0);
  }, [checkedFiles]);

  const totalSelectedAmount = useMemo(() => {
    return tenders
      .filter((t) => (checkedFiles[t.id]?.size || 0) > 0)
      .reduce((sum, tender) => sum + (tender.initial_price || 0), 0);
  }, [tenders, checkedFiles]);

  const completedCount = useMemo(() => {
    if (!jobData?.tenders) return 0;
    return Object.values(jobData.tenders).filter((item) =>
      ["success", "partial", "error"].includes(item.status || "")
    ).length;
  }, [jobData]);

  const handleRefresh = useCallback(async (tenderId: string) => {
    setRefreshingId(tenderId);

    try {
      const response = await fetch(`/api/tenders/${tenderId}/refresh-files`, {
        method: "POST",
      });

      const data = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(data?.detail?.message || `HTTP ${response.status}`);
      }

      const files: TenderFile[] = Array.isArray(data) ? data : data.files || [];
      setTenderFiles((prev) => ({ ...prev, [tenderId]: files }));

      if (Array.isArray(data.errors) && data.errors.length > 0) {
        setRefreshErrors((prev) => ({ ...prev, [tenderId]: data.errors }));
      } else {
        setRefreshErrors((prev) => ({ ...prev, [tenderId]: [] }));
      }

      setCheckedFiles((prev) => {
        const selected = new Set(prev[tenderId] || []);
        const availableNames = new Set(files.map((file) => file.name));
        const next = new Set<string>();

        selected.forEach((item) => {
          if (availableNames.has(item)) {
            next.add(item);
          }
        });

        return { ...prev, [tenderId]: next };
      });
    } catch (error) {
      logger.error("Refresh failed:", error);
      setRefreshErrors((prev) => ({
        ...prev,
        [tenderId]: [{ message: String(error) }],
      }));
    } finally {
      setRefreshingId(null);
    }
  }, []);

  const toggleFile = (tenderId: string, fileName: string) => {
    if (launchingAnalysis || (jobData && jobData.status !== "completed")) return;

    setCheckedFiles((prev) => {
      const nextSet = new Set(prev[tenderId] || []);
      if (nextSet.has(fileName)) {
        nextSet.delete(fileName);
      } else {
        nextSet.add(fileName);
      }
      return { ...prev, [tenderId]: nextSet };
    });
  };

  const selectAllFiles = (tenderId: string) => {
    const files = tenderFiles[tenderId] || [];
    setCheckedFiles((prev) => ({
      ...prev,
      [tenderId]: new Set(files.map((item) => item.name)),
    }));
  };

  const clearAllFiles = (tenderId: string) => {
    setCheckedFiles((prev) => ({
      ...prev,
      [tenderId]: new Set<string>(),
    }));
  };

  const handleAnalyze = async () => {
    const selectedMap = Object.fromEntries(
      Object.entries(checkedFiles)
        .filter(([, files]) => files.size > 0)
        .map(([tenderId, files]) => [tenderId, Array.from(files)])
    );

    const tenderIds = Object.keys(selectedMap);

    if (tenderIds.length === 0) {
      alert("Выберите хотя бы один файл для анализа");
      return;
    }

    if (launchingAnalysis || (jobData && jobData.status !== "completed")) {
      return;
    }

    setLaunchingAnalysis(true);
    setPollError("");
    setJobData(null);

    try {
      const response = await fetch("/api/ai/analyze-tenders-batch", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          tender_ids: tenderIds,
          selected_files: selectedMap,
        }),
      });

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({}));
        throw new Error(errorData?.detail || `HTTP ${response.status}`);
      }

      const data = await response.json();
      setCurrentJobId(data.job_id);
    } catch (error) {
      logger.error("Analyze error:", error);
      alert(`Ошибка запуска анализа: ${String(error)}`);
    } finally {
      setLaunchingAnalysis(false);
    }
  };

  const handleExportCurrentReport = async () => {
    if (!selectedTender || !selectedTenderResult?.final_report_markdown) {
      return;
    }

    setExportingReport(true);

    try {
      const response = await fetch("/api/ai/export-risks-word", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          results: [
            {
              id: selectedTender.id,
              description: selectedTender.description || selectedTender.title,
              final_report_markdown: selectedTenderResult.final_report_markdown,
              summary_notes: selectedTenderResult.summary_notes || "",
              file_statuses: selectedTenderResult.file_statuses || [],
            },
          ],
        }),
      });

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }

      const blob = await response.blob();
      const blobUrl = window.URL.createObjectURL(blob);
      const link = document.createElement("a");
      link.href = blobUrl;
      link.download = `report_${selectedTender.id}.docx`;
      document.body.appendChild(link);
      link.click();
      link.remove();
      window.URL.revokeObjectURL(blobUrl);
    } catch (error) {
      logger.error("Export error:", error);
      alert(`Ошибка экспорта отчёта: ${String(error)}`);
    } finally {
      setExportingReport(false);
    }
  };

  const isJobRunning = Boolean(currentJobId && jobData?.status !== "completed");

  return (
    <div className="flex flex-col h-full bg-slate-50">
      <div className="px-6 py-4 bg-white border-b border-slate-200 flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="w-11 h-11 rounded-2xl bg-blue-50 flex items-center justify-center">
            <Scale className="w-5 h-5 text-blue-600" />
          </div>
          <div>
            <h1 className="text-xl font-semibold text-slate-900">ИИ Юрист</h1>
            <p className="text-sm text-slate-500">
              Анализ тендерной документации и экранный отчёт по результатам
            </p>
          </div>
        </div>

        <div className="flex items-center gap-3">
          <div className="grid grid-cols-3 gap-3">
            <div className="min-w-[120px] rounded-xl border border-slate-200 bg-slate-50 px-3 py-2 text-center">
              <div className="text-lg font-semibold text-slate-900">{tenders.length}</div>
              <div className="text-xs text-slate-500">тендеров</div>
            </div>
            <div className="min-w-[120px] rounded-xl border border-blue-200 bg-blue-50 px-3 py-2 text-center">
              <div className="text-lg font-semibold text-blue-700">{totalCheckedFiles}</div>
              <div className="text-xs text-gray-500">файлов выбрано</div>
            </div>
            <div className="min-w-[140px] rounded-xl border border-emerald-200 bg-emerald-50 px-3 py-2 text-center">
              <div className="text-lg font-semibold text-emerald-700">
                {totalSelectedAmount > 0 ? formatPrice(totalSelectedAmount) : "—"}
              </div>
              <div className="text-xs text-emerald-600">сумма выбранных</div>
            </div>
          </div>

          <button
            onClick={handleAnalyze}
            disabled={launchingAnalysis || isJobRunning || totalCheckedFiles === 0}
            className="inline-flex items-center gap-2 px-4 py-2.5 rounded-xl bg-blue-600 text-white text-sm font-medium hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
          >
            {launchingAnalysis || isJobRunning ? (
              <>
                <Loader2 className="w-4 h-4 animate-spin" />
                Анализ выполняется
              </>
            ) : (
              <>
                <FileText className="w-4 h-4" />
                Анализировать ({totalCheckedFiles})
              </>
            )}
          </button>
        </div>
      </div>

      <div className="flex-1 overflow-hidden flex flex-col">
        <div className="flex flex-1 overflow-hidden">
          <div className="w-[420px] flex-shrink-0 flex flex-col border-r border-slate-200 bg-white">
            <div className="p-4 border-b border-slate-100 space-y-3">
              <div className="relative">
                <Search className="w-4 h-4 text-slate-400 absolute left-3 top-3" />
                <input
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  placeholder="Поиск по номеру, названию, описанию"
                  className="w-full pl-9 pr-3 py-2.5 text-sm rounded-xl border border-slate-200 focus:outline-none focus:ring-2 focus:ring-blue-500"
                />
              </div>

              <div className="flex items-center gap-2 flex-wrap">
                <div className="flex rounded-lg border border-slate-200 overflow-hidden text-xs">
                  {(["all", "fz44", "fz223"] as const).map((value) => (
                    <button
                      key={value}
                      onClick={() => setFilterLaw(value)}
                      className={`px-3 py-1.5 transition-colors ${
                        filterLaw === value
                          ? "bg-blue-600 text-white"
                          : "bg-white text-slate-600 hover:bg-slate-50"
                      }`}
                    >
                      {value === "all" ? "Все" : value === "fz44" ? "44-ФЗ" : "223-ФЗ"}
                    </button>
                  ))}
                </div>

                <div className="flex rounded-lg border border-slate-200 overflow-hidden text-xs">
                  {(["all", "has", "empty"] as const).map((value) => (
                    <button
                      key={value}
                      onClick={() => setFilterDocs(value)}
                      className={`px-3 py-1.5 transition-colors ${
                        filterDocs === value
                          ? "bg-blue-600 text-white"
                          : "bg-white text-slate-600 hover:bg-slate-50"
                      }`}
                    >
                      {value === "all" ? "Все" : value === "has" ? "С файлами" : "Без файлов"}
                    </button>
                  ))}
                </div>

                <button
                  onClick={() => setSortBy((prev) => (prev === "date" ? "price" : "date"))}
                  className="ml-auto inline-flex items-center gap-1 text-xs text-slate-500 hover:text-slate-700"
                >
                  <ArrowUpDown className="w-3.5 h-3.5" />
                  {sortBy === "date" ? "По дате" : "По сумме"}
                </button>
              </div>

              <div className="text-xs text-slate-500">
                Показано: {filteredTenders.length} из {tenders.length} · Выбрано тендеров: {selectedTenderCount}
              </div>
            </div>

            <div className="flex-1 overflow-y-auto">
              {filteredTenders.length === 0 ? (
                <div className="h-48 flex flex-col items-center justify-center text-slate-400">
                  <Search className="w-8 h-8 opacity-40 mb-2" />
                  <div className="text-sm">Тендеры не найдены</div>
                </div>
              ) : (
                filteredTenders.map((tender) => {
                  const files = tenderFiles[tender.id] || [];
                  const checkedCount = checkedFiles[tender.id]?.size || 0;
                  const result = jobData?.tenders?.[tender.id];
                  const isSelected = selectedTenderId === tender.id;

                  return (
                    <button
                      key={tender.id}
                      onClick={() => setSelectedTenderId(tender.id)}
                      className={`w-full text-left p-4 border-b border-slate-100 transition-colors ${
                        isSelected ? "bg-blue-50 border-l-4 border-l-blue-600" : "hover:bg-slate-50 border-l-4 border-l-transparent"
                      }`}
                    >
                      <div className="flex items-start justify-between gap-3 mb-2">
                        <div className="flex items-center gap-2 flex-wrap">
                          {getLawBadge(tender)}
                          {files.length > 0 ? (
                            <span className="px-2 py-0.5 text-xs rounded-full bg-emerald-100 text-emerald-700">
                              {files.length} файл(ов)
                            </span>
                          ) : (
                            <span className="px-2 py-0.5 text-xs rounded-full bg-slate-100 text-slate-500">
                              без файлов
                            </span>
                          )}
                          {checkedCount > 0 && (
                            <span className="px-2 py-0.5 text-xs rounded-full bg-blue-100 text-blue-700">
                              выбрано {checkedCount}
                            </span>
                          )}
                        </div>
                        {result ? getResultBadge(result.status) : null}
                      </div>

                      <div className="text-xs font-mono text-slate-500 mb-1">
                        № {tender.id.replace("223-", "")}
                      </div>

                      <div className="text-sm font-medium text-slate-900 leading-5 mb-2">
                        {tender.title}
                      </div>

                      {tender.description ? (
                        <div className="text-xs text-slate-500 leading-5 line-clamp-3 mb-2">
                          {tender.description}
                        </div>
                      ) : null}

                      <div className="flex items-center justify-between gap-3 text-xs text-slate-500">
                        <span className="font-semibold text-slate-700">
                          {formatPrice(tender.initial_price)}
                        </span>
                        <span className="truncate">
                          {tender.deadline ? `Срок: ${tender.deadline}` : "Срок не указан"}
                        </span>
                      </div>

                      {isSelected ? (
                        <div className="mt-2 flex justify-end">
                          <ChevronRight className="w-4 h-4 text-blue-600" />
                        </div>
                      ) : null}
                    </button>
                  );
                })
              )}
            </div>
          </div>

          <div className="flex-1 overflow-hidden flex flex-col">
            {!selectedTender ? (
              <div className="flex-1 flex flex-col items-center justify-center text-slate-400">
                <ClipboardList className="w-14 h-14 opacity-40 mb-3" />
                <div className="text-lg">Выберите тендер из списка</div>
                <div className="text-sm mt-1">После этого можно обновить документы и запустить анализ</div>
              </div>
            ) : (
              <>
                <div className="px-6 py-5 bg-white border-b border-slate-200">
                  <div className="flex items-start justify-between gap-4">
                    <div className="min-w-0">
                      <div className="flex items-center gap-2 mb-2 flex-wrap">
                        {getLawBadge(selectedTender)}
                        <span className="text-sm font-mono text-slate-500">
                          № {selectedTender.id.replace("223-", "")}
                        </span>
                        {selectedTender.url ? (
                          <a
                            href={selectedTender.url}
                            target="_blank"
                            rel="noopener noreferrer"
                            className="inline-flex items-center gap-1 text-sm text-blue-600 hover:text-blue-800"
                          >
                            <ExternalLink className="w-4 h-4" />
                            Открыть в ЕИС
                          </a>
                        ) : null}
                        {selectedTenderResult ? getResultBadge(selectedTenderResult.status) : null}
                      </div>

                      <h2 className="text-xl font-semibold text-slate-900 leading-7">
                        {selectedTender.title}
                      </h2>

                      {selectedTender.description ? (
                        <p className="mt-2 text-sm text-slate-600 leading-6 max-w-4xl">
                          {selectedTender.description}
                        </p>
                      ) : null}

                      <div className="mt-3 flex items-center gap-5 text-sm text-slate-500 flex-wrap">
                        <span>Начальная цена: <span className="font-semibold text-slate-800">{formatPrice(selectedTender.initial_price)}</span></span>
                        <span>Дедлайн: <span className="font-semibold text-slate-800">{selectedTender.deadline || "не указан"}</span></span>
                        {currentJobId ? (
                          <span>
                            Job ID: <span className="font-mono text-slate-700">{currentJobId}</span>
                          </span>
                        ) : null}
                      </div>
                    </div>

                    {selectedTenderResult?.final_report_markdown ? (
                      <button
                        onClick={handleExportCurrentReport}
                        disabled={exportingReport}
                        className="inline-flex items-center gap-2 px-4 py-2 rounded-xl border border-slate-200 bg-white text-sm font-medium text-slate-700 hover:bg-slate-50 disabled:opacity-50"
                      >
                        {exportingReport ? (
                          <>
                            <Loader2 className="w-4 h-4 animate-spin" />
                            Экспорт...
                          </>
                        ) : (
                          <>
                            <Download className="w-4 h-4" />
                            Скачать DOCX
                          </>
                        )}
                      </button>
                    ) : null}
                  </div>
                </div>

                <div className="flex-1 overflow-auto p-6 space-y-6">
                  {currentJobId ? (
                    <div className="grid grid-cols-4 gap-4">
                      <div className="rounded-2xl bg-white border border-slate-200 p-4">
                        <div className="text-xs uppercase tracking-wide text-slate-400 mb-2">Статус job</div>
                        <div className="text-lg font-semibold text-slate-900">
                          {jobData?.status === "completed" ? "Завершён" : "Выполняется"}
                        </div>
                      </div>
                      <div className="rounded-2xl bg-white border border-slate-200 p-4">
                        <div className="text-xs uppercase tracking-wide text-slate-400 mb-2">Готово тендеров</div>
                        <div className="text-lg font-semibold text-slate-900">
                          {completedCount} / {Object.keys(jobData?.tenders || {}).length || 0}
                        </div>
                      </div>
                      <div className="rounded-2xl bg-white border border-slate-200 p-4">
                        <div className="text-xs uppercase tracking-wide text-slate-400 mb-2">Этап</div>
                        <div className="text-lg font-semibold text-slate-900">
                          {selectedTenderResult?.stage || "Ожидание"}
                        </div>
                      </div>
                      <div className="rounded-2xl bg-white border border-slate-200 p-4">
                        <div className="text-xs uppercase tracking-wide text-slate-400 mb-2">Прогресс</div>
                        <div className="text-lg font-semibold text-slate-900">
                          {selectedTenderResult?.progress || 0}%
                        </div>
                      </div>
                    </div>
                  ) : null}

                  {pollError ? (
                    <div className="rounded-2xl border border-red-200 bg-red-50 p-4 text-sm text-red-700">
                      Ошибка опроса статуса job: {pollError}
                    </div>
                  ) : null}

                  <div className="grid grid-cols-12 gap-6">
                    <div className="col-span-4 space-y-6">
                      <div className="bg-white rounded-2xl border border-slate-200 overflow-hidden">
                        <div className="px-4 py-3 border-b border-slate-100 bg-slate-50 flex items-center justify-between">
                          <div className="flex items-center gap-2">
                            <FolderSync className="w-4 h-4 text-slate-500" />
                            <span className="text-sm font-semibold text-slate-800">
                              Документы тендера ({(tenderFiles[selectedTender.id] || []).length})
                            </span>
                          </div>

                          <div className="flex items-center gap-2">
                            <button
                              onClick={() => selectAllFiles(selectedTender.id)}
                              className="inline-flex items-center gap-1 text-xs text-blue-600 hover:text-blue-800"
                              disabled={isJobRunning}
                            >
                              <CheckCheck className="w-3.5 h-3.5" />
                              Все
                            </button>
                            <span className="text-slate-300">|</span>
                            <button
                              onClick={() => clearAllFiles(selectedTender.id)}
                              className="inline-flex items-center gap-1 text-xs text-slate-500 hover:text-slate-700"
                              disabled={isJobRunning}
                            >
                              <X className="w-3.5 h-3.5" />
                              Снять
                            </button>
                            <span className="text-slate-300">|</span>
                            <button
                              onClick={() => handleRefresh(selectedTender.id)}
                              disabled={refreshingId === selectedTender.id}
                              className="inline-flex items-center gap-1 text-xs text-blue-600 hover:text-blue-800 disabled:opacity-50"
                            >
                              <RefreshCw className={`w-3.5 h-3.5 ${refreshingId === selectedTender.id ? "animate-spin" : ""}`} />
                              {refreshingId === selectedTender.id ? "Обновление..." : "Обновить"}
                            </button>
                          </div>
                        </div>

                        {loadingFilesId === selectedTender.id ? (
                          <div className="py-10 flex items-center justify-center text-slate-400">
                            <Loader2 className="w-5 h-5 animate-spin mr-2" />
                            Загрузка файлов...
                          </div>
                        ) : (tenderFiles[selectedTender.id] || []).length === 0 ? (
                          <div className="py-10 px-4 flex flex-col items-center justify-center text-slate-400">
                            <AlertCircle className="w-8 h-8 mb-2 opacity-50" />
                            <div className="text-sm">Файлы не найдены</div>
                          </div>
                        ) : (
                          <div className="divide-y divide-slate-100">
                            {(tenderFiles[selectedTender.id] || []).map((file) => {
                              const isChecked = checkedFiles[selectedTender.id]?.has(file.name) || false;

                              return (
                                <button
                                  key={file.name}
                                  onClick={() => toggleFile(selectedTender.id, file.name)}
                                  className={`w-full flex items-center gap-3 px-4 py-3 text-left transition-colors ${
                                    isChecked ? "bg-blue-50" : "hover:bg-slate-50"
                                  }`}
                                >
                                  <div
                                    className={`w-4 h-4 rounded border-2 flex items-center justify-center ${
                                      isChecked ? "bg-blue-600 border-blue-600" : "border-slate-300"
                                    }`}
                                  >
                                    {isChecked ? (
                                      <svg className="w-2.5 h-2.5 text-white" fill="currentColor" viewBox="0 0 12 12">
                                        <path d="M10 3L5 8.5 2 5.5" stroke="white" strokeWidth="2" fill="none" strokeLinecap="round" />
                                      </svg>
                                    ) : null}
                                  </div>

                                  <FileIcon className={`w-4 h-4 flex-shrink-0 ${getFileIconColor(file.name)}`} />

                                  <div className="min-w-0 flex-1">
                                    <div className="text-sm text-slate-800 truncate">{file.name}</div>
                                    <div className="text-xs text-slate-400">{formatSize(file.size)}</div>
                                  </div>
                                </button>
                              );
                            })}
                          </div>
                        )}

                        {(checkedFiles[selectedTender.id]?.size || 0) > 0 ? (
                          <div className="px-4 py-3 bg-blue-50 border-t border-blue-100 flex items-center justify-between">
                            <span className="text-sm text-blue-700">
                              Выбрано файлов: {checkedFiles[selectedTender.id]?.size || 0}
                            </span>
                            <button
                              onClick={handleAnalyze}
                              disabled={launchingAnalysis || isJobRunning}
                              className="inline-flex items-center gap-2 px-3 py-1.5 rounded-lg bg-blue-600 text-white text-sm hover:bg-blue-700 disabled:opacity-50"
                            >
                              {launchingAnalysis || isJobRunning ? (
                                <>
                                  <Loader2 className="w-4 h-4 animate-spin" />
                                  Выполняется
                                </>
                              ) : (
                                <>
                                  <FileText className="w-4 h-4" />
                                  Анализировать
                                </>
                              )}
                            </button>
                          </div>
                        ) : null}
                      </div>

                      {refreshErrors[selectedTender.id]?.length ? (
                        <div className="bg-white rounded-2xl border border-amber-200 overflow-hidden">
                          <div className="px-4 py-3 border-b border-amber-100 bg-amber-50 flex items-center gap-2">
                            <AlertCircle className="w-4 h-4 text-amber-600" />
                            <span className="text-sm font-semibold text-amber-800">
                              Предупреждения при скачивании
                            </span>
                          </div>
                          <div className="p-4 space-y-3">
                            {refreshErrors[selectedTender.id].map((item, index) => (
                              <div key={index} className="rounded-xl border border-amber-100 bg-amber-50/50 p-3">
                                <div className="text-sm font-medium text-amber-900">
                                  {item.title || item.reason || item.docs_url || "Ошибка скачивания"}
                                </div>
                                <div className="text-xs text-amber-700 mt-1 whitespace-pre-wrap">
                                  {item.error || item.message || "Подробности отсутствуют"}
                                </div>
                              </div>
                            ))}
                          </div>
                        </div>
                      ) : null}

                      {selectedTenderResult?.file_statuses?.length ? (
                        <div className="bg-white rounded-2xl border border-slate-200 overflow-hidden">
                          <div className="px-4 py-3 border-b border-slate-100 bg-slate-50 flex items-center gap-2">
                            <ClipboardList className="w-4 h-4 text-slate-500" />
                            <span className="text-sm font-semibold text-slate-800">Статусы обработки файлов</span>
                          </div>
                          <div className="divide-y divide-slate-100">
                            {selectedTenderResult.file_statuses.map((item) => (
                              <div key={`${item.filename}-${item.status}`} className="px-4 py-3">
                                <div className="flex items-center justify-between gap-3">
                                  <div className="text-sm font-medium text-slate-800 break-all">
                                    {item.filename}
                                  </div>
                                  {getResultBadge(item.status)}
                                </div>
                                {item.message ? (
                                  <div className="text-xs text-slate-500 mt-1 whitespace-pre-wrap">
                                    {item.message}
                                  </div>
                                ) : null}
                              </div>
                            ))}
                          </div>
                        </div>
                      ) : null}
                    </div>

                    <div className="col-span-8 space-y-6">
                      {selectedTenderResult ? (
                        <>
                          <div className="grid grid-cols-4 gap-4">
                            <div className="rounded-2xl bg-white border border-slate-200 p-4">
                              <div className="text-xs uppercase tracking-wide text-slate-400 mb-2">Статус</div>
                              <div>{getResultBadge(selectedTenderResult.status)}</div>
                            </div>
                            <div className="rounded-2xl bg-white border border-slate-200 p-4">
                              <div className="text-xs uppercase tracking-wide text-slate-400 mb-2">Этап</div>
                              <div className="text-sm font-semibold text-slate-900">
                                {selectedTenderResult.stage || "Ожидание"}
                              </div>
                            </div>
                            <div className="rounded-2xl bg-white border border-slate-200 p-4">
                              <div className="text-xs uppercase tracking-wide text-slate-400 mb-2">Прогресс</div>
                              <div className="text-sm font-semibold text-slate-900">
                                {selectedTenderResult.progress || 0}%
                              </div>
                              <div className="mt-2 h-2 rounded-full bg-slate-100 overflow-hidden">
                                <div
                                  className="h-full bg-blue-600 rounded-full transition-all"
                                  style={{ width: `${selectedTenderResult.progress || 0}%` }}
                                />
                              </div>
                            </div>
                            <div className="rounded-2xl bg-white border border-slate-200 p-4">
                              <div className="text-xs uppercase tracking-wide text-slate-400 mb-2">Файлы</div>
                              <div className="text-sm font-semibold text-slate-900">
                                {selectedTenderResult.file_statuses?.length || 0} шт.
                              </div>
                            </div>
                          </div>

                          {selectedTenderResult.summary_notes ? (
                            <div className="bg-white rounded-2xl border border-slate-200 p-5">
                              <div className="flex items-center gap-2 mb-3">
                                <CheckCircle2 className="w-4 h-4 text-emerald-600" />
                                <h3 className="text-sm font-semibold text-slate-800">Краткое резюме</h3>
                              </div>
                              <div className="text-sm text-slate-700 leading-7 whitespace-pre-wrap">
                                {selectedTenderResult.summary_notes}
                              </div>
                            </div>
                          ) : null}

                          {selectedTenderResult.error_message ? (
                            <div className="bg-white rounded-2xl border border-red-200 p-5">
                              <div className="flex items-center gap-2 mb-3">
                                <AlertCircle className="w-4 h-4 text-red-600" />
                                <h3 className="text-sm font-semibold text-red-800">Сообщение об ошибке</h3>
                              </div>
                              <div className="text-sm text-red-700 whitespace-pre-wrap">
                                {selectedTenderResult.error_message}
                              </div>
                            </div>
                          ) : null}

                          {selectedTenderResult.final_report_markdown ? (
                            <div className="bg-white rounded-2xl border border-slate-200 overflow-hidden">
                              <div className="px-5 py-4 border-b border-slate-100 bg-slate-50 flex items-center justify-between">
                                <div className="flex items-center gap-2">
                                  <FileText className="w-4 h-4 text-blue-600" />
                                  <h3 className="text-sm font-semibold text-slate-800">
                                    Экранный отчёт ИИ-юриста
                                  </h3>
                                </div>

                                <button
                                  onClick={handleExportCurrentReport}
                                  disabled={exportingReport}
                                  className="inline-flex items-center gap-2 px-3 py-1.5 rounded-lg border border-slate-200 bg-white text-sm text-slate-700 hover:bg-slate-50 disabled:opacity-50"
                                >
                                  {exportingReport ? (
                                    <>
                                      <Loader2 className="w-4 h-4 animate-spin" />
                                      Экспорт...
                                    </>
                                  ) : (
                                    <>
                                      <Download className="w-4 h-4" />
                                      DOCX
                                    </>
                                  )}
                                </button>
                              </div>

                              <div className="p-6">
                                <ReactMarkdown
                                  remarkPlugins={[remarkGfm]}
                                  components={markdownComponents}
                                >
                                  {selectedTenderResult.final_report_markdown}
                                </ReactMarkdown>
                              </div>
                            </div>
                          ) : (
                            <div className="bg-white rounded-2xl border border-slate-200 p-6 text-center text-slate-400">
                              <FileText className="w-8 h-8 mx-auto mb-3 opacity-40" />
                              <div className="text-sm">
                                Отчёт ещё не сформирован. Дождитесь завершения анализа.
                              </div>
                            </div>
                          )}
                        </>
                      ) : (
                        <div className="bg-white rounded-2xl border border-slate-200 p-10 text-center text-slate-400">
                          <Scale className="w-10 h-10 mx-auto mb-3 opacity-40" />
                          <div className="text-lg text-slate-600">Результат для выбранного тендера пока не загружен</div>
                          <div className="text-sm mt-2">
                            Выберите файлы слева и запустите анализ. После этого отчёт появится здесь.
                          </div>
                        </div>
                      )}
                    </div>
                  </div>
                </div>
              </>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
