import React, { useEffect, useRef, useState } from 'react';
import { logger } from "../services/loggerService";
import { Search, Play, CheckCircle, ExternalLink, AlertCircle, Loader2, Briefcase, XCircle, CheckSquare, Square, Upload } from 'lucide-react';
import { useNavigate } from 'react-router-dom';
import { MOCK_CATALOG } from './ProductCatalog';
import { searchTenders, getTendersFromBackend, processSelectedTenders, cancelSearch, uploadManualTenderDocuments } from '../services/geminiService';
import { Tender } from '../types';

type TenderSearchGroup = {
  id: string;
  label: string;
  query: string;
  tenders: Tender[];
  newCount: number;
  newTenderIds: string[];
  rawCount: number;
  duplicatesExcluded: number;
  error?: string;
};

type PersistedTenderSearchState = {
  query: string;
  keywordQuery: string;
  searchMode: 'keyword' | 'catalog';
  isActive: boolean;
  fz44: boolean;
  fz223: boolean;
  publishDaysBack: number;
  resultGroups: TenderSearchGroup[];
  activeGroupId: string | null;
};

const SEARCH_STATE_STORAGE_KEY = 'TENDER_SEARCH_STATE_V2';

const defaultPersistedSearchState: PersistedTenderSearchState = {
  query: '',
  keywordQuery: '',
  searchMode: 'keyword',
  isActive: true,
  fz44: true,
  fz223: true,
  publishDaysBack: 30,
  resultGroups: [],
  activeGroupId: null,
};

const loadPersistedSearchState = (): PersistedTenderSearchState => {
  if (typeof window === 'undefined') {
    return defaultPersistedSearchState;
  }

  try {
    const raw = localStorage.getItem(SEARCH_STATE_STORAGE_KEY);
    if (!raw) {
      return defaultPersistedSearchState;
    }

    const parsed = JSON.parse(raw);
    const resultGroups = Array.isArray(parsed?.resultGroups)
      ? parsed.resultGroups.map((group: any): TenderSearchGroup => ({
          id: typeof group?.id === 'string' ? group.id : '',
          label: typeof group?.label === 'string' ? group.label : '',
          query: typeof group?.query === 'string' ? group.query : '',
          tenders: Array.isArray(group?.tenders) ? group.tenders : [],
          newCount: typeof group?.newCount === 'number' ? group.newCount : 0,
          newTenderIds: Array.isArray(group?.newTenderIds) ? group.newTenderIds.filter((item: unknown) => typeof item === 'string') : [],
          rawCount: typeof group?.rawCount === 'number' ? group.rawCount : 0,
          duplicatesExcluded: typeof group?.duplicatesExcluded === 'number' ? group.duplicatesExcluded : 0,
          error: typeof group?.error === 'string' ? group.error : undefined,
        }))
      : defaultPersistedSearchState.resultGroups;

    return {
      query: typeof parsed?.keywordQuery === 'string'
        ? parsed.keywordQuery
        : typeof parsed?.query === 'string'
          ? parsed.query
          : defaultPersistedSearchState.query,
      keywordQuery: typeof parsed?.keywordQuery === 'string'
        ? parsed.keywordQuery
        : typeof parsed?.query === 'string'
          ? parsed.query
          : defaultPersistedSearchState.keywordQuery,
      searchMode: parsed?.searchMode === 'catalog' ? 'catalog' : 'keyword',
      isActive: typeof parsed?.isActive === 'boolean' ? parsed.isActive : defaultPersistedSearchState.isActive,
      fz44: typeof parsed?.fz44 === 'boolean' ? parsed.fz44 : defaultPersistedSearchState.fz44,
      fz223: typeof parsed?.fz223 === 'boolean' ? parsed.fz223 : defaultPersistedSearchState.fz223,
      publishDaysBack: typeof parsed?.publishDaysBack === 'number' ? parsed.publishDaysBack : defaultPersistedSearchState.publishDaysBack,
      resultGroups,
      activeGroupId: typeof parsed?.activeGroupId === 'string' ? parsed.activeGroupId : null,
    };
  } catch {
    return defaultPersistedSearchState;
  }
};

const parseKeywordList = (raw: string): string[] => {
  const uniqueKeywords = new Set<string>();
  const keywords: string[] = [];

  for (const part of raw.split(/[,\n;]+/)) {
    const keyword = part.replace(/\s+/g, ' ').trim();
    const normalized = keyword.toLowerCase();

    if (!keyword || uniqueKeywords.has(normalized)) {
      continue;
    }

    uniqueKeywords.add(normalized);
    keywords.push(keyword);
  }

  return keywords;
};

const getTenderUniqueId = (tender: Pick<Tender, 'id' | 'eis_number'>): string => {
  return String(tender.id || tender.eis_number || '').trim();
};

const sortTendersForDisplay = (tenders: Tender[], newTenderIds: string[]): Tender[] => {
  const newTenderSet = new Set(newTenderIds);
  return [...tenders].sort((left, right) => {
    const leftIsNew = newTenderSet.has(getTenderUniqueId(left));
    const rightIsNew = newTenderSet.has(getTenderUniqueId(right));

    if (leftIsNew !== rightIsNew) {
      return leftIsNew ? -1 : 1;
    }

    return 0;
  });
};

const TenderSearch = () => {
  const initialState = React.useMemo(loadPersistedSearchState, []);
  const navigate = useNavigate();
  const [keywordQuery, setKeywordQuery] = useState(initialState.keywordQuery);
  const [resultGroups, setResultGroups] = useState<TenderSearchGroup[]>(initialState.resultGroups);
  const [activeGroupId, setActiveGroupId] = useState<string | null>(initialState.activeGroupId);
  const [loading, setLoading] = useState(false);
  const [loadingStatus, setLoadingStatus] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [processing, setProcessing] = useState(false);
  const [searchMode, setSearchMode] = useState<'keyword' | 'catalog'>(initialState.searchMode);
  const [isActive, setIsActive] = useState(initialState.isActive);
  const [fz44, setFz44] = useState(initialState.fz44);
  const [fz223, setFz223] = useState(initialState.fz223);
  const [publishDaysBack, setPublishDaysBack] = useState(initialState.publishDaysBack);
  const [selectedTenders, setSelectedTenders] = useState<Tender[]>([]);

  const [crmTenders, setCrmTenders] = useState<Tender[]>([]);
  const [submittingTenderId, setSubmittingTenderId] = useState<string | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);
  const [manualUploadLoading, setManualUploadLoading] = useState(false);
  const [manualUploadError, setManualUploadError] = useState<string | null>(null);
  const abortControllerRef = useRef<AbortController | null>(null);
  const manualUploadRef = useRef<HTMLInputElement | null>(null);

  useEffect(() => {
    refreshCrmTenders().catch(logger.error);
  }, []);

  useEffect(() => {
    try {
      localStorage.setItem(
        SEARCH_STATE_STORAGE_KEY,
        JSON.stringify({
          query: keywordQuery,
          keywordQuery,
          searchMode,
          isActive,
          fz44,
          fz223,
          publishDaysBack,
          resultGroups,
          activeGroupId,
        } satisfies PersistedTenderSearchState)
      );
    } catch {
      // Ignore localStorage failures in restricted environments.
    }
  }, [keywordQuery, searchMode, isActive, fz44, fz223, publishDaysBack, resultGroups, activeGroupId]);

  const refreshCrmTenders = async () => {
    const fresh = await getTendersFromBackend();
    setCrmTenders(fresh);
  };

  const activeGroup = resultGroups.find((group) => group.id === activeGroupId) || resultGroups[0] || null;
  const visibleResults = activeGroup ? sortTendersForDisplay(activeGroup.tenders, activeGroup.newTenderIds) : [];
  const totalVisibleResults = resultGroups.reduce((total, group) => total + group.tenders.length, 0);
  const totalDuplicatesExcluded = resultGroups.reduce((total, group) => total + group.duplicatesExcluded, 0);
  const totalNewTenders = resultGroups.reduce((total, group) => total + group.newCount, 0);

  const isInCrm = (id: string) => crmTenders.some(t => t.id === id);
  const isSelected = (id: string) => selectedTenders.some(t => t.id === id);

  const handleSendToWork = async (tender: Tender) => {
    if (submittingTenderId || processing) return;
    setSubmittingTenderId(tender.id);
    setActionError(null);
    try {
      await processSelectedTenders([tender]);
      await refreshCrmTenders();
    } catch (err: any) {
      setActionError(err.message || "Ошибка при отправке в CRM");
    } finally {
      setSubmittingTenderId(null);
    }
  };

  const toggleTenderSelection = (tender: Tender) => {
    if (tender.id === 'err_msg') return;
    if (loading || processing) return;

    if (isSelected(tender.id)) {
      setSelectedTenders(prev => prev.filter(t => t.id !== tender.id));
    } else {
      setSelectedTenders(prev => [...prev, tender]);
    }
  };

  const handleProcessSelected = async () => {
    if (processing || loading) return;
    if (selectedTenders.length === 0) return;

    const snapshot = [...selectedTenders];
    setProcessing(true);

    try {
      await processSelectedTenders(snapshot);

      setCrmTenders(prev => {
        const existingIds = new Set(prev.map(t => t.id));
        const toAdd = snapshot
          .filter(t => !existingIds.has(t.id))
          .map(t => ({ ...t, status: 'Found' as const }));
        return [...prev, ...toAdd];
      });

      setSelectedTenders(prev =>
        prev.filter(t => !snapshot.some(s => s.id === t.id))
      );
    } catch (searchError: unknown) {
      logger.error(String(searchError));
    } finally {
      setProcessing(false);
    }
  };

  const handleManualUpload = async (event: React.ChangeEvent<HTMLInputElement>) => {
    const files = Array.from(event.target.files || []);
    event.target.value = '';

    if (files.length === 0 || manualUploadLoading || loading || processing) {
      return;
    }

    setManualUploadLoading(true);
    setManualUploadError(null);
    setActionError(null);

    try {
      const tender = await uploadManualTenderDocuments(files);
      const groupId = `manual-${tender.id}-${Date.now()}`;
      const tenderId = getTenderUniqueId(tender);

      const group: TenderSearchGroup = {
        id: groupId,
        label: 'Ручная загрузка',
        query: files.map(file => file.name).join(', '),
        tenders: [tender],
        newCount: 1,
        newTenderIds: tenderId ? [tenderId] : [],
        rawCount: 1,
        duplicatesExcluded: 0,
      };

      setResultGroups(prev => [group, ...prev]);
      setActiveGroupId(groupId);
      setSelectedTenders([]);
    } catch (err: any) {
      setManualUploadError(err.message || 'Не удалось создать тендер из документации');
    } finally {
      setManualUploadLoading(false);
    }
  };

  const handleSearch = async () => {
    if (loading || processing) return;

    const controller = new AbortController();
    abortControllerRef.current = controller;
    const previousResultGroupsSnapshot = [...resultGroups];
    const previousTenderIds = new Set(
      previousResultGroupsSnapshot.flatMap((group) =>
        group.tenders
          .map(getTenderUniqueId)
          .filter(Boolean)
      )
    );

    setLoading(true);
    setLoadingStatus(null);
    setActiveGroupId(null);
    setSelectedTenders([]);
    setError(null);
    setActionError(null);

    try {
      const catalogContext = MOCK_CATALOG.map(p => `${p.title} (${p.category})`).join(', ');

      if (searchMode === 'catalog') {
        setLoadingStatus('Поиск тендеров по каталогу материалов');

        const catalogQuery = `Найти тендеры, где требуются товары/материалы из списка: ${catalogContext}.`;
        const tenders = await searchTenders(
          catalogQuery,
          catalogContext,
          isActive,
          fz44,
          fz223,
          publishDaysBack,
          controller.signal
        );

        if (controller.signal.aborted) {
          return;
        }

        const group: TenderSearchGroup = {
          id: 'catalog',
          label: 'Каталог',
          query: catalogQuery,
          tenders,
          newCount: tenders.filter((tender) => !previousTenderIds.has(getTenderUniqueId(tender))).length,
          newTenderIds: tenders
            .map(getTenderUniqueId)
            .filter((tenderId) => Boolean(tenderId) && !previousTenderIds.has(tenderId)),
          rawCount: tenders.length,
          duplicatesExcluded: 0,
        };

        setResultGroups([group]);
        setActiveGroupId(group.id);
        return;
      }

      const keywords = parseKeywordList(keywordQuery);
      if (keywords.length === 0) {
        setError('Укажите хотя бы одно ключевое слово через запятую.');
        return;
      }

      const seenTenderIds = new Set<string>();
      const nextGroups: TenderSearchGroup[] = [];
      const failedKeywords: string[] = [];

      for (let index = 0; index < keywords.length; index += 1) {
        const keyword = keywords[index];

        if (controller.signal.aborted) {
          break;
        }

        setLoadingStatus(`Поиск ${index + 1}/${keywords.length}: ${keyword}`);

        try {
          const tenders = await searchTenders(
            keyword,
            catalogContext,
            isActive,
            fz44,
            fz223,
            publishDaysBack,
            controller.signal
          );

          if (controller.signal.aborted) {
            break;
          }

          const uniqueTenders = tenders.filter((tender) => {
            const tenderId = String(tender.id || tender.eis_number || '').trim();
            if (!tenderId) {
              return true;
            }
            if (seenTenderIds.has(tenderId)) {
              return false;
            }
            seenTenderIds.add(tenderId);
            return true;
          });

          const group: TenderSearchGroup = {
            id: `keyword-${index}`,
            label: keyword,
            query: keyword,
            tenders: uniqueTenders,
            newCount: uniqueTenders.filter((tender) => !previousTenderIds.has(getTenderUniqueId(tender))).length,
            newTenderIds: uniqueTenders
              .map(getTenderUniqueId)
              .filter((tenderId) => Boolean(tenderId) && !previousTenderIds.has(tenderId)),
            rawCount: tenders.length,
            duplicatesExcluded: Math.max(0, tenders.length - uniqueTenders.length),
          };

          nextGroups.push(group);
          setResultGroups([...nextGroups]);
          setActiveGroupId(prev => prev ?? group.id);
        } catch (searchError: any) {
          if (controller.signal.aborted || searchError?.name === 'AbortError') {
            break;
          }

          const group: TenderSearchGroup = {
            id: `keyword-${index}`,
            label: keyword,
            query: keyword,
            tenders: [],
            newCount: 0,
            newTenderIds: [],
            rawCount: 0,
            duplicatesExcluded: 0,
            error: searchError?.message || 'Ошибка поиска',
          };

          nextGroups.push(group);
          failedKeywords.push(keyword);
          setResultGroups([...nextGroups]);
          setActiveGroupId(prev => prev ?? group.id);
          logger.error(searchError);
        }
      }

      if (failedKeywords.length > 0) {
        setError(`Часть запросов завершилась с ошибкой: ${failedKeywords.join(', ')}`);
      }
    } catch (searchError: any) {
      logger.error(searchError);
      if (searchError?.name !== 'AbortError') {
        setError(searchError?.message || 'Произошла ошибка при поиске');
      }
    } finally {
      setLoading(false);
      setLoadingStatus(null);
      abortControllerRef.current = null;
    }
  };

  const handleCancelSearch = async () => {
    if (abortControllerRef.current) {
      abortControllerRef.current.abort();
    }
    setLoadingStatus('Отмена поиска...');
    await cancelSearch();
    setLoading(false);
    setLoadingStatus(null);
  };

  const getTenderPriceValue = (tender: Tender): number | null => {
    if (typeof tender.initial_price_value === 'number' && Number.isFinite(tender.initial_price_value) && tender.initial_price_value > 0) {
      return tender.initial_price_value;
    }

    const rawPrice = typeof tender.initial_price === 'number'
      ? String(tender.initial_price)
      : tender.initial_price || tender.initial_price_text || '';
    const normalizedPrice = rawPrice
      .replace(/\s+/g, '')
      .replace(/,/g, '.')
      .replace(/[^\d.-]/g, '');
    const parsedPrice = Number.parseFloat(normalizedPrice);

    if (!Number.isFinite(parsedPrice) || parsedPrice <= 0) {
      return null;
    }

    return parsedPrice;
  };

  const formatPrice = (tender: Tender) => {
    if (tender.initial_price_text && tender.initial_price_text !== '0' && tender.initial_price_text !== '0.0') {
      return tender.initial_price_text;
    }
    if (tender.initial_price === 0 || tender.initial_price === '0' || tender.initial_price === '0.0') {
      return 'Цена не указана';
    }

    const price = getTenderPriceValue(tender);

    if (price === null) {
      return 'Цена не указана';
    }

    return new Intl.NumberFormat('ru-RU', { style: 'currency', currency: 'RUB', maximumFractionDigits: 0 }).format(price);
  };

  const formatMetaValue = (value?: string) => {
    const normalized = value?.trim();
    return normalized || 'Не указано';
  };

  const getGroupMaxPriceTender = (group: TenderSearchGroup): Tender | null => {
    let maxTender: Tender | null = null;
    let maxPrice = -1;

    for (const tender of group.tenders) {
      const price = getTenderPriceValue(tender);
      if (price === null || price <= maxPrice) {
        continue;
      }

      maxPrice = price;
      maxTender = tender;
    }

    return maxTender;
  };

  const renderEmptyState = () => {
    if (!activeGroup) {
      return (
        <div className="rounded-xl border border-dashed border-slate-300 bg-slate-50 px-6 py-10 text-center text-slate-500">
          Для поиска по ключевым словам укажите список через запятую. Для каждого слова будет создан отдельный список результатов.
        </div>
      );
    }

    if (activeGroup.error) {
      return (
        <div className="rounded-xl border border-amber-200 bg-amber-50 px-6 py-5 text-amber-800">
          <p className="font-medium">Ошибка поиска по запросу «{activeGroup.label}»</p>
          <p className="mt-1 text-sm">{activeGroup.error}</p>
        </div>
      );
    }

    if (activeGroup.rawCount > 0 && activeGroup.duplicatesExcluded >= activeGroup.rawCount) {
      return (
        <div className="rounded-xl border border-slate-200 bg-slate-50 px-6 py-5 text-slate-600">
          Все результаты по запросу «{activeGroup.label}» уже были найдены в предыдущих списках и исключены как дубликаты.
        </div>
      );
    }

    return (
      <div className="rounded-xl border border-slate-200 bg-slate-50 px-6 py-5 text-slate-600">
        По запросу «{activeGroup.label}» тендеры не найдены.
      </div>
    );
  };

  return (
    <div className="p-6 max-w-[1600px] mx-auto h-[calc(100vh-64px)] flex flex-col gap-4 relative min-h-0">
      <div className="flex flex-col gap-4 xl:flex-row xl:items-end xl:justify-between">
        <div>
          <h2 className="text-2xl font-bold text-slate-900">Поиск тендеров (ЕИС)</h2>
          <p className="text-slate-500 text-sm">Используется браузерный движок Playwright для обхода защиты Zakupki.gov.ru</p>
        </div>

        <div className="flex flex-wrap items-center gap-3">
          {selectedTenders.length > 0 && (
            <button
              onClick={handleProcessSelected}
              disabled={processing || loading}
              className="bg-blue-600 text-white px-4 py-2.5 rounded-full shadow-lg hover:bg-blue-700 transition-all flex items-center gap-2 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {processing ? <Loader2 size={18} className="animate-spin" /> : <CheckCircle size={18} />}
              <span className="font-medium">
                {processing ? 'Обработка...' : `Обработать выбранные (${selectedTenders.length})`}
              </span>
            </button>
          )}
          {crmTenders.length > 0 && (
            <button
              onClick={() => navigate('/crm')}
              className="bg-white text-slate-700 px-4 py-2.5 rounded-full shadow-lg hover:bg-slate-50 transition-all flex items-center gap-2 border border-slate-200"
            >
              <Briefcase size={16} />
              <span className="text-sm font-bold">CRM: {crmTenders.length} активных</span>
            </button>
          )}
        </div>
      </div>

      <div className="grid min-h-0 flex-1 gap-4 xl:grid-cols-[380px_minmax(0,1fr)]">
        <div className="bg-white p-4 rounded-xl border border-slate-200 shadow-sm min-h-0 xl:sticky xl:top-4 xl:self-start">
          <div className="flex gap-2 mb-4">
            <button onClick={() => setSearchMode('keyword')} className={`px-4 py-2 text-sm font-medium rounded-md ${searchMode === 'keyword' ? 'bg-blue-50 text-blue-600' : 'text-slate-500'}`}>Ключевые слова</button>
            <button onClick={() => setSearchMode('catalog')} className={`px-4 py-2 text-sm font-medium rounded-md ${searchMode === 'catalog' ? 'bg-blue-50 text-blue-600' : 'text-slate-500'}`}>По каталогу</button>
          </div>

          <div className="mb-4 rounded-xl border border-dashed border-slate-300 bg-slate-50 px-4 py-4">
            <div className="flex flex-col gap-3">
              <div>
                <p className="text-sm font-semibold text-slate-800">Ручная загрузка документации</p>
                <p className="mt-1 text-xs text-slate-500">
                  Архив или набор файлов по одному тендеру. Карточка будет заполнена автоматически по содержимому документов.
                </p>
              </div>
              <div className="flex items-center gap-3">
                <input
                  ref={manualUploadRef}
                  type="file"
                  multiple
                  className="hidden"
                  onChange={handleManualUpload}
                />
                <button
                  onClick={() => manualUploadRef.current?.click()}
                  disabled={manualUploadLoading || loading || processing}
                  className={`px-4 py-2 rounded-lg text-sm font-medium flex items-center gap-2 ${
                    manualUploadLoading || loading || processing
                      ? 'bg-slate-200 text-slate-500 cursor-wait'
                      : 'bg-slate-900 text-white hover:bg-slate-800'
                  }`}
                >
                  {manualUploadLoading ? <Loader2 size={16} className="animate-spin" /> : <Upload size={16} />}
                  {manualUploadLoading ? 'Обрабатываем...' : 'Загрузить документы'}
                </button>
              </div>
            </div>
          </div>

          <div className="grid grid-cols-2 gap-3 mb-4 text-sm text-slate-700 xl:grid-cols-1">
            <label className="flex items-center gap-2 cursor-pointer">
              <input type="checkbox" checked={fz44} onChange={(e) => setFz44(e.target.checked)} className="rounded text-blue-600 focus:ring-blue-500" />
              44-ФЗ
            </label>
            <label className="flex items-center gap-2 cursor-pointer">
              <input type="checkbox" checked={fz223} onChange={(e) => setFz223(e.target.checked)} className="rounded text-blue-600 focus:ring-blue-500" />
              223-ФЗ
            </label>
            <label className="flex items-center gap-2 cursor-pointer col-span-2 xl:col-span-1">
              <input type="checkbox" checked={isActive} onChange={(e) => setIsActive(e.target.checked)} className="rounded text-blue-600 focus:ring-blue-500" />
              Только этап подачи заявок
            </label>
            <label className="flex items-center gap-2 col-span-2 xl:col-span-1">
              Дней с публикации:
              <input type="number" value={publishDaysBack} onChange={(e) => setPublishDaysBack(Number(e.target.value))} className="w-16 px-2 py-1 rounded border border-slate-300 focus:outline-none focus:ring-2 focus:ring-blue-500" min="1" max="365" />
            </label>
          </div>

          <div className="space-y-3">
            {searchMode === 'keyword' ? (
              <>
                <div className="relative">
                  <Search className="absolute left-3 top-4 text-slate-400" size={20} />
                  <textarea
                    value={keywordQuery}
                    onChange={(e) => setKeywordQuery(e.target.value)}
                    placeholder="Например: гидроизоляция, профлист, арматура"
                    rows={6}
                    className="w-full pl-10 pr-4 py-3 rounded-lg border border-slate-300 bg-white text-slate-900 placeholder-slate-400 focus:outline-none focus:ring-2 focus:ring-blue-500 resize-none"
                  />
                </div>
                <p className="text-xs text-slate-500">
                  Ключевые слова указываются через запятую. По каждому слову выполняется отдельный поиск.
                </p>
              </>
            ) : (
              <div className="rounded-lg border border-slate-300 bg-slate-50 px-4 py-4 text-sm text-slate-600">
                Будет выполнен поиск тендеров по материалам из текущего каталога.
              </div>
            )}

            {loading ? (
              <button onClick={handleCancelSearch} className="w-full px-6 py-3 bg-red-600 text-white rounded-lg flex items-center justify-center gap-2 hover:bg-red-700">
                <XCircle size={20} />
                Отмена
              </button>
            ) : (
              <button onClick={handleSearch} disabled={loading} className="w-full px-6 py-3 bg-blue-600 text-white rounded-lg flex items-center justify-center gap-2 hover:bg-blue-700">
                <Play size={20} />
                Найти
              </button>
            )}
          </div>
        </div>

        <div className="flex min-h-0 flex-col gap-4">
          {error && (
            <div className="p-4 bg-red-50 border border-red-200 rounded-xl flex items-start gap-3 text-red-700">
              <AlertCircle className="shrink-0 mt-0.5" size={20} />
              <div>
                <p className="font-bold">Ошибка при поиске</p>
                <p className="text-sm opacity-90">{error}</p>
              </div>
            </div>
          )}

          {actionError && (
            <div className="p-4 bg-amber-50 border border-amber-200 rounded-xl">
              <div className="flex items-start gap-3">
                <AlertCircle className="w-5 h-5 text-amber-500 mt-0.5" />
                <div>
                  <p className="font-medium text-amber-800">Ошибка передачи в CRM</p>
                  <p className="text-amber-700 mt-1">{actionError}</p>
                </div>
              </div>
            </div>
          )}

          {manualUploadError && (
            <div className="p-4 bg-red-50 border border-red-200 rounded-xl">
              <div className="flex items-start gap-3">
                <AlertCircle className="w-5 h-5 text-red-500 mt-0.5" />
                <div>
                  <p className="font-medium text-red-800">Ошибка ручной загрузки</p>
                  <p className="text-red-700 mt-1">{manualUploadError}</p>
                </div>
              </div>
            </div>
          )}

          <div className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm min-h-0 flex-1 flex flex-col">
            {(loadingStatus || resultGroups.length > 0) && (
              <div className="mb-4">
                {loadingStatus && (
                  <div className="flex items-center gap-2 text-sm text-slate-700">
                    <Loader2 size={16} className="animate-spin text-blue-600" />
                    <span>{loadingStatus}</span>
                  </div>
                )}

                {resultGroups.length > 0 && (
                  <>
                    <div className={`flex gap-2 overflow-x-auto pb-1 ${loadingStatus ? 'mt-3' : ''}`}>
                      {resultGroups.map((group) => {
                        const maxPriceTender = getGroupMaxPriceTender(group);
                        const maxPriceText = maxPriceTender ? formatPrice(maxPriceTender) : 'Цена не указана';

                        return (
                          <div key={group.id} className="flex flex-col gap-1">
                            <button
                              onClick={() => setActiveGroupId(group.id)}
                              className={`px-3 py-2 rounded-lg text-sm whitespace-nowrap border transition-colors ${
                                activeGroup?.id === group.id
                                  ? 'bg-blue-600 border-blue-600 text-white'
                                  : 'bg-slate-50 border-slate-200 text-slate-700 hover:bg-slate-100'
                              }`}
                            >
                              <span>{group.label}</span>
                              <span className="ml-2 text-xs opacity-80">
                                Новых: {group.newCount} · Всего: {group.tenders.length}
                              </span>
                            </button>
                            {maxPriceTender?.url ? (
                              <a
                                href={maxPriceTender.url}
                                target="_blank"
                                rel="noreferrer"
                                title={maxPriceTender.title}
                                className="px-3 text-xs font-medium text-blue-600 hover:underline truncate"
                              >
                                {maxPriceText}
                              </a>
                            ) : (
                              <span className="px-3 text-xs text-slate-500 truncate" title={maxPriceTender?.title || 'Тендер с указанной суммой не найден'}>
                                {maxPriceText}
                              </span>
                            )}
                          </div>
                        );
                      })}
                    </div>

                    {activeGroup && (
                      <p className="mt-3 text-sm text-slate-600">
                        Активный список: {activeGroup.label} · Новых: {activeGroup.newCount}
                        {` · Всего в списке: ${activeGroup.tenders.length}`}
                        {activeGroup.rawCount > 0 && ` из ${activeGroup.rawCount}`}
                        {activeGroup.duplicatesExcluded > 0 && ` · Исключено повторов: ${activeGroup.duplicatesExcluded}`}
                        {totalDuplicatesExcluded > 0 && activeGroup.duplicatesExcluded !== totalDuplicatesExcluded && ` · Всего исключено повторов: ${totalDuplicatesExcluded}`}
                        {` · Всего новых тендеров: ${totalNewTenders}`}
                        {` · Всего уникальных тендеров: ${totalVisibleResults}`}
                      </p>
                    )}
                  </>
                )}
              </div>
            )}

            <div className="min-h-0 flex-1 overflow-auto space-y-4 pr-1 pb-2">
              {visibleResults.length === 0 ? (
                renderEmptyState()
              ) : (
                visibleResults.map((tender) => {
                  const inCrm = isInCrm(tender.id);
                  const isSubmitting = submittingTenderId === tender.id;
                  const tenderId = getTenderUniqueId(tender);
                  const isNewTender = activeGroup?.newTenderIds.includes(tenderId) || false;
                  const cardTone = isNewTender
                    ? 'border-amber-300 bg-amber-50/40'
                    : inCrm
                      ? 'border-emerald-200 bg-emerald-50/20'
                      : 'border-slate-200';

                  return (
                    <div key={tender.id} className={`relative bg-white p-5 rounded-xl border ${cardTone} ${isSelected(tender.id) ? 'ring-2 ring-blue-500' : ''}`}>
                      <div className="absolute top-5 left-5">
                        <button
                          onClick={() => toggleTenderSelection(tender)}
                          className="text-blue-600 hover:text-blue-700"
                        >
                          {isSelected(tender.id) ? <CheckSquare size={24} /> : <Square size={24} className="text-slate-300" />}
                        </button>
                      </div>
                      <div className="absolute top-5 right-5">
                        <button
                          onClick={() => handleSendToWork(tender)}
                          disabled={isSubmitting}
                          className={`flex items-center gap-2 px-3 py-1.5 rounded-lg text-sm font-medium ${
                            isSubmitting
                              ? 'bg-slate-200 text-slate-500 cursor-wait'
                              : inCrm
                                ? 'bg-emerald-100 text-emerald-700 hover:bg-emerald-200'
                                : 'bg-blue-600 text-white hover:bg-blue-700'
                          }`}
                        >
                          {isSubmitting ? <Loader2 size={16} className="animate-spin" /> : inCrm ? <CheckCircle size={16} /> : <Briefcase size={16} />}
                          {isSubmitting ? 'Передача...' : inCrm ? 'Обновить в CRM' : 'В работу'}
                        </button>
                      </div>
                      <div className="pl-10">
                        <h3 className="text-lg font-bold text-slate-800 pr-24">
                          {tender.title}
                          {isNewTender && <span className="ml-2 px-2 py-0.5 text-xs font-semibold bg-amber-100 text-amber-800 rounded">Новый</span>}
                          {tender.seen && <span className="ml-2 px-2 py-0.5 text-xs font-semibold bg-gray-200 text-gray-700 rounded">Просмотрено</span>}
                        </h3>
                        <p className="text-sm text-slate-600 mt-2 line-clamp-2 pr-24">{tender.description}</p>
                        <div className="mt-4 grid gap-3 md:grid-cols-3">
                          <div className="rounded-lg bg-slate-50 px-3 py-2">
                            <p className="text-[11px] font-semibold uppercase tracking-wide text-slate-500">Заказчик</p>
                            <p className="mt-1 text-sm text-slate-800 break-words">{formatMetaValue(tender.customer_name)}</p>
                          </div>
                          <div className="rounded-lg bg-slate-50 px-3 py-2">
                            <p className="text-[11px] font-semibold uppercase tracking-wide text-slate-500">ИНН заказчика</p>
                            <p className="mt-1 text-sm text-slate-800 break-words">{formatMetaValue(tender.customer_inn)}</p>
                          </div>
                          <div className="rounded-lg bg-slate-50 px-3 py-2">
                            <p className="text-[11px] font-semibold uppercase tracking-wide text-slate-500">Место нахождения</p>
                            <p className="mt-1 text-sm text-slate-800 break-words">{formatMetaValue(tender.customer_location)}</p>
                          </div>
                        </div>
                        <div className="mt-4 flex justify-between items-end border-t pt-3">
                          <span className="text-xl font-bold text-slate-900">{formatPrice(tender)}</span>
                          <div className="text-right">
                            <span className="text-xs text-slate-500 block">№ {tender.eis_number}</span>
                            {tender.url && <a href={tender.url} target="_blank" rel="noreferrer" className="text-blue-600 text-sm hover:underline flex items-center gap-1 justify-end">ЕИС <ExternalLink size={12}/></a>}
                          </div>
                        </div>
                      </div>
                    </div>
                  );
                })
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default TenderSearch;
