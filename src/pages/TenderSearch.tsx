import React, { useState, useEffect, useRef } from 'react';
import { Search, Filter, Play, CheckCircle, ExternalLink, AlertCircle, Loader2, WifiOff, Briefcase, XCircle } from 'lucide-react';
import { useNavigate } from 'react-router-dom';
import { MOCK_CATALOG } from './ProductCatalog';
import { searchTenders, getTendersFromBackend, processSelectedTenders, cancelSearch } from '../services/geminiService';
import { Tender } from '../types';

const TenderSearch = () => {
  const navigate = useNavigate();
  const [query, setQuery] = useState('');
  const [results, setResults] = useState<Tender[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [processing, setProcessing] = useState(false);
  const [searchMode, setSearchMode] = useState<'keyword' | 'catalog'>('keyword');
  const [isActive, setIsActive] = useState(true);
  const [fz44, setFz44] = useState(true);
  const [fz223, setFz223] = useState(true);
  const [publishDaysBack, setPublishDaysBack] = useState(30);

  // CRM State from Backend
  const [crmTenders, setCrmTenders] = useState<Tender[]>([]);
  const [submittingTenderId, setSubmittingTenderId] = useState<string | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);
  const abortControllerRef = useRef<AbortController | null>(null);

  useEffect(() => {
    refreshCrmTenders().catch(console.error);
  }, []);

  const refreshCrmTenders = async () => {
    const fresh = await getTendersFromBackend();
    setCrmTenders(fresh);
  };

  const isInCrm = (id: string) => crmTenders.some(t => t.id === id);

  const handleSendToWork = async (tender: Tender) => {
    if (!tender || tender.id === 'err_msg') return;
    if (isInCrm(tender.id)) return;

    setActionError(null);
    setSubmittingTenderId(tender.id);

    try {
      await processSelectedTenders([tender]);
      await refreshCrmTenders();

      setResults(prev =>
        prev.map(item =>
          item.id === tender.id
            ? { ...item, status: 'Found' as const }
            : item
        )
      );
    } catch (error: any) {
      console.error('Failed to send tender to CRM:', error);
      setActionError(error?.message || 'Не удалось передать тендер в CRM');
    } finally {
      setSubmittingTenderId(null);
    }
  };

  const handleSearch = async () => {
    setLoading(true);
    setResults([]);
    setError(null);
    setActionError(null);
    
    abortControllerRef.current = new AbortController();

    try {
      const catalogContext = MOCK_CATALOG.map(p => `${p.title} (${p.category})`).join(', ');
      const effectiveQuery = searchMode === 'catalog' 
        ? `Найти тендеры, где требуются товары из списка: ${catalogContext}.`
        : query;

      if (!effectiveQuery && searchMode === 'keyword') {
        setLoading(false);
        return;
      }

      const tenders = await searchTenders(
          effectiveQuery, 
          catalogContext, 
          isActive, 
          fz44, 
          fz223, 
          publishDaysBack,
          abortControllerRef.current.signal
      );
      setResults(tenders);
    } catch (err: any) {
      console.error(err);
      setError(err.message || 'Произошла ошибка при поиске');
    } finally {
      setLoading(false);
      abortControllerRef.current = null;
    }
  };

  const handleCancelSearch = async () => {
    if (abortControllerRef.current) {
        abortControllerRef.current.abort();
    }
    await cancelSearch();
    setLoading(false);
  };

  const formatPrice = (tender: Tender) => {
    if (tender.initial_price_text && tender.initial_price_text !== '0' && tender.initial_price_text !== '0.0') {
        return tender.initial_price_text;
    }
    if (tender.initial_price === 0 || tender.initial_price === '0' || tender.initial_price === '0.0') {
        return 'Цена не указана';
    }
    
    const price = typeof tender.initial_price === 'string' ? parseFloat(tender.initial_price) : tender.initial_price;
    
    if (isNaN(price)) {
        return 'Цена не указана';
    }
    
    return new Intl.NumberFormat('ru-RU', { style: 'currency', currency: 'RUB', maximumFractionDigits: 0 }).format(price);
  };

  return (
    <div className="p-6 max-w-7xl mx-auto h-[calc(100vh-64px)] flex flex-col relative">
      <div className="mb-6">
        <h2 className="text-2xl font-bold text-slate-900">Поиск тендеров (ЕИС)</h2>
        <p className="text-slate-500 text-sm">Используется браузерный движок Playwright для обхода защиты Zakupki.gov.ru</p>
      </div>

      {/* Floating Action Bar */}
      <div className="absolute top-6 right-6 z-10 flex gap-3">
        {crmTenders.length > 0 && (
            <button 
                onClick={() => navigate('/crm')}
                className="bg-white text-slate-700 px-4 py-3 rounded-full shadow-lg hover:bg-slate-50 transition-all flex items-center gap-2 border border-slate-200"
            >
                <Briefcase size={16} />
                <span className="text-sm font-bold">CRM: {crmTenders.length} активных</span>
            </button>
        )}
      </div>

      {/* Search UI */}
      <div className="bg-white p-4 rounded-xl border border-slate-200 shadow-sm mb-6">
        <div className="flex gap-4 mb-4">
             <button onClick={() => setSearchMode('keyword')} className={`px-4 py-2 text-sm font-medium rounded-md ${searchMode === 'keyword' ? 'bg-blue-50 text-blue-600' : 'text-slate-500'}`}>Ключевые слова</button>
             <button onClick={() => setSearchMode('catalog')} className={`px-4 py-2 text-sm font-medium rounded-md ${searchMode === 'catalog' ? 'bg-blue-50 text-blue-600' : 'text-slate-500'}`}>По каталогу</button>
        </div>
        <div className="flex flex-wrap gap-4 mb-4 items-center text-sm text-slate-700">
            <label className="flex items-center gap-2 cursor-pointer">
                <input type="checkbox" checked={fz44} onChange={(e) => setFz44(e.target.checked)} className="rounded text-blue-600 focus:ring-blue-500" />
                44-ФЗ
            </label>
            <label className="flex items-center gap-2 cursor-pointer">
                <input type="checkbox" checked={fz223} onChange={(e) => setFz223(e.target.checked)} className="rounded text-blue-600 focus:ring-blue-500" />
                223-ФЗ
            </label>
            <label className="flex items-center gap-2 cursor-pointer">
                <input type="checkbox" checked={isActive} onChange={(e) => setIsActive(e.target.checked)} className="rounded text-blue-600 focus:ring-blue-500" />
                Только этап подачи заявок
            </label>
            <label className="flex items-center gap-2">
                Дней с публикации:
                <input type="number" value={publishDaysBack} onChange={(e) => setPublishDaysBack(Number(e.target.value))} className="w-16 px-2 py-1 rounded border border-slate-300 focus:outline-none focus:ring-2 focus:ring-blue-500" min="1" max="365" />
            </label>
        </div>
        <div className="flex gap-4">
          <div className="flex-1 relative">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-400" size={20} />
            <input 
              type="text" 
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              placeholder="Введите запрос..."
              className="w-full pl-10 pr-4 py-3 rounded-lg border border-slate-300 bg-white text-slate-900 placeholder-slate-400 focus:outline-none focus:ring-2 focus:ring-blue-500"
            />
          </div>
          {loading ? (
            <button onClick={handleCancelSearch} className="px-6 py-3 bg-red-600 text-white rounded-lg flex items-center gap-2 hover:bg-red-700">
                <XCircle size={20} />
                Отмена
            </button>
          ) : (
            <button onClick={handleSearch} disabled={loading} className="px-6 py-3 bg-blue-600 text-white rounded-lg flex items-center gap-2 hover:bg-blue-700">
                <Play size={20} />
                Найти
            </button>
          )}
        </div>
      </div>

      {error && (
        <div className="mb-6 p-4 bg-red-50 border border-red-200 rounded-xl flex items-start gap-3 text-red-700">
            <AlertCircle className="shrink-0 mt-0.5" size={20} />
            <div>
                <p className="font-bold">Ошибка при поиске</p>
                <p className="text-sm opacity-90">{error}</p>
            </div>
        </div>
      )}

      {actionError && (
        <div className="mb-8 p-4 bg-amber-50 border border-amber-200 rounded-xl">
          <div className="flex items-start gap-3">
            <AlertCircle className="w-5 h-5 text-amber-500 mt-0.5" />
            <div>
              <p className="font-medium text-amber-800">Ошибка передачи в CRM</p>
              <p className="text-amber-700 mt-1">{actionError}</p>
            </div>
          </div>
        </div>
      )}

      {/* Results List */}
      <div className="flex-1 overflow-auto space-y-4 pb-6">
         {results.map((tender) => {
             const inCrm = isInCrm(tender.id);
             const isSubmitting = submittingTenderId === tender.id;

             return (
                 <div key={tender.id} className={`relative bg-white p-5 rounded-xl border ${inCrm ? 'border-emerald-200 bg-emerald-50/20' : 'border-slate-200'}`}>
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
                    <h3 className="text-lg font-bold text-slate-800 pr-24">
                        {tender.title}
                        {tender.seen && <span className="ml-2 px-2 py-0.5 text-xs font-semibold bg-gray-200 text-gray-700 rounded">Просмотрено</span>}
                    </h3>
                    <p className="text-sm text-slate-600 mt-2 line-clamp-2 w-3/4">{tender.description}</p>
                    <div className="mt-4 flex justify-between items-end border-t pt-3">
                        <span className="text-xl font-bold text-slate-900">{formatPrice(tender)}</span>
                        <div className="text-right">
                             <span className="text-xs text-slate-500 block">№ {tender.eis_number}</span>
                             {tender.url && <a href={tender.url} target="_blank" className="text-blue-600 text-sm hover:underline flex items-center gap-1 justify-end">ЕИС <ExternalLink size={12}/></a>}
                        </div>
                    </div>
                 </div>
             )
         })}
      </div>
    </div>
  );
};

export default TenderSearch;
