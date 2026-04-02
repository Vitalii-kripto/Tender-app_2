import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import { MOCK_CATALOG } from './ProductCatalog';
import { findProductEquivalent, startBatchAnalysisJob, getJobStatus, getTendersFromBackend, deleteTenderFromBackend } from '../services/geminiService';
import { AnalysisResult, Tender, LegalAnalysisResult } from '../types';
import { FileText, Shield, ArrowRight, CheckCircle, AlertTriangle, Cpu, Trash2, FileDown, ScanEye, Loader2, Square, CheckSquare, ShieldAlert, Layout, ChevronDown, Table } from 'lucide-react';

const Analysis = () => {
  const navigate = useNavigate();
  const [inputText, setInputText] = useState('');
  const [loading, setLoading] = useState(false);
  const [activeTab, setActiveTab] = useState<'match' | 'batch'>('batch');
  const [statusText, setStatusText] = useState('');
  
  // Single analysis state
  const [matchResult, setMatchResult] = useState<AnalysisResult | null>(null);

  // Batch analysis state
  const [crmTenders, setCrmTenders] = useState<Tender[]>([]);
  const [selectedTenderIds, setSelectedTenderIds] = useState<Set<string>>(new Set());
  const [tenderFiles, setTenderFiles] = useState<Record<string, any[]>>({});
  const [selectedFiles, setSelectedFiles] = useState<Record<string, Set<string>>>({});
  const [batchResults, setBatchResults] = useState<Record<string, LegalAnalysisResult>>({});
  const [analysisError, setAnalysisError] = useState('');
  const [analysisStages, setAnalysisStages] = useState<Record<string, { stage: string, progress: number }>>({});

  // Filtering & Sorting state
  const [filterBlock, setFilterBlock] = useState<string>('all');
  const [filterRisk, setFilterRisk] = useState<string>('all');
  const [filterProblematic, setFilterProblematic] = useState<boolean>(false);
  const [filterProblematicFiles, setFilterProblematicFiles] = useState<boolean>(false);
  const [sortBy, setSortBy] = useState<'risk' | 'block'>('risk');

  const getRecommendedProduct = (id: string) => {
    return MOCK_CATALOG.find(p => p.id === id);
  };

  const formatSpecKey = (key: string) => {
    return key.replace(/_/g, ' ');
  };

  useEffect(() => {
    const loadData = async () => {
        try {
            const tenders = await getTendersFromBackend();
            setCrmTenders(tenders);
            if (tenders.length > 0) setActiveTab('batch');
            
            // Fetch files for all tenders
            tenders.forEach(async (t) => {
                try {
                    const response = await fetch(`/api/tenders/${t.id}/files`);
                    const files = await response.json();
                    setTenderFiles(prev => ({ ...prev, [t.id]: files }));
                    // Manual selection: don't select all by default
                    setSelectedFiles(prev => ({ ...prev, [t.id]: new Set() }));
                } catch (e) {
                    console.error(`Failed to load files for tender ${t.id}`, e);
                }
            });
        } catch (e) {
            console.error("Failed to load analysis tenders", e);
        }
    };
    loadData();
  }, []);

  const handleSingleAnalyze = async () => {
    if (!inputText) return;
    setLoading(true);
    setStatusText("Анализ...");
    
    try {
      if (activeTab === 'match') {
        const result = await findProductEquivalent(inputText);
        setMatchResult(result);
      }
    } catch (e) {
      console.error(e);
    } finally {
      setLoading(false);
    }
  };

  const runBatchAnalysis = async () => {
    if (selectedTenderIds.size === 0) {
        setAnalysisError('Выберите хотя бы один тендер для анализа.');
        return;
    }
    setAnalysisError('');
    setLoading(true);
    setStatusText("Запуск пакетного анализа...");
    
    try {
        const idsArray = Array.from(selectedTenderIds);
        
        // Validation: check if files are selected for each tender
        const tendersWithoutFiles = [];
        for (const tid of idsArray) {
            if (!selectedFiles[tid] || selectedFiles[tid].size === 0) {
                const tender = crmTenders.find(t => t.id === tid);
                tendersWithoutFiles.push(tender ? tender.eis_number || tid : tid);
            }
        }
        
        if (tendersWithoutFiles.length > 0) {
            setAnalysisError(`Выберите файлы для следующих тендеров: ${tendersWithoutFiles.join(', ')}`);
            setLoading(false);
            return;
        }

        // Prepare selected files mapping
        const filesMapping: Record<string, string[]> = {};
        idsArray.forEach(id => {
            if (selectedFiles[id]) {
                filesMapping[id] = Array.from(selectedFiles[id]);
            }
        });

        // Initialize stages
        const initialStages: Record<string, { stage: string, progress: number }> = {};
        idsArray.forEach(id => {
            initialStages[id] = { stage: 'Ожидание', progress: 0 };
        });
        setAnalysisStages(initialStages);

        // Start analysis
        const jobId = await startBatchAnalysisJob(idsArray, filesMapping);
        
        // Polling
        const pollInterval = setInterval(async () => {
            try {
                const job = await getJobStatus(jobId);
                
                const newStages: Record<string, { stage: string, progress: number }> = {};
                for (const tid in job.tenders) {
                    newStages[tid] = {
                        stage: job.tenders[tid].stage,
                        progress: job.tenders[tid].progress
                    };
                }
                setAnalysisStages(prev => ({ ...prev, ...newStages }));
                
                if (job.status === 'completed' || job.status === 'error' || job.status === 'failed') {
                    clearInterval(pollInterval);
                    const newResults: Record<string, LegalAnalysisResult> = { ...batchResults };
                    for (const tid in job.tenders) {
                        newResults[tid] = {
                            id: tid,
                            ...job.tenders[tid]
                        };
                    }
                    setBatchResults(newResults);
                    setLoading(false);
                    setStatusText("");
                    
                    if (job.status === 'error' || job.status === 'failed') {
                        setAnalysisError(job.error_message || 'Анализ завершился с ошибкой.');
                    }
                }
            } catch (e) {
                console.error("Error polling job status", e);
                clearInterval(pollInterval);
                setAnalysisError('Произошла ошибка при получении статуса анализа.');
                setLoading(false);
                setStatusText("");
            }
        }, 3000);
        
    } catch (e) {
        console.error("Batch analysis failed", e);
        setAnalysisError('Произошла ошибка при запуске анализа.');
        setLoading(false);
        setStatusText("");
    }
  };

  const removeTender = async (id: string) => {
    if(confirm("Убрать этот тендер из CRM?")) {
        const updated = crmTenders.filter(t => t.id !== id);
        setCrmTenders(updated);
        
        const newSelected = new Set(selectedTenderIds);
        newSelected.delete(id);
        setSelectedTenderIds(newSelected);
        
        await deleteTenderFromBackend(id);
        if (updated.length === 0) setActiveTab('match');
    }
  };

  const toggleSelection = (id: string) => {
      if (loading) return;
      const newSelected = new Set(selectedTenderIds);
      if (newSelected.has(id)) {
          newSelected.delete(id);
      } else {
          newSelected.add(id);
      }
      setSelectedTenderIds(newSelected);
      setAnalysisError('');
  };

  const selectAll = () => {
      if (loading) return;
      setSelectedTenderIds(new Set(crmTenders.map(t => t.id)));
      setAnalysisError('');
  };

  const deselectAll = () => {
      if (loading) return;
      setSelectedTenderIds(new Set());
      setAnalysisError('');
  };

  const toggleFileSelection = (tenderId: string, fileName: string) => {
    if (loading) return;
    const newSelected = new Set(selectedFiles[tenderId] || new Set());
    if (newSelected.has(fileName)) {
        newSelected.delete(fileName);
    } else {
        newSelected.add(fileName);
    }
    setSelectedFiles(prev => ({ ...prev, [tenderId]: newSelected }));
  };

  const selectAllFiles = (tenderId: string) => {
    if (loading) return;
    const files = tenderFiles[tenderId] || [];
    setSelectedFiles(prev => ({ ...prev, [tenderId]: new Set(files.map(f => f.name)) }));
  };

  const deselectAllFiles = (tenderId: string) => {
    if (loading) return;
    setSelectedFiles(prev => ({ ...prev, [tenderId]: new Set() }));
  };

  const exportToWord = async (results: LegalAnalysisResult[]) => {
    try {
        const resultsWithMeta = results.map(result => {
            const tender = crmTenders.find(t => t.id === result.id);
            return {
                ...result,
                description: tender ? `${tender.title}\n${tender.description}` : 'Нет описания'
            };
        });

        const response = await fetch('/api/ai/export-risks-word', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ results: resultsWithMeta })
        });
        if (!response.ok) throw new Error('Export failed');
        
        const blob = await response.blob();
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        
        const isZip = response.headers.get('Content-Type') === 'application/zip';
        const extension = isZip ? 'zip' : 'docx';
        
        a.download = `tender_risks_report_${new Date().toISOString().split('T')[0]}.${extension}`;
        document.body.appendChild(a);
        a.click();
        window.URL.revokeObjectURL(url);
    } catch (e) {
        console.error("Word export failed", e);
        alert("Не удалось экспортировать в Word.");
    }
  };

  const formatCurrency = (amount: number | string) => {
      const num = typeof amount === 'string' ? parseFloat(amount) : amount;
      if (isNaN(num)) return amount;
      return new Intl.NumberFormat('ru-RU', { style: 'currency', currency: 'RUB' }).format(num);
  };

  return (
    <div className="p-6 max-w-6xl mx-auto pb-20">
      <div className="mb-8">
        <h2 className="text-2xl font-bold text-slate-900 flex items-center gap-2">
          <Cpu className="text-blue-600" />
          ИИ Юрист
        </h2>
        <p className="text-slate-500 text-sm mt-1">
          Анализ тендерной документации на юридические риски.
        </p>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
        {/* Input/Selection Section */}
        <div className="bg-white rounded-xl border border-slate-200 shadow-sm flex flex-col h-[700px] overflow-hidden">
          <div className="border-b border-slate-200 flex bg-slate-50">
            <button 
              onClick={() => setActiveTab('batch')}
              className={`flex-1 py-3 text-sm font-medium flex items-center justify-center gap-2 transition-colors ${activeTab === 'batch' ? 'bg-white text-blue-600 border-t-2 border-t-blue-600 shadow-sm' : 'text-slate-500 hover:text-slate-700'}`}
            >
              <div className="relative">
                <FileText size={16} />
                {crmTenders.length > 0 && <span className="absolute -top-1 -right-2 w-3 h-3 bg-red-500 rounded-full border border-white"></span>}
              </div>
              Тендеры ({crmTenders.length})
            </button>
            <button 
              onClick={() => setActiveTab('match')}
              className={`flex-1 py-3 text-sm font-medium flex items-center justify-center gap-2 transition-colors ${activeTab === 'match' ? 'bg-white text-blue-600 border-t-2 border-t-blue-600 shadow-sm' : 'text-slate-500 hover:text-slate-700'}`}
            >
              <ScanEye size={16} />
              Ручной ввод
            </button>
          </div>
          
          {activeTab === 'batch' ? (
            <div className="flex-1 flex flex-col p-4 bg-slate-50/50">
                {crmTenders.length === 0 ? (
                    <div className="flex-1 flex flex-col items-center justify-center text-center p-6">
                        <FileText size={48} className="text-slate-300 mb-4" />
                        <h3 className="text-lg font-medium text-slate-700">Нет тендеров в CRM</h3>
                        <p className="text-sm text-slate-500 mb-6">Перейдите в поиск и добавьте закупки в работу.</p>
                        <button onClick={() => navigate('/tenders')} className="px-4 py-2 bg-blue-100 text-blue-700 rounded-lg hover:bg-blue-200 transition-colors text-sm font-medium">
                            Перейти к поиску
                        </button>
                    </div>
                ) : (
                    <>
                        <div className="flex justify-between items-center mb-3 px-1">
                            <div className="flex gap-3">
                                <button onClick={selectAll} disabled={loading} className="text-xs text-blue-600 hover:underline font-medium disabled:opacity-50">Выбрать все</button>
                                <button onClick={deselectAll} disabled={loading} className="text-xs text-slate-500 hover:underline font-medium disabled:opacity-50">Снять все</button>
                            </div>
                            <div className="text-xs font-medium text-slate-600">
                                Всего: {crmTenders.length} | Выбрано: <span className={selectedTenderIds.size > 0 ? "text-blue-600 font-bold" : ""}>{selectedTenderIds.size}</span>
                            </div>
                        </div>
                        
                        <div className="flex-1 overflow-y-auto space-y-3 pr-2 mb-4 custom-scrollbar">
                            {crmTenders.map(tender => {
                                const isSelected = selectedTenderIds.has(tender.id);
                                return (
                                <div key={tender.id} className={`bg-white p-3 rounded-lg border shadow-sm flex gap-3 transition-colors ${isSelected ? 'border-blue-400 ring-1 ring-blue-400/20' : 'border-slate-200 hover:border-blue-300'}`}>
                                    <button 
                                        onClick={() => toggleSelection(tender.id)}
                                        disabled={loading}
                                        className="mt-1 text-slate-400 hover:text-blue-600 disabled:opacity-50"
                                    >
                                        {isSelected ? <CheckSquare size={20} className="text-blue-600" /> : <Square size={20} />}
                                    </button>
                                    <div className="flex-1 min-w-0 cursor-pointer" onClick={() => toggleSelection(tender.id)}>
                                        <div className="flex items-center gap-2 mb-1">
                                            <span className="text-[10px] font-mono bg-slate-100 px-1.5 py-0.5 rounded text-slate-500 border border-slate-200">#{tender.eis_number}</span>
                                            <span className="text-xs font-black text-blue-700">{formatCurrency(tender.initial_price)}</span>
                                            {batchResults[tender.id] && (
                                                <span className="ml-auto text-[9px] font-black px-1.5 py-0.5 rounded uppercase tracking-tighter bg-emerald-100 text-emerald-600">
                                                    Analyzed
                                                </span>
                                            )}
                                        </div>
                                        <h4 className="text-sm font-black text-slate-800 line-clamp-1 mb-1 group-hover:text-blue-600 transition-colors">{tender.title}</h4>
                                        <p className="text-[11px] text-slate-500 line-clamp-2 mb-2 leading-tight">{tender.description}</p>
                                        
                                        {/* File List for Selection */}
                                        {tenderFiles[tender.id] && tenderFiles[tender.id].length > 0 && (
                                            <div className="mt-2 pt-2 border-t border-slate-100">
                                                <div className="flex justify-between items-center mb-1">
                                                    <span className="text-[10px] font-bold text-slate-400 uppercase">Документы ({tenderFiles[tender.id].length})</span>
                                                    <div className="flex gap-2">
                                                        <button onClick={(e) => { e.stopPropagation(); selectAllFiles(tender.id); }} className="text-[10px] text-blue-500 hover:underline">Все</button>
                                                        <button onClick={(e) => { e.stopPropagation(); deselectAllFiles(tender.id); }} className="text-[10px] text-slate-400 hover:underline">Ничего</button>
                                                    </div>
                                                </div>
                                                <div className="max-h-24 overflow-y-auto space-y-1 pr-1 custom-scrollbar">
                                                    {tenderFiles[tender.id].map(file => (
                                                        <div 
                                                            key={file.name} 
                                                            className="flex items-center gap-2 text-[11px] text-slate-600 hover:bg-slate-50 p-0.5 rounded"
                                                            onClick={(e) => { e.stopPropagation(); toggleFileSelection(tender.id, file.name); }}
                                                        >
                                                            {selectedFiles[tender.id]?.has(file.name) ? (
                                                                <CheckSquare size={12} className="text-blue-500" />
                                                            ) : (
                                                                <Square size={12} className="text-slate-300" />
                                                            )}
                                                            <span className="truncate flex-1">{file.name}</span>
                                                            <span className="text-[9px] text-slate-400">{(file.size / 1024).toFixed(0)} KB</span>
                                                        </div>
                                                    ))}
                                                </div>
                                            </div>
                                        )}
                                    </div>
                                    <button 
                                        onClick={(e) => { e.stopPropagation(); removeTender(tender.id); }}
                                        disabled={loading}
                                        className="text-slate-400 hover:text-red-500 p-1 self-start disabled:opacity-50"
                                        title="Удалить из CRM"
                                    >
                                        <Trash2 size={16} />
                                    </button>
                                </div>
                            )})}
                        </div>
                        
                        {analysisError && (
                            <div className="mb-3 p-2 bg-red-50 border border-red-200 text-red-600 text-sm rounded-lg flex items-center gap-2">
                                <AlertTriangle size={16} /> {analysisError}
                            </div>
                        )}
                        
                        <div className="border-t border-slate-200 pt-4">
                            <button 
                                onClick={runBatchAnalysis}
                                disabled={loading || selectedTenderIds.size === 0}
                                className={`w-full flex items-center justify-center gap-2 py-3 rounded-xl text-white font-bold transition-all shadow-md ${loading || selectedTenderIds.size === 0 ? 'bg-slate-400 cursor-not-allowed' : 'bg-gradient-to-r from-blue-600 to-indigo-600 hover:shadow-lg hover:scale-[1.01]'}`}
                            >
                                {loading ? (
                                    <>
                                        <Loader2 size={20} className="animate-spin" />
                                        Анализ...
                                    </>
                                ) : (
                                    <>
                                        <Cpu size={20} />
                                        Анализировать выбранные тендеры
                                    </>
                                )}
                            </button>
                        </div>
                    </>
                )}
            </div>
          ) : (
             <div className="p-4 flex-1 flex flex-col">
                <textarea
                className="w-full flex-1 p-4 bg-slate-50 border border-slate-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 resize-none font-mono text-sm"
                placeholder="Вставьте тех. характеристики или текст контракта вручную..."
                value={inputText}
                onChange={(e) => setInputText(e.target.value)}
                />
                <div className="flex items-center gap-4 mt-4">
                     <label className="flex items-center gap-2 text-sm text-slate-600">
                        <input type="radio" name="mode" checked={activeTab === 'match'} onChange={() => setActiveTab('match')} />
                        Подбор товара
                     </label>
                     <button 
                        onClick={handleSingleAnalyze}
                        disabled={loading || !inputText}
                        className={`ml-auto flex items-center gap-2 px-6 py-2.5 rounded-lg text-white font-medium transition-all ${loading || !inputText ? 'bg-slate-300 cursor-not-allowed' : 'bg-blue-600 hover:bg-blue-700 shadow-md'}`}
                    >
                        {loading ? 'Анализ...' : 'Запустить'}
                    </button>
                </div>
            </div>
          )}
        </div>

        {/* Results Section */}
        <div className="space-y-6 h-[700px] overflow-y-auto pr-2 custom-scrollbar">
          
          {/* 1. Loading State */}
          {loading && (
            <div className="h-full flex flex-col items-center justify-center text-slate-400 space-y-6">
              <div className="relative">
                <div className="w-20 h-20 border-4 border-slate-100 border-t-blue-600 rounded-full animate-spin"></div>
                <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2">
                    <Cpu size={32} className="text-blue-600" />
                </div>
              </div>
              <div className="text-center">
                <p className="font-black text-slate-700 text-lg mb-1">{statusText || "Анализ запущен"}</p>
                <p className="text-xs text-slate-400 uppercase tracking-widest font-bold">ИИ обрабатывает документы...</p>
              </div>
              
              <div className="w-64 space-y-3">
                 {Object.entries(analysisStages).map(([tid, stage]) => (
                    <div key={tid} className="bg-white p-3 rounded-lg border border-slate-200 shadow-sm">
                        <div className="flex justify-between text-[10px] font-black uppercase mb-1">
                            <span className="truncate max-w-[100px]">Тендер {tid}</span>
                            <span className="text-blue-600">{stage.progress}%</span>
                        </div>
                        <div className="w-full bg-slate-100 h-1.5 rounded-full overflow-hidden">
                            <div className="bg-blue-600 h-full transition-all duration-500" style={{ width: `${stage.progress}%` }}></div>
                        </div>
                        <div className="text-[9px] text-slate-500 mt-1 font-bold">{stage.stage}</div>
                    </div>
                 ))}
              </div>
            </div>
          )}

          {/* 2. Batch Results State */}
          {!loading && activeTab === 'batch' && Object.keys(batchResults).length > 0 && (
             <div className="space-y-8">
                <div className="flex justify-between items-center bg-white p-4 rounded-xl border border-slate-200 shadow-sm">
                    <h2 className="text-lg font-bold text-slate-800">Результаты анализа ({Object.keys(batchResults).length})</h2>
                    <div className="flex gap-2">
                        <button 
                            onClick={() => exportToWord(Object.values(batchResults))}
                            className="flex items-center gap-2 px-4 py-2 bg-blue-50 text-blue-700 hover:bg-blue-100 border border-blue-200 rounded-lg text-sm font-bold transition-colors shadow-sm"
                        >
                            <FileDown size={16} />
                            Скачать отчет Word
                        </button>
                    </div>
                </div>
                {Object.values(batchResults).map(result => {
                    const tender = crmTenders.find(t => t.id === result.id);
                    if (!tender) return null;

                    return (
                        <div key={result.id} className="animate-in slide-in-from-bottom-4 fade-in duration-500">
                            <div className="bg-white rounded-xl border border-slate-200 shadow-sm overflow-hidden mb-8">
                                {/* Tender Header Info */}
                                <div className="p-5 border-b border-slate-100 bg-slate-50/50">
                                    <div className="flex flex-wrap items-center justify-between gap-4 mb-3">
                                        <div className="flex items-center gap-3">
                                            <span className="bg-blue-600 text-white text-xs font-mono px-2.5 py-1 rounded font-bold shadow-sm">#{tender.eis_number}</span>
                                            <h3 className="font-bold text-slate-900 text-xl">{tender.title}</h3>
                                        </div>
                                        <div className="text-xl font-black text-slate-900 bg-white px-4 py-1 rounded-lg border border-slate-200 shadow-sm">
                                            {formatCurrency(tender.initial_price)}
                                        </div>
                                    </div>
                                    <p className="text-sm text-slate-600 mb-4 leading-relaxed">{tender.description}</p>
                                    
                                    <div className="flex flex-wrap gap-3">
                                        <span className="flex items-center gap-1.5 px-3 py-1.5 rounded-full text-[10px] font-bold uppercase tracking-wider bg-blue-50 text-blue-700 border border-blue-200 shadow-sm">
                                            <FileText size={14}/>
                                            Файлов выбрано пользователем: {result.selected_files_count !== undefined ? result.selected_files_count : result.file_statuses.length}
                                        </span>
                                        <span className="flex items-center gap-1.5 px-3 py-1.5 rounded-full text-[10px] font-bold uppercase tracking-wider bg-slate-100 text-slate-600 border border-slate-200 shadow-sm">
                                            <Shield size={14}/>
                                            Успешно: {result.file_statuses.filter(f => f.status === 'ok').length} / Ошибок: {result.file_statuses.filter(f => f.status !== 'ok').length}
                                        </span>
                                    </div>
                                </div>

                                {/* File Statuses Block */}
                                <div className="px-5 py-4 bg-slate-50 border-b border-slate-100">
                                    <div className="flex justify-between items-center mb-2">
                                        <h4 className="text-[10px] font-black text-slate-400 uppercase tracking-widest">Статус документов</h4>
                                        <label className="flex items-center gap-1.5 text-[10px] text-slate-500 cursor-pointer hover:text-slate-700">
                                            <input type="checkbox" checked={filterProblematicFiles} onChange={(e) => setFilterProblematicFiles(e.target.checked)} className="rounded border-slate-300 text-blue-600 focus:ring-blue-500" />
                                            Только проблемные
                                        </label>
                                    </div>
                                    <div className="flex flex-col gap-1.5 max-h-48 overflow-y-auto custom-scrollbar pr-1">
                                        {result.file_statuses
                                            .filter(fs => !filterProblematicFiles || fs.status !== 'ok')
                                            .sort((a, b) => (a.status === 'ok' ? 1 : -1) - (b.status === 'ok' ? 1 : -1))
                                            .map((fs, i) => (
                                            <div key={i} className={`flex items-start gap-2 px-2.5 py-1.5 rounded-lg border text-[11px] font-medium transition-colors ${fs.status === 'ok' ? 'bg-white border-slate-200 text-slate-600' : 'bg-red-50 border-red-200 text-red-700'}`}>
                                                {fs.status === 'ok' ? <CheckCircle size={14} className="text-emerald-500 mt-0.5 shrink-0" /> : <AlertTriangle size={14} className="text-red-500 mt-0.5 shrink-0" />}
                                                <div className="flex flex-col min-w-0">
                                                    <span className="truncate font-bold">{fs.filename}</span>
                                                    {fs.status !== 'ok' && <span className="text-[10px] text-red-600/80 mt-0.5 leading-tight">{fs.message}</span>}
                                                </div>
                                            </div>
                                        ))}
                                    </div>
                                </div>

                                 {/* 1.5 Structured Data */}
                                 {result.status === 'success' && result.structured_data && Object.keys(result.structured_data).length > 0 && (
                                     <div className="px-6 py-6 bg-slate-50 border-b border-slate-100">
                                         <h4 className="text-sm font-bold text-slate-800 uppercase tracking-wider mb-4 flex items-center gap-2">
                                             <Table size={16} className="text-blue-600" />
                                             Извлеченные данные (ИИ)
                                         </h4>
                                         <div className="grid grid-cols-1 md:grid-cols-2 gap-4 text-sm">
                                             {result.structured_data.customer && (
                                                 <div className="bg-white p-4 rounded border border-slate-200 shadow-sm">
                                                     <div className="font-bold text-slate-700 mb-2 border-b pb-1">Заказчик</div>
                                                     <div className="text-slate-600"><span className="text-slate-400">Название:</span> {result.structured_data.customer.name || '—'}</div>
                                                     <div className="text-slate-600"><span className="text-slate-400">ИНН:</span> {result.structured_data.customer.inn || '—'}</div>
                                                     <div className="text-slate-600"><span className="text-slate-400">Контакты:</span> {result.structured_data.customer.contact_person || '—'} {result.structured_data.customer.phone || ''}</div>
                                                 </div>
                                             )}
                                             {result.structured_data.nmcc && (
                                                 <div className="bg-white p-4 rounded border border-slate-200 shadow-sm">
                                                     <div className="font-bold text-slate-700 mb-2 border-b pb-1">НМЦК</div>
                                                     <div className="text-slate-900 font-mono text-lg">{formatCurrency(result.structured_data.nmcc.total)}</div>
                                                 </div>
                                             )}
                                             {result.structured_data.delivery_terms && (
                                                 <div className="bg-white p-4 rounded border border-slate-200 shadow-sm md:col-span-2">
                                                     <div className="font-bold text-slate-700 mb-2 border-b pb-1">Сроки и условия поставки</div>
                                                     <div className="text-slate-600 mb-1"><span className="text-slate-400">Сроки:</span> {result.structured_data.delivery_terms}</div>
                                                     {result.structured_data.logistics && <div className="text-slate-600 mb-1"><span className="text-slate-400">Логистика:</span> {result.structured_data.logistics}</div>}
                                                     {result.structured_data.restrictions && <div className="text-slate-600"><span className="text-slate-400">Ограничения:</span> {result.structured_data.restrictions}</div>}
                                                 </div>
                                             )}
                                             {result.structured_data.items && result.structured_data.items.length > 0 && (
                                                 <div className="bg-white p-4 rounded border border-slate-200 shadow-sm md:col-span-2">
                                                     <div className="font-bold text-slate-700 mb-2 border-b pb-1">Позиции ({result.structured_data.items.length})</div>
                                                     <div className="overflow-x-auto">
                                                         <table className="w-full text-left text-xs">
                                                             <thead>
                                                                 <tr className="text-slate-400 border-b">
                                                                     <th className="pb-2 font-medium">Наименование</th>
                                                                     <th className="pb-2 font-medium">Кол-во</th>
                                                                     <th className="pb-2 font-medium">Цена за ед.</th>
                                                                     <th className="pb-2 font-medium">Характеристики</th>
                                                                 </tr>
                                                             </thead>
                                                             <tbody className="divide-y divide-slate-100">
                                                                 {result.structured_data.items.map((item: any, idx: number) => (
                                                                     <tr key={idx} className="text-slate-600">
                                                                         <td className="py-2 pr-2 font-medium text-slate-800">{item.name}</td>
                                                                         <td className="py-2 pr-2 whitespace-nowrap">{item.quantity} {item.unit}</td>
                                                                         <td className="py-2 pr-2 whitespace-nowrap">{item.price_per_unit ? formatCurrency(item.price_per_unit) : '—'}</td>
                                                                         <td className="py-2 text-[10px] text-slate-500">{item.characteristics}</td>
                                                                     </tr>
                                                                 ))}
                                                             </tbody>
                                                         </table>
                                                     </div>
                                                 </div>
                                             )}
                                         </div>
                                     </div>
                                 )}

                                 {/* 1. Main Legal Report (Markdown) - PRIMARY OUTPUT */}
                                 {result.status === 'success' && result.final_report_markdown && (
                                     <div className="px-6 py-10 bg-white border-b border-slate-100">
                                         <div className="flex items-center justify-between mb-8">
                                             <div className="flex items-center gap-3">
                                                 <div className="p-2 bg-indigo-600 rounded-xl text-white shadow-lg shadow-indigo-200">
                                                     <FileText size={20} />
                                                 </div>
                                                 <h4 className="text-2xl font-black text-slate-900 tracking-tight">Юридический отчет по тендеру</h4>
                                             </div>
                                         </div>
                                         <div className="markdown-body prose prose-slate max-w-none prose-headings:text-slate-900 prose-strong:text-slate-900 prose-table:border prose-table:border-slate-200 prose-th:bg-slate-50 prose-th:px-4 prose-th:py-2 prose-td:px-4 prose-td:py-2">
                                             <ReactMarkdown remarkPlugins={[remarkGfm]}>
                                                 {result.final_report_markdown}
                                             </ReactMarkdown>
                                         </div>
                                     </div>
                                 )}

                                 {/* Fallback for empty results */}
                                 {result.status === 'success' && !result.final_report_markdown && (
                                    <div className="p-10 text-center text-slate-400 bg-white">
                                        <Shield size={48} className="mx-auto mb-4 opacity-20" />
                                        <p className="text-sm font-medium">Документация выглядит стандартной. Критических условий не найдено.</p>
                                    </div>
                                 )}

                                 {/* Error Block */}
                                 {result.status === 'error' && (
                                     <div className="p-6 bg-red-50 border-b border-red-100">
                                         <div className="flex items-center gap-3 mb-4">
                                             <AlertTriangle size={24} className="text-red-500" />
                                             <h4 className="text-lg font-bold text-red-900">Ошибка анализа</h4>
                                         </div>
                                         <div className="text-red-700 text-sm whitespace-pre-wrap">
                                             {result.error_message || result.final_report_markdown || "Произошла неизвестная ошибка при анализе."}
                                         </div>
                                     </div>
                                 )}

                                {result.status === 'success' && (
                                    <div className="bg-slate-50 p-4 border-t border-slate-100 flex justify-end gap-6">
                                        <button onClick={() => exportToWord([result])} className="text-[10px] text-blue-600 font-black hover:underline flex items-center gap-1.5 uppercase tracking-wider">
                                            <FileDown size={14} /> Скачать отчет Word
                                        </button>
                                    </div>
                                )}
                            </div>
                        </div>
                    );
                })}
             </div>
          )}

          {/* 3. Single Result State (Match) */}
          {!loading && matchResult && activeTab === 'match' && (
            <div className="space-y-6">
              <div className={`p-6 rounded-xl border ${matchResult.is_equivalent ? 'bg-emerald-50 border-emerald-200' : 'bg-red-50 border-red-200'}`}>
                <div className="flex items-start gap-4">
                  <div className={`p-3 rounded-full ${matchResult.is_equivalent ? 'bg-emerald-100 text-emerald-600' : 'bg-red-100 text-red-600'}`}>
                    {matchResult.is_equivalent ? <CheckCircle size={24} /> : <AlertTriangle size={24} />}
                  </div>
                  <div>
                    <h3 className={`text-lg font-bold ${matchResult.is_equivalent ? 'text-emerald-800' : 'text-red-800'}`}>
                      {matchResult.is_equivalent ? 'Найден эквивалент' : 'Нет прямого аналога'}
                    </h3>
                    <p className="text-sm mt-1 opacity-80">
                      Уверенность ИИ: <strong>{(matchResult.confidence * 100).toFixed(0)}%</strong>
                    </p>
                  </div>
                </div>
              </div>

              {matchResult.recommended_product_id && (
                 <div className="bg-white p-6 rounded-xl border border-slate-200 shadow-sm">
                   <p className="text-xs text-slate-400 uppercase tracking-wider font-bold mb-3">Рекомендованный продукт</p>
                   {(() => {
                     const p = getRecommendedProduct(matchResult.recommended_product_id);
                     if (!p) return null;
                     return (
                       <div>
                         <h4 className="text-xl font-bold text-slate-800">{p.title}</h4>
                         <p className="text-sm text-slate-500 mb-4">{p.category} | {p.material_type}</p>
                         <div className="grid grid-cols-2 gap-4 bg-slate-50 p-4 rounded-lg">
                            {Object.entries(p.specs).map(([k,v]) => (
                                <div key={k}>
                                    <span className="block text-xs text-slate-400 capitalize">{formatSpecKey(k)}</span>
                                    <span className="font-medium text-slate-800">{String(v)}</span>
                                </div>
                            ))}
                         </div>
                       </div>
                     );
                   })()}
                 </div>
              )}

              <div className="bg-white p-6 rounded-xl border border-slate-200 shadow-sm">
                <h4 className="font-bold text-slate-800 mb-2">Обоснование ИИ</h4>
                <p className="text-slate-600 text-sm leading-relaxed">{matchResult.reasoning}</p>
                {matchResult.critical_mismatches.length > 0 && (
                  <div className="mt-4 pt-4 border-t border-slate-100">
                     <h5 className="text-red-600 text-sm font-bold mb-2">Критические расхождения:</h5>
                     <ul className="list-disc pl-5 text-sm text-slate-600 space-y-1">
                        {matchResult.critical_mismatches.map((m, i) => (
                            <li key={i}>{m}</li>
                        ))}
                     </ul>
                  </div>
                )}
              </div>
            </div>
          )}
          
          {/* Empty State */}
          {!loading && !matchResult && Object.keys(batchResults).length === 0 && (
              <div className="h-full flex flex-col items-center justify-center text-slate-400 opacity-50">
                  <Cpu size={48} className="mb-4"/>
                  <p>Результаты появятся здесь</p>
              </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default Analysis;
