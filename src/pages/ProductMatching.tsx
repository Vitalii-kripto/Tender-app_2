import React, { useState, useEffect } from 'react';
import { Search, Globe, Database, Upload, CheckCircle, AlertTriangle, ArrowRight, Loader2, Layers, FileText, Plus, Trash2, FileUp, Zap, FileDown, Eye, Wand2, RefreshCw } from 'lucide-react';
import { MOCK_CATALOG } from './ProductCatalog';
import { Product, Tender } from '../types';
import { findProductEquivalent, searchProductsInternet, validateProductCompliance, getTendersFromBackend, uploadTenderFile, extractProductsFromText, validateComplexCompliance, enrichProductSpecs } from '../services/geminiService';

interface SearchItem {
    id: string;
    query: string; // Название или описание для поиска
    status: 'idle' | 'loading' | 'done';
    results: any[];
    source: 'database' | 'internet';
}

const ProductMatching = () => {
  // --- STATE ---
  const [activeTab, setActiveTab] = useState<'search' | 'validate'>('validate'); 
  const [availableTenders, setAvailableTenders] = useState<Tender[]>([]);

  // -- SEARCH TAB STATE (Refactored) --
  const [searchItems, setSearchItems] = useState<SearchItem[]>([{ id: '1', query: '', status: 'idle', results: [], source: 'database' }]);
  const [selectedSearchTenderId, setSelectedSearchTenderId] = useState<string>('custom');
  const [searchExtracting, setSearchExtracting] = useState(false);

  // -- VALIDATION TAB STATE --
  const [selectedValidationTenderId, setSelectedValidationTenderId] = useState<string>('custom');
  const [requirementsText, setRequirementsText] = useState('');
  const [proposalItems, setProposalItems] = useState<{id: string, name: string, quantity: string, specs: string, loading?: boolean}[]>([
      { id: '1', name: '', quantity: '', specs: '' }
  ]);
  const [validationExtracting, setValidationExtracting] = useState(false);
  const [validationLoading, setValidationLoading] = useState(false);
  const [complianceReport, setComplianceReport] = useState<any>(null);

  useEffect(() => {
    getTendersFromBackend().then(setAvailableTenders).catch(console.error);
  }, []);

  // ==========================
  // HANDLERS: SEARCH TAB
  // ==========================

  const handleSearchTenderSelect = async (e: React.ChangeEvent<HTMLSelectElement>) => {
      const id = e.target.value;
      setSelectedSearchTenderId(id);
      
      if (id === 'custom') {
          setSearchItems([{ id: Date.now().toString(), query: '', status: 'idle', results: [], source: 'database' }]);
          return;
      }

      const tender = availableTenders.find(t => t.id === id);
      if (tender) {
          setSearchExtracting(true);
          try {
              const items = await extractProductsFromText(tender.description);
              if (items && items.length > 0) {
                  setSearchItems(items.map((item: any, idx: number) => ({
                      id: Date.now() + idx.toString(),
                      query: `${item.name} ${item.specs}`,
                      status: 'idle',
                      results: [],
                      source: 'database'
                  })));
              } else {
                  setSearchItems([{ id: '1', query: tender.description.substring(0, 100), status: 'idle', results: [], source: 'database' }]);
              }
          } catch(e) {
              setSearchItems([{ id: '1', query: tender.description.substring(0, 100), status: 'idle', results: [], source: 'database' }]);
          } finally {
              setSearchExtracting(false);
          }
      }
  };

  const handleSearchFileUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
      if (!e.target.files || e.target.files.length === 0) return;
      setSearchExtracting(true);
      try {
          let allItems: any[] = [];
          for (let i = 0; i < e.target.files.length; i++) {
              const file = e.target.files[i];
              const { text } = await uploadTenderFile(file);
              const items = await extractProductsFromText(text);
              if (items && items.length > 0) {
                  allItems = [...allItems, ...items];
              }
          }
          if (allItems.length > 0) {
              setSearchItems(allItems.map((item: any, idx: number) => ({
                  id: Date.now() + idx.toString(),
                  query: `${item.name} ${item.specs}`,
                  status: 'idle',
                  results: [],
                  source: 'database'
              })));
          } else {
              alert("Не удалось распознать товары.");
          }
      } catch(e) {
          console.error(e);
          alert("Ошибка загрузки файла.");
      } finally {
          setSearchExtracting(false);
      }
  };

  const addSearchItem = () => {
      setSearchItems(prev => [...prev, { id: Date.now().toString(), query: '', status: 'idle', results: [], source: 'database' }]);
  };

  const removeSearchItem = (id: string) => {
      setSearchItems(prev => prev.filter(item => item.id !== id));
  };

  const updateSearchItem = (id: string, field: keyof SearchItem, value: any) => {
      setSearchItems(prev => prev.map(item => item.id === id ? { ...item, [field]: value } : item));
  };

  const runSingleSearch = async (id: string) => {
      const item = searchItems.find(i => i.id === id);
      if (!item || !item.query) return;

      updateSearchItem(id, 'status', 'loading');
      updateSearchItem(id, 'results', []);

      try {
          if (item.source === 'database') {
              const result = await findProductEquivalent(item.query);
              if (result.all_matches) {
                   const hydrated = result.all_matches.map((m: any) => {
                      const localProd = MOCK_CATALOG.find(p => p.id === m.id);
                      return { ...(localProd || {}), ...m };
                  });
                  updateSearchItem(id, 'results', hydrated);
              } else {
                  updateSearchItem(id, 'results', []);
              }
          } else {
              const result = await searchProductsInternet(item.query);
              updateSearchItem(id, 'results', [{ id: 'web', title: 'Результаты поиска (Google)', description: result.text, is_web: true }]);
          }
      } catch (e) {
          console.error(e);
      } finally {
          updateSearchItem(id, 'status', 'done');
      }
  };

  const runAllSearches = () => {
      searchItems.forEach(item => {
          if (item.query && item.status !== 'loading') {
              runSingleSearch(item.id);
          }
      });
  };

  // ==========================
  // HANDLERS: VALIDATION TAB
  // ==========================

  const handleValidationTenderSelect = (e: React.ChangeEvent<HTMLSelectElement>) => {
      const id = e.target.value;
      setSelectedValidationTenderId(id);
      if (id === 'custom') {
          setRequirementsText('');
      } else {
          const tender = availableTenders.find(t => t.id === id);
          if (tender) {
              setRequirementsText(`ТЕНДЕР: ${tender.title}\n\nОПИСАНИЕ ЗАКУПКИ (ТЗ):\n${tender.description}\n\nТРЕБУЕМЫЕ ХАРАКТЕРИСТИКИ:\n(Автоматически подгружено из CRM)`);
          }
      }
  };

  const handleRequirementUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
      if (!e.target.files || e.target.files.length === 0) return;
      setRequirementsText("Загрузка текста из файлов...");
      try {
          let allText = "";
          for (let i = 0; i < e.target.files.length; i++) {
              const file = e.target.files[i];
              const { text } = await uploadTenderFile(file);
              allText += `\n--- Файл: ${file.name} ---\n${text}\n`;
          }
          setRequirementsText(allText);
      } catch(err) {
          setRequirementsText("Ошибка чтения файлов.");
      }
  };

  const handleProposalUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
      if (!e.target.files || e.target.files.length === 0) return;
      setValidationExtracting(true);
      try {
          let allItems: any[] = [];
          for (let i = 0; i < e.target.files.length; i++) {
              const file = e.target.files[i];
              const { text } = await uploadTenderFile(file);
              const items = await extractProductsFromText(text);
              if (items && items.length > 0) {
                  allItems = [...allItems, ...items];
              }
          }
          if (allItems.length > 0) {
              setProposalItems(allItems.map((item: any, idx: number) => ({
                  id: Date.now() + idx.toString(),
                  name: item.name || '',
                  quantity: item.quantity || '',
                  specs: item.specs || ''
              })));
          } else {
              alert("Не удалось извлечь товары из файлов.");
          }
      } catch(err) {
          console.error(err);
          alert("Ошибка обработки файлов предложения.");
      } finally {
          setValidationExtracting(false);
      }
  };

  const updateProposalItem = (id: string, field: string, value: string) => {
      setProposalItems(prev => prev.map(item => item.id === id ? { ...item, [field]: value } : item));
  };

  const handleEnrichItem = async (id: string, name: string) => {
      if (!name) return;
      setProposalItems(prev => prev.map(item => item.id === id ? { ...item, loading: true } : item));
      try {
          const specs = await enrichProductSpecs(name);
          setProposalItems(prev => prev.map(item => item.id === id ? { ...item, specs: specs, loading: false } : item));
      } catch(e) {
          setProposalItems(prev => prev.map(item => item.id === id ? { ...item, loading: false } : item));
      }
  };

  const addProposalItem = () => {
      setProposalItems(prev => [...prev, { id: Date.now().toString(), name: '', quantity: '', specs: '' }]);
  };

  const removeProposalItem = (id: string) => {
      setProposalItems(prev => prev.filter(item => item.id !== id));
  };

  const runValidation = async () => {
      if (!requirementsText || proposalItems.length === 0) return;
      setValidationLoading(true);
      setComplianceReport(null);
      try {
          const result = await validateComplexCompliance(requirementsText, proposalItems);
          setComplianceReport(result);
      } catch (e) {
          console.error(e);
      } finally {
          setValidationLoading(false);
      }
  };

  const inputClass = "w-full border border-slate-300 rounded-lg p-2.5 text-sm bg-white text-slate-900 placeholder-slate-400 focus:outline-none focus:ring-2 focus:ring-blue-500";

  return (
    <div className="p-6 h-[calc(100vh-64px)] flex flex-col">
      <div className="mb-4 flex justify-between items-center flex-shrink-0">
        <div>
            <h2 className="text-2xl font-bold text-slate-900 flex items-center gap-2">
            <Layers className="text-blue-600" />
            Подбор и Валидация
            </h2>
            <p className="text-slate-500 text-sm">Сравнение ТЗ с предложением или поиск аналога.</p>
        </div>
        <div className="flex bg-slate-100 p-1 rounded-lg">
            <button onClick={() => setActiveTab('validate')} className={`px-4 py-2 text-sm font-medium rounded-md transition-all ${activeTab === 'validate' ? 'bg-white shadow text-blue-600' : 'text-slate-500'}`}>Проверка ТЗ</button>
            <button onClick={() => setActiveTab('search')} className={`px-4 py-2 text-sm font-medium rounded-md transition-all ${activeTab === 'search' ? 'bg-white shadow text-blue-600' : 'text-slate-500'}`}>Поиск аналога</button>
        </div>
      </div>

      {/* VALIDATION TAB CONTENT */}
      {activeTab === 'validate' && (
          <div className="flex-1 grid grid-cols-1 lg:grid-cols-3 gap-6 overflow-hidden min-h-0">
              {/* COL 1: SOURCE */}
              <div className="flex flex-col bg-white border border-slate-200 rounded-xl shadow-sm overflow-hidden">
                  <div className="p-4 bg-slate-50 border-b border-slate-200 font-bold text-slate-700 flex justify-between items-center">
                      <span>1. Требования (ТЗ)</span>
                      <div className="relative group cursor-pointer hover:text-blue-600 transition-colors">
                          <FileUp size={16} />
                          <span className="ml-1 text-xs">Загрузить ТЗ</span>
                          <input type="file" multiple className="absolute inset-0 opacity-0 cursor-pointer" onChange={handleRequirementUpload}/>
                      </div>
                  </div>
                  <div className="p-4 flex-1 flex flex-col overflow-y-auto">
                      <div className="mb-4">
                          <label className="text-xs font-bold text-slate-500 uppercase mb-1 block">Источник</label>
                          <select value={selectedValidationTenderId} onChange={handleValidationTenderSelect} className={inputClass}>
                              <option value="custom">-- Ручной ввод / Файл --</option>
                              {availableTenders.map(t => (
                                  <option key={t.id} value={t.id}>{t.eis_number} - {t.title.substring(0, 30)}...</option>
                              ))}
                          </select>
                      </div>
                      <textarea 
                          className={`${inputClass} flex-1 resize-none font-mono text-xs`} 
                          placeholder="Вставьте текст требований из ТЗ или выберите тендер..."
                          value={requirementsText}
                          onChange={e => setRequirementsText(e.target.value)}
                      />
                  </div>
              </div>

              {/* COL 2: PROPOSAL */}
              <div className="flex flex-col bg-white border border-slate-200 rounded-xl shadow-sm overflow-hidden relative">
                  <div className="p-4 bg-slate-50 border-b border-slate-200 font-bold text-slate-700 flex justify-between items-center">
                      <span>2. Предложение (КП)</span>
                      <div className="relative cursor-pointer bg-blue-600 hover:bg-blue-700 text-white px-3 py-1.5 rounded-lg text-xs font-medium flex items-center gap-1 shadow-sm transition-all">
                          <Zap size={12}/>
                          <span>Загрузить Тех.Предложение</span>
                          <input type="file" multiple className="absolute inset-0 opacity-0 cursor-pointer" onChange={handleProposalUpload}/>
                      </div>
                  </div>
                  
                  {validationExtracting && (
                      <div className="absolute inset-0 bg-white/80 z-10 flex flex-col items-center justify-center backdrop-blur-sm">
                          <Loader2 size={40} className="animate-spin text-blue-600 mb-2"/>
                          <p className="text-sm font-medium text-slate-700">ИИ анализирует файл предложения...</p>
                      </div>
                  )}

                  <div className="p-4 flex-1 overflow-y-auto space-y-3 bg-slate-50/50 custom-scrollbar">
                      {proposalItems.map((item, idx) => (
                          <div key={item.id} className="bg-white p-3 rounded-lg border border-slate-200 shadow-sm relative group">
                              <div className="flex justify-between mb-2">
                                  <span className="text-xs font-bold text-slate-400">Позиция #{idx + 1}</span>
                                  <button onClick={() => removeProposalItem(item.id)} className="text-slate-300 hover:text-red-500"><Trash2 size={14}/></button>
                              </div>
                              
                              <div className="flex gap-2 mb-2">
                                  <input 
                                      type="text" 
                                      placeholder="Название материала (например, Техноэласт ЭПП)" 
                                      className="flex-1 border-b border-slate-200 pb-1 text-sm font-medium focus:outline-none focus:border-blue-500 text-slate-900 bg-transparent placeholder-slate-400"
                                      value={item.name}
                                      onChange={e => updateProposalItem(item.id, 'name', e.target.value)}
                                  />
                                  <button 
                                      onClick={() => handleEnrichItem(item.id, item.name)}
                                      disabled={!item.name || item.loading}
                                      className="text-blue-600 hover:bg-blue-50 p-1.5 rounded disabled:opacity-30 transition-colors"
                                      title="Найти реальные характеристики в интернете (Волшебная палочка)"
                                  >
                                      {item.loading ? <Loader2 size={16} className="animate-spin"/> : <Wand2 size={16} />}
                                  </button>
                              </div>

                              <div className="flex gap-2 mb-2">
                                  <input 
                                      type="text" 
                                      placeholder="Кол-во" 
                                      className="w-1/3 bg-slate-50 border border-slate-200 rounded px-2 py-1 text-xs text-slate-900 placeholder-slate-400"
                                      value={item.quantity}
                                      onChange={e => updateProposalItem(item.id, 'quantity', e.target.value)}
                                  />
                              </div>
                              <textarea 
                                  placeholder="Характеристики (заполните вручную или нажмите палочку)" 
                                  className="w-full bg-slate-50 border border-slate-200 rounded px-2 py-1 text-xs text-slate-600 resize-none h-20 focus:bg-white focus:ring-1 focus:ring-blue-500 outline-none placeholder-slate-400"
                                  value={item.specs}
                                  onChange={e => updateProposalItem(item.id, 'specs', e.target.value)}
                              />
                          </div>
                      ))}
                      <button onClick={addProposalItem} className="w-full py-2 border-2 border-dashed border-slate-300 rounded-lg text-slate-500 text-sm font-medium hover:border-blue-500 hover:text-blue-600 transition-colors flex items-center justify-center gap-2">
                          <Plus size={16}/> Добавить позицию
                      </button>
                  </div>
                  
                  <div className="p-4 border-t border-slate-200">
                      <button 
                        onClick={runValidation}
                        disabled={validationLoading || proposalItems.length === 0}
                        className="w-full py-3 bg-gradient-to-r from-blue-600 to-indigo-600 text-white rounded-xl font-bold shadow-lg hover:shadow-xl transition-all disabled:opacity-50 flex items-center justify-center gap-2"
                      >
                          {validationLoading ? <Loader2 className="animate-spin"/> : <CheckCircle size={20}/>}
                          {validationLoading ? 'Тройная проверка (ТЗ-КП-Интернет)...' : 'Сравнить'}
                      </button>
                      <p className="text-[10px] text-center text-slate-400 mt-2">
                          ИИ проверит соответствие ТЗ, а также сверит заявленные характеристики с реальными данными из интернета.
                      </p>
                  </div>
              </div>

              {/* COL 3: REPORT */}
              <div className="flex flex-col bg-white border border-slate-200 rounded-xl shadow-sm overflow-hidden">
                  <div className="p-4 bg-slate-50 border-b border-slate-200 font-bold text-slate-700">3. Отчет о соответствии</div>
                  
                  {!complianceReport && !validationLoading && (
                      <div className="flex-1 flex flex-col items-center justify-center text-slate-300 p-6 text-center">
                          <FileText size={48} className="mb-4 opacity-50"/>
                          <p>Результат сравнения появится здесь</p>
                      </div>
                  )}

                  {validationLoading && (
                      <div className="flex-1 flex flex-col items-center justify-center text-slate-400">
                          <Loader2 size={40} className="animate-spin text-blue-500 mb-4"/>
                          <p className="font-bold">Идет тройная проверка...</p>
                          <ul className="text-xs mt-2 space-y-1 opacity-70">
                              <li>1. Анализ требований ТЗ</li>
                              <li>2. Поиск реальных характеристик (Web)</li>
                              <li>3. Сверка данных</li>
                          </ul>
                      </div>
                  )}

                  {complianceReport && !validationLoading && (
                      <div className="flex-1 overflow-y-auto p-0 custom-scrollbar">
                          <div className={`p-6 text-center text-white ${complianceReport.score > 80 ? 'bg-emerald-500' : complianceReport.score > 50 ? 'bg-amber-500' : 'bg-red-500'}`}>
                              <div className="text-4xl font-bold mb-1">{complianceReport.score}%</div>
                              <div className="text-sm font-medium opacity-90">Уровень соответствия</div>
                          </div>
                          
                          <div className="p-4 bg-slate-50 border-b border-slate-200">
                              <p className="text-sm font-medium text-slate-700">{complianceReport.summary}</p>
                          </div>

                          <div className="divide-y divide-slate-100">
                              {complianceReport.items.map((item: any, idx: number) => (
                                  <div key={idx} className="p-4 hover:bg-slate-50 transition-colors">
                                      <div className="flex justify-between items-start mb-1">
                                          <span className="text-xs font-bold text-slate-500 uppercase">Требование</span>
                                          <span className={`text-[10px] font-bold px-2 py-0.5 rounded uppercase ${
                                              item.status === 'OK' ? 'bg-emerald-100 text-emerald-700' : 
                                              item.status === 'FAIL' ? 'bg-red-100 text-red-700' : 
                                              item.status === 'FAKE' ? 'bg-purple-100 text-purple-700' : 
                                              item.status === 'MISSING' ? 'bg-gray-100 text-gray-700' : 'bg-amber-100 text-amber-700'
                                          }`}>
                                              {item.status}
                                          </span>
                                      </div>
                                      <p className="text-sm font-semibold text-slate-800 mb-2">{item.requirement_name}</p>
                                      
                                      <div className="grid grid-cols-1 gap-2 mb-2 bg-blue-50/50 p-2 rounded">
                                          <div className="flex items-center gap-2">
                                              <ArrowRight size={14} className="text-blue-400"/>
                                              <span className="text-xs text-blue-800 font-bold">{item.proposal_name || 'Не предложено'}</span>
                                          </div>
                                          {item.real_specs_found && (
                                              <div className="flex items-start gap-2">
                                                  <Globe size={12} className="text-slate-400 mt-0.5"/>
                                                  <span className="text-[10px] text-slate-500 italic leading-tight">{item.real_specs_found}</span>
                                              </div>
                                          )}
                                      </div>
                                      
                                      <p className="text-xs text-slate-600 bg-white p-2 border border-slate-100 rounded">
                                          {item.comment}
                                      </p>
                                  </div>
                              ))}
                          </div>
                      </div>
                  )}
              </div>
          </div>
      )}

      {/* SEARCH TAB CONTENT */}
      {activeTab === 'search' && (
          <div className="flex-1 flex flex-col md:flex-row gap-6 overflow-hidden min-h-0">
               {/* LEFT COLUMN */}
               <div className="w-full md:w-1/3 bg-white border border-slate-200 rounded-xl shadow-sm overflow-hidden flex flex-col">
                    <div className="p-4 bg-slate-50 border-b border-slate-200">
                        <div className="mb-4">
                             <label className="text-xs font-bold text-slate-500 uppercase mb-1 block">Источник ТЗ / Списка</label>
                             <div className="flex gap-2">
                                <select value={selectedSearchTenderId} onChange={handleSearchTenderSelect} className={inputClass}>
                                    <option value="custom">-- Ручной ввод / Файл --</option>
                                    {availableTenders.map(t => (
                                        <option key={t.id} value={t.id}>{t.eis_number} - {t.title.substring(0, 25)}...</option>
                                    ))}
                                </select>
                                <div className="relative group cursor-pointer bg-white border border-slate-300 rounded-lg w-10 flex items-center justify-center hover:bg-blue-50">
                                    <FileUp size={18} className="text-slate-500 group-hover:text-blue-600"/>
                                    <input type="file" multiple className="absolute inset-0 opacity-0 cursor-pointer" onChange={handleSearchFileUpload}/>
                                </div>
                             </div>
                        </div>
                        {searchExtracting && <div className="text-xs text-blue-600 animate-pulse text-center mb-2">ИИ извлекает товары из источника...</div>}
                    </div>

                    <div className="flex-1 overflow-y-auto p-4 space-y-3 bg-slate-50/50 custom-scrollbar">
                        {searchItems.map((item, idx) => (
                            <div key={item.id} className={`p-3 rounded-lg border shadow-sm transition-all ${item.status === 'loading' ? 'bg-blue-50 border-blue-200' : 'bg-white border-slate-200'}`}>
                                <div className="flex justify-between items-start mb-2">
                                    <span className="text-[10px] font-bold text-slate-400">Позиция #{idx + 1}</span>
                                    <button onClick={() => removeSearchItem(item.id)} className="text-slate-300 hover:text-red-500"><Trash2 size={12}/></button>
                                </div>
                                <textarea 
                                    className="w-full text-sm border-b border-slate-200 focus:border-blue-500 focus:outline-none bg-transparent resize-none h-16 placeholder-slate-400 text-slate-900" 
                                    placeholder="Введите название или требования (напр. 'Техноэласт ЭПП, толщина 4мм')"
                                    value={item.query}
                                    onChange={e => updateSearchItem(item.id, 'query', e.target.value)}
                                />
                                <div className="flex items-center gap-2 mt-2">
                                    <div className="flex rounded bg-slate-100 p-0.5">
                                        <button 
                                            onClick={() => updateSearchItem(item.id, 'source', 'database')}
                                            className={`px-2 py-1 text-[10px] font-bold rounded ${item.source === 'database' ? 'bg-white shadow text-blue-700' : 'text-slate-500'}`}
                                        >
                                            База
                                        </button>
                                        <button 
                                            onClick={() => updateSearchItem(item.id, 'source', 'internet')}
                                            className={`px-2 py-1 text-[10px] font-bold rounded ${item.source === 'internet' ? 'bg-white shadow text-blue-700' : 'text-slate-500'}`}
                                        >
                                            Internet
                                        </button>
                                    </div>
                                    <button 
                                        onClick={() => runSingleSearch(item.id)}
                                        disabled={item.status === 'loading' || !item.query}
                                        className="ml-auto bg-blue-100 text-blue-700 p-1.5 rounded hover:bg-blue-200 disabled:opacity-50"
                                    >
                                        {item.status === 'loading' ? <Loader2 size={14} className="animate-spin"/> : <Search size={14}/>}
                                    </button>
                                </div>
                            </div>
                        ))}
                        <button onClick={addSearchItem} className="w-full py-2 border-2 border-dashed border-slate-300 rounded-lg text-slate-400 text-sm hover:border-blue-400 hover:text-blue-500 flex items-center justify-center gap-1">
                             <Plus size={14}/> Добавить
                        </button>
                    </div>

                    <div className="p-4 border-t border-slate-200">
                        <button 
                            onClick={runAllSearches} 
                            disabled={searchItems.length === 0}
                            className="w-full bg-blue-600 text-white py-2.5 rounded-lg font-bold hover:bg-blue-700 flex justify-center items-center gap-2 disabled:opacity-50"
                        >
                            <RefreshCw size={16}/> Найти все аналог(и)
                        </button>
                    </div>
               </div>

               {/* RIGHT COLUMN: Results */}
               <div className="flex-1 bg-white border border-slate-200 rounded-xl shadow-sm overflow-hidden flex flex-col">
                    <div className="p-4 bg-slate-50 border-b border-slate-200 font-bold text-slate-700">Результаты поиска</div>
                    <div className="flex-1 overflow-y-auto p-6 space-y-6 custom-scrollbar">
                        {searchItems.filter(i => i.status === 'done' || i.results.length > 0).length === 0 ? (
                            <div className="text-center text-slate-400 mt-20">
                                <Search size={48} className="mx-auto mb-4 opacity-30"/>
                                <p>Добавьте позиции слева и нажмите "Найти"</p>
                            </div>
                        ) : (
                            searchItems.map((item, idx) => (
                                (item.results.length > 0 || item.status === 'done') && (
                                    <div key={item.id} className="animate-in fade-in slide-in-from-bottom-2">
                                        <div className="flex items-center gap-2 mb-2">
                                            <span className="bg-slate-200 text-slate-600 text-xs font-bold px-2 py-0.5 rounded">Позиция #{idx + 1}</span>
                                            <h4 className="font-bold text-slate-800 text-sm truncate">{item.query}</h4>
                                        </div>
                                        
                                        {item.results.length === 0 ? (
                                            <div className="p-4 bg-slate-50 rounded-lg text-sm text-slate-500 border border-slate-100">
                                                Ничего не найдено. Попробуйте уточнить запрос или сменить источник.
                                            </div>
                                        ) : (
                                            <div className="grid grid-cols-1 gap-3">
                                                {item.results.map((res, rIdx) => (
                                                    <div key={rIdx} className="p-4 border border-slate-200 rounded-lg hover:shadow-md transition-shadow bg-white relative group">
                                                        {res.is_web ? (
                                                            <div>
                                                                <div className="flex items-center gap-2 mb-2 text-blue-600 font-bold text-xs">
                                                                    <Globe size={12} /> 
                                                                    Результат поиска в Google
                                                                </div>
                                                                <div className="prose prose-sm prose-slate text-xs max-w-none">
                                                                     {/* Simple render for text with potential markdown */}
                                                                     {res.description.split('\n').map((line: string, i: number) => (
                                                                         <p key={i} className="mb-1 leading-relaxed">
                                                                             {line.startsWith('**') || line.startsWith('#') 
                                                                                ? <strong className="text-slate-800 block mt-2">{line.replace(/[*#]/g, '')}</strong> 
                                                                                : line.replace(/[*]/g, '')}
                                                                         </p>
                                                                     ))}
                                                                </div>
                                                            </div>
                                                        ) : (
                                                            <div className="flex justify-between items-start">
                                                                <div>
                                                                    <h4 className="font-bold text-sm text-slate-800 flex items-center gap-2">
                                                                        {res.title}
                                                                        {res.similarity_score >= 90 && <CheckCircle size={14} className="text-emerald-500" />}
                                                                    </h4>
                                                                    <div className="text-xs text-slate-600 mt-1">
                                                                        <span className="font-semibold text-slate-400">Почему подходит:</span> {res.match_reason}
                                                                    </div>
                                                                    {res.specs && (
                                                                        <div className="mt-2 text-[10px] text-slate-500 grid grid-cols-2 gap-x-4">
                                                                            {Object.entries(res.specs).slice(0,4).map(([k,v]) => (
                                                                                <div key={k}><span className="opacity-70">{k}:</span> {String(v)}</div>
                                                                            ))}
                                                                        </div>
                                                                    )}
                                                                </div>
                                                                <div className="text-right flex-shrink-0 ml-2">
                                                                    <span className={`inline-block font-bold px-2 py-1 rounded text-xs ${res.similarity_score > 80 ? 'bg-emerald-100 text-emerald-700' : 'bg-amber-100 text-amber-700'}`}>
                                                                        {res.similarity_score}%
                                                                    </span>
                                                                    {res.price && res.price > 0 ? <div className="mt-1 text-sm font-bold text-slate-900">₽{res.price}</div> : null}
                                                                </div>
                                                            </div>
                                                        )}
                                                    </div>
                                                ))}
                                            </div>
                                        )}
                                        <div className="border-b border-slate-100 my-4"></div>
                                    </div>
                                )
                            ))
                        )}
                    </div>
               </div>
          </div>
      )}
    </div>
  );
};

export default ProductMatching;