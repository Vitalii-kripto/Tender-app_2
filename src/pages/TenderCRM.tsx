import React, { useState, useEffect } from 'react';
import { Calendar, DollarSign, AlertCircle, ArrowRight, ArrowLeft, Trash2, Plus, Info, Link, FileUp, X, Check, Loader2, UserPlus, User, Server, HardDrive, Globe, FileText, Sparkles } from 'lucide-react';
import { useNavigate } from 'react-router-dom';
import { Tender, TenderStatus, Employee } from '../types';
import { getTendersFromBackend, addOrUpdateTender, deleteTenderFromBackend, getEmployees, checkBackendHealth, searchTenders, uploadTenderFile, extractDetailsFromText } from '../services/geminiService';

const COLUMNS: { id: TenderStatus, title: string, color: string }[] = [
  { id: 'Found', title: 'Найдено', color: 'border-l-blue-500' },
  { id: 'Calculation', title: 'В расчете', color: 'border-l-indigo-500' },
  { id: 'Applied', title: 'Подана заявка', color: 'border-l-amber-500' },
  { id: 'Contract', title: 'Контракт', color: 'border-l-emerald-500' }
];

const TenderCRM = () => {
  const navigate = useNavigate();
  const [tenders, setTenders] = useState<Tender[]>([]);
  const [employees, setEmployees] = useState<Employee[]>([]);
  const [loading, setLoading] = useState(true);
  const [showAddModal, setShowAddModal] = useState(false);
  const [isServerOnline, setIsServerOnline] = useState(false);
  
  // --- ADD MODAL STATE ---
  const [addMode, setAddMode] = useState<'link' | 'file'>('link');
  const [linkInput, setLinkInput] = useState('');
  const [isAnalyzing, setIsAnalyzing] = useState(false);
  const [analysisStatus, setAnalysisStatus] = useState('');
  
  // Manual Form State
  const [manualForm, setManualForm] = useState({ 
      eis_number: '', 
      title: '', 
      price: '', 
      deadline: '', 
      description: '',
      url: '' 
  });

  // Load from Backend on mount
  useEffect(() => {
    loadData();
    checkBackendHealth().then(setIsServerOnline);
  }, []);

  const loadData = async () => {
    setLoading(true);
    try {
        const [data, emps] = await Promise.all([
            getTendersFromBackend(),
            Promise.resolve(getEmployees()) // Sync call wrapped
        ]);
        setTenders(data);
        setEmployees(emps);
    } catch (e) {
        console.error("Failed to load CRM data", e);
    } finally {
        setLoading(false);
    }
  };

  // --- LOGIC FOR SMART ADD ---

  const extractIdFromUrl = (url: string) => {
      const match = url.match(/(\d{11,19})/); // Ищем последовательность 11-19 цифр (номер закупки)
      return match ? match[0] : null;
  };

  const handleParseLink = async () => {
      if (!linkInput) return;
      setIsAnalyzing(true);
      setAnalysisStatus('Извлечение номера закупки...');

      const eisId = extractIdFromUrl(linkInput);
      if (!eisId) {
          alert('Не удалось найти номер закупки в ссылке. Убедитесь, что это ссылка на zakupki.gov.ru');
          setIsAnalyzing(false);
          return;
      }

      setAnalysisStatus(`Поиск данных для №${eisId} в ЕИС...`);
      try {
          // Используем существующий механизм поиска по номеру
          const results = await searchTenders(eisId, "", true);
          if (results.length > 0) {
              const t = results[0];
              setManualForm({
                  eis_number: t.eis_number,
                  title: t.title,
                  price: t.initial_price.toString(),
                  deadline: t.deadline,
                  description: t.description,
                  url: linkInput
              });
              setAnalysisStatus('Данные успешно загружены!');
          } else {
              alert('Закупка не найдена в поиске. Попробуйте ввести данные вручную.');
          }
      } catch (e) {
          console.error(e);
          alert('Ошибка при обращении к серверу.');
      } finally {
          setIsAnalyzing(false);
      }
  };

  const handleFileUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
      if (!e.target.files || e.target.files.length === 0) return;
      
      const file = e.target.files[0];
      setIsAnalyzing(true);
      setAnalysisStatus('Загрузка и OCR обработка файла...');

      try {
          // 1. Загрузка файла на сервер и извлечение текста
          const { text } = await uploadTenderFile(file);
          
          if (!text || text.length < 50) {
              throw new Error("Не удалось извлечь текст из файла.");
          }

          setAnalysisStatus('ИИ анализирует содержимое (Gemini)...');

          // 2. Отправка текста в AI для извлечения сущностей
          const details = await extractDetailsFromText(text);
          
          setManualForm({
              eis_number: details.eis_number || 'AUTO-GEN',
              title: details.title || file.name,
              price: details.initial_price ? details.initial_price.toString() : '',
              deadline: details.deadline || '-',
              description: details.description || 'Извлечено из файла',
              url: ''
          });

      } catch (err) {
          console.error(err);
          alert('Ошибка обработки файла. Попробуйте другой формат или введите вручную.');
      } finally {
          setIsAnalyzing(false);
      }
  };


  const handleSaveTender = async (e: React.FormEvent) => {
    e.preventDefault();
    const newTender: Tender = {
        id: `tender_${Date.now()}`,
        eis_number: manualForm.eis_number || 'MANUAL',
        title: manualForm.title || 'Новый тендер',
        description: manualForm.description || 'Добавлено вручную',
        initial_price: Number(manualForm.price) || 0,
        deadline: manualForm.deadline || '-',
        status: 'Found',
        risk_level: 'Low',
        region: 'Manual',
        law_type: 'Коммерч.',
        url: manualForm.url
    };

    setTenders(prev => [...prev, newTender]);
    setShowAddModal(false);
    await addOrUpdateTender(newTender);
    // Reset form
    setManualForm({ eis_number: '', title: '', price: '', deadline: '', description: '', url: '' });
    setLinkInput('');
  };

  const moveStatus = async (id: string, direction: 'next' | 'prev') => {
    const statusOrder: TenderStatus[] = ['Found', 'Calculation', 'Applied', 'Contract'];
    const tender = tenders.find(t => t.id === id);
    if (!tender) return;

    const currentIndex = statusOrder.indexOf(tender.status);
    let newIndex = direction === 'next' ? currentIndex + 1 : currentIndex - 1;
    if (newIndex < 0) newIndex = 0;
    if (newIndex >= statusOrder.length) newIndex = statusOrder.length - 1;
    
    const newStatus = statusOrder[newIndex];
    const updatedTenders = tenders.map(t => t.id === id ? { ...t, status: newStatus } : t);
    setTenders(updatedTenders);
    await addOrUpdateTender({ ...tender, status: newStatus });
  };

  const assignEmployee = async (tenderId: string, empId: string) => {
      const tender = tenders.find(t => t.id === tenderId);
      if (!tender) return;
      
      const updated = { ...tender, responsible_id: empId };
      setTenders(prev => prev.map(t => t.id === tenderId ? updated : t));
      await addOrUpdateTender(updated);
  };

  const deleteTender = async (id: string) => {
    if (confirm('Удалить тендер из CRM навсегда?')) {
        setTenders(prev => prev.filter(t => t.id !== id));
        await deleteTenderFromBackend(id);
    }
  };

  const getResponsibleName = (id?: string) => {
      if (!id) return null;
      const emp = employees.find(e => e.id === id);
      return emp ? emp.name : null;
  };

  // Standard input class for consistent styling
  const inputClass = "w-full border border-slate-300 rounded-lg p-2.5 text-sm bg-white text-slate-900 placeholder-slate-400 focus:outline-none focus:ring-2 focus:ring-blue-500 shadow-sm";

  return (
    <div className="p-6 h-[calc(100vh-64px)] overflow-hidden flex flex-col relative">
      <div className="mb-6 flex justify-between items-center">
        <div>
          <h2 className="text-2xl font-bold text-slate-900">CRM</h2>
          <div className="flex items-center gap-2 text-sm mt-1">
             <span className="text-slate-500">Управление продажами</span>
             <span className="text-slate-300">|</span>
             <div className={`flex items-center gap-1.5 px-2 py-0.5 rounded-full text-xs font-medium border ${isServerOnline ? 'bg-emerald-50 text-emerald-700 border-emerald-200' : 'bg-amber-50 text-amber-700 border-amber-200'}`}>
                 {isServerOnline ? <Server size={10} /> : <HardDrive size={10} />}
                 {isServerOnline ? 'Источник: Сервер (БД)' : 'Источник: Локально (Демо)'}
             </div>
          </div>
        </div>
        <div className="flex gap-2">
            <button 
                onClick={() => setShowAddModal(true)}
                className="px-4 py-2 bg-white text-blue-600 border border-blue-200 rounded-lg text-sm font-medium hover:bg-blue-50 flex items-center gap-2"
            >
            <Plus size={18} />
            Добавить тендер
            </button>
            <button 
                onClick={() => navigate('/tenders')}
                className="px-4 py-2 bg-blue-600 text-white rounded-lg text-sm font-medium hover:bg-blue-700 flex items-center gap-2"
            >
            <Plus size={18} />
            Из Поиска
            </button>
        </div>
      </div>

      {loading ? (
          <div className="flex items-center justify-center flex-1">
              <Loader2 className="animate-spin text-blue-600" size={40} />
          </div>
      ) : (
        <div className="flex-1 flex gap-6 overflow-x-auto pb-4 custom-scrollbar">
            {COLUMNS.map((col) => {
            const colTenders = tenders.filter(t => t.status === col.id);
            const totalValue = colTenders.reduce((acc, curr) => acc + Number(curr.initial_price), 0);

            return (
                <div key={col.id} className="w-80 flex-shrink-0 flex flex-col bg-slate-50 rounded-xl border border-slate-200 max-h-full transition-all">
                <div className={`p-4 border-b border-slate-200 bg-white rounded-t-xl border-l-4 ${col.color}`}>
                    <div className="flex justify-between items-center mb-1">
                    <h3 className="font-bold text-slate-800">{col.title}</h3>
                    <span className="text-xs font-semibold bg-slate-100 text-slate-600 px-2 py-0.5 rounded-full">{colTenders.length}</span>
                    </div>
                    <p className="text-xs text-slate-400 font-medium">Сумма: ₽{(totalValue / 1000000).toFixed(1)}M</p>
                </div>

                <div className="p-3 overflow-y-auto space-y-3 flex-1 bg-slate-50/50">
                    {colTenders.map((tender) => (
                    <div key={tender.id} className="bg-white p-3 rounded-lg border border-slate-200 shadow-sm hover:shadow-md transition-all group relative">
                        <div className="flex justify-between items-start mb-2">
                            <div className="flex items-center gap-1.5">
                                <span className="text-[10px] font-bold text-blue-600 bg-blue-50 px-1 rounded">{tender.law_type || 'Коммерч.'}</span>
                            </div>
                            <button onClick={() => deleteTender(tender.id)} className="opacity-0 group-hover:opacity-100 text-slate-300 hover:text-red-500">
                                <Trash2 size={14} />
                            </button>
                        </div>
                        <h4 className="text-sm font-semibold text-slate-800 line-clamp-2 mb-3 cursor-pointer hover:text-blue-600">{tender.title}</h4>
                        
                        <div className="flex items-center justify-between text-xs mb-3">
                            <div className="flex items-center text-slate-700 font-medium gap-1">
                                <DollarSign size={12} className="text-slate-400"/>
                                <span>{(Number(tender.initial_price) / 1000).toLocaleString()}k</span>
                            </div>
                        </div>

                        {/* Responsible Person Selector */}
                        <div className="flex items-center justify-between pt-2 border-t border-slate-50 mt-2">
                            <div className="relative group/assign">
                                <div className={`flex items-center gap-1.5 px-2 py-1 rounded cursor-pointer ${tender.responsible_id ? 'bg-blue-50 text-blue-700' : 'bg-slate-100 text-slate-400 hover:bg-slate-200'}`}>
                                    <User size={12} />
                                    <span className="text-[10px] font-bold max-w-[80px] truncate">
                                        {getResponsibleName(tender.responsible_id) || 'Назначить'}
                                    </span>
                                </div>
                                
                                {/* Dropdown on Hover */}
                                <div className="hidden group-hover/assign:block absolute bottom-full left-0 mb-1 w-40 bg-white border border-slate-200 shadow-xl rounded-lg py-1 z-20">
                                    <div className="text-[10px] text-slate-400 px-2 py-1 font-bold uppercase">Назначить:</div>
                                    {employees.map(emp => (
                                        <button 
                                            key={emp.id}
                                            onClick={() => assignEmployee(tender.id, emp.id)}
                                            className="w-full text-left px-2 py-1.5 text-xs hover:bg-blue-50 text-slate-700 flex items-center gap-2"
                                        >
                                            <div className="w-4 h-4 rounded-full bg-blue-100 text-blue-600 flex items-center justify-center text-[8px] font-bold">
                                                {emp.name.substring(0,1)}
                                            </div>
                                            {emp.name}
                                        </button>
                                    ))}
                                    <button 
                                        onClick={() => assignEmployee(tender.id, '')}
                                        className="w-full text-left px-2 py-1.5 text-xs hover:bg-red-50 text-red-600 border-t border-slate-100"
                                    >
                                        Снять задачу
                                    </button>
                                </div>
                            </div>

                            <div className="flex gap-1 ml-auto">
                                {col.id !== 'Found' && <button onClick={() => moveStatus(tender.id, 'prev')} className="p-1 hover:bg-slate-100 rounded text-slate-400 hover:text-blue-600"><ArrowLeft size={14} /></button>}
                                {col.id !== 'Contract' && <button onClick={() => moveStatus(tender.id, 'next')} className="p-1 hover:bg-slate-100 rounded text-slate-400 hover:text-blue-600"><ArrowRight size={14} /></button>}
                            </div>
                        </div>
                    </div>
                    ))}
                </div>
                </div>
            );
            })}
        </div>
      )}

       {showAddModal && (
        <div className="fixed inset-0 bg-black/50 z-50 flex items-center justify-center backdrop-blur-sm p-4 animate-in fade-in duration-200">
            <div className="bg-white rounded-xl shadow-2xl w-full max-w-2xl overflow-hidden flex flex-col max-h-[90vh]">
                <div className="flex justify-between items-center p-4 border-b border-slate-100">
                    <h3 className="text-lg font-bold flex items-center gap-2 text-slate-800">
                        <Plus className="text-blue-600" size={20} />
                        Новый тендер
                    </h3>
                    <button onClick={() => setShowAddModal(false)} className="text-slate-400 hover:text-slate-600"><X size={20}/></button>
                </div>
                
                <div className="flex border-b border-slate-100">
                    <button 
                        onClick={() => setAddMode('link')}
                        className={`flex-1 py-3 text-sm font-medium flex items-center justify-center gap-2 transition-colors ${addMode === 'link' ? 'text-blue-600 border-b-2 border-blue-600 bg-blue-50/50' : 'text-slate-500 hover:bg-slate-50'}`}
                    >
                        <Globe size={16} /> По ссылке
                    </button>
                    <button 
                        onClick={() => setAddMode('file')}
                        className={`flex-1 py-3 text-sm font-medium flex items-center justify-center gap-2 transition-colors ${addMode === 'file' ? 'text-blue-600 border-b-2 border-blue-600 bg-blue-50/50' : 'text-slate-500 hover:bg-slate-50'}`}
                    >
                        <FileText size={16} /> Из файла
                    </button>
                </div>

                <div className="p-6 overflow-y-auto">
                    {/* PARSE SECTION */}
                    <div className="mb-6 bg-slate-50 p-4 rounded-xl border border-slate-200">
                        {addMode === 'link' ? (
                            <div className="flex gap-2">
                                <input 
                                    type="text" 
                                    placeholder="Вставьте ссылку на zakupki.gov.ru..." 
                                    className={inputClass}
                                    value={linkInput}
                                    onChange={e => setLinkInput(e.target.value)}
                                />
                                <button 
                                    onClick={handleParseLink}
                                    disabled={isAnalyzing || !linkInput}
                                    className="bg-blue-600 text-white px-4 py-2 rounded-lg text-sm font-medium hover:bg-blue-700 disabled:opacity-50 flex items-center gap-2 shadow-sm"
                                >
                                    {isAnalyzing ? <Loader2 className="animate-spin" size={16}/> : <Sparkles size={16}/>}
                                    Авто
                                </button>
                            </div>
                        ) : (
                            <div className="relative border-2 border-dashed border-slate-300 rounded-lg p-6 text-center hover:bg-white hover:border-blue-400 transition-colors bg-white">
                                <input 
                                    type="file" 
                                    className="absolute inset-0 opacity-0 cursor-pointer"
                                    onChange={handleFileUpload}
                                    accept=".pdf,.docx,.doc"
                                />
                                {isAnalyzing ? (
                                    <div className="flex flex-col items-center text-blue-600">
                                        <Loader2 className="animate-spin mb-2" size={24} />
                                        <span className="text-sm font-medium">Анализ документа...</span>
                                    </div>
                                ) : (
                                    <div className="flex flex-col items-center text-slate-500">
                                        <FileUp className="mb-2" size={24} />
                                        <span className="text-sm font-medium">Нажмите для загрузки файла</span>
                                        <span className="text-xs mt-1">ИИ извлечет данные автоматически</span>
                                    </div>
                                )}
                            </div>
                        )}
                        {isAnalyzing && <p className="text-xs text-blue-600 mt-2 text-center animate-pulse">{analysisStatus}</p>}
                    </div>

                    {/* FORM SECTION */}
                    <form onSubmit={handleSaveTender} className="space-y-4">
                        <div className="grid grid-cols-2 gap-4">
                            <div>
                                <label className="block text-xs font-bold text-slate-500 mb-1">Номер закупки (EIS)</label>
                                <input type="text" className={inputClass} value={manualForm.eis_number} onChange={e => setManualForm({...manualForm, eis_number: e.target.value})} />
                            </div>
                            <div>
                                <label className="block text-xs font-bold text-slate-500 mb-1">Сумма (НМЦК)</label>
                                <input type="number" className={inputClass} value={manualForm.price} onChange={e => setManualForm({...manualForm, price: e.target.value})} />
                            </div>
                        </div>

                        <div>
                            <label className="block text-xs font-bold text-slate-500 mb-1">Название закупки</label>
                            <input type="text" required className={inputClass} value={manualForm.title} onChange={e => setManualForm({...manualForm, title: e.target.value})} />
                        </div>

                        <div className="grid grid-cols-2 gap-4">
                            <div>
                                <label className="block text-xs font-bold text-slate-500 mb-1">Срок подачи (Дедлайн)</label>
                                <input type="text" className={inputClass} placeholder="дд.мм.гггг" value={manualForm.deadline} onChange={e => setManualForm({...manualForm, deadline: e.target.value})} />
                            </div>
                             <div>
                                <label className="block text-xs font-bold text-slate-500 mb-1">Ссылка</label>
                                <input type="text" className={`${inputClass} text-blue-600`} value={manualForm.url} onChange={e => setManualForm({...manualForm, url: e.target.value})} />
                            </div>
                        </div>

                        <div>
                            <label className="block text-xs font-bold text-slate-500 mb-1">Краткое описание / ТЗ</label>
                            <textarea className={`${inputClass} h-24 resize-none`} value={manualForm.description} onChange={e => setManualForm({...manualForm, description: e.target.value})} />
                        </div>

                        <div className="pt-4 border-t border-slate-100 flex justify-end gap-3">
                            <button type="button" onClick={() => setShowAddModal(false)} className="px-4 py-2 text-slate-600 hover:bg-slate-100 rounded-lg text-sm transition-colors">Отмена</button>
                            <button type="submit" className="px-6 py-2 bg-blue-600 text-white rounded-lg text-sm font-bold hover:bg-blue-700 shadow-md transition-colors">Сохранить</button>
                        </div>
                    </form>
                </div>
            </div>
        </div>
      )}
    </div>
  );
};

export default TenderCRM;