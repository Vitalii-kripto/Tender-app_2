import React, { useState, useEffect } from 'react';
import { FileCheck, Upload, AlertCircle, CheckCircle, XCircle, FileText, Loader2, Info, FileInput } from 'lucide-react';
import { Tender, ComplianceResult } from '../types';
import { checkTenderCompliance, getTendersFromBackend } from '../services/geminiService';

const ComplianceCheck = () => {
  // State
  const [availableTenders, setAvailableTenders] = useState<Tender[]>([]);
  const [selectedTenderId, setSelectedTenderId] = useState<string>('');
  const [uploadedFiles, setUploadedFiles] = useState<File[]>([]);
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<ComplianceResult | null>(null);

  // Load tenders from Unified Service
  useEffect(() => {
    const loadData = async () => {
        try {
            const tenders = await getTendersFromBackend();
            setAvailableTenders(tenders);
            if (tenders.length > 0) setSelectedTenderId(tenders[0].id);
        } catch (e) {
            console.error(e);
        }
    };
    loadData();
  }, []);

  const handleFileUpload = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files) {
      const filesArr = Array.from(e.target.files);
      setUploadedFiles(prev => [...prev, ...filesArr]);
      // Reset result when new files are added to force re-check
      setResult(null); 
    }
  };

  const removeFile = (idx: number) => {
    setUploadedFiles(prev => prev.filter((_, i) => i !== idx));
  };

  const runCheck = async () => {
    const tender = availableTenders.find(t => t.id === selectedTenderId);
    if (!tender) return;

    setLoading(true);
    try {
        const fileNames = uploadedFiles.map(f => f.name);
        const res = await checkTenderCompliance(tender.title, tender.description, fileNames);
        setResult(res);
    } catch (e) {
        console.error(e);
        alert("Ошибка при проверке. Проверьте консоль.");
    } finally {
        setLoading(false);
    }
  };

  return (
    <div className="p-6 max-w-6xl mx-auto pb-20">
      <div className="mb-8">
        <h2 className="text-2xl font-bold text-slate-900 flex items-center gap-2">
          <FileCheck className="text-blue-600" />
          Валидация форм заявки
        </h2>
        <p className="text-slate-500 text-sm">
          Система проверяет наличие обязательных документов и корректность заполнения форм (Форма 2, Анкета, Декларации).
        </p>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-12 gap-6">
        
        {/* LEFT COLUMN: Controls & Upload */}
        <div className="lg:col-span-4 space-y-6">
            
            {/* Tender Selector */}
            <div className="bg-white p-5 rounded-xl border border-slate-200 shadow-sm">
                <label className="block text-sm font-bold text-slate-700 mb-2">Выберите тендер из CRM</label>
                {availableTenders.length === 0 ? (
                    <div className="text-center p-4 bg-slate-50 rounded-lg text-sm text-slate-500">
                        Нет тендеров в работе. Добавьте их через Поиск или CRM.
                    </div>
                ) : (
                    <select 
                        className="w-full border border-slate-300 rounded-lg p-2.5 text-sm focus:ring-2 focus:ring-blue-500 outline-none bg-slate-50"
                        value={selectedTenderId}
                        onChange={(e) => setSelectedTenderId(e.target.value)}
                    >
                        {availableTenders.map(t => (
                            <option key={t.id} value={t.id}>{t.eis_number} - {t.title.substring(0, 40)}...</option>
                        ))}
                    </select>
                )}
            </div>

            {/* File Upload Area */}
            <div className="bg-white p-5 rounded-xl border border-slate-200 shadow-sm h-full max-h-[500px] flex flex-col">
                <h3 className="font-bold text-slate-800 mb-3">Пакет документов для проверки</h3>
                
                <div className="border-2 border-dashed border-slate-300 rounded-lg p-6 text-center hover:bg-slate-50 transition-colors relative mb-4">
                    <input type="file" multiple className="absolute inset-0 opacity-0 cursor-pointer" onChange={handleFileUpload} />
                    <Upload className="mx-auto text-slate-400 mb-2" />
                    <p className="text-sm font-medium text-slate-600">Загрузить заполненные формы</p>
                    <p className="text-xs text-slate-400 mt-1">PDF, DOCX, XLSX</p>
                </div>

                <div className="flex-1 overflow-y-auto space-y-2 mb-4 custom-scrollbar">
                    {uploadedFiles.length === 0 && (
                        <p className="text-center text-xs text-slate-400 py-4">Файлы не выбраны</p>
                    )}
                    {uploadedFiles.map((file, idx) => (
                        <div key={idx} className="flex items-center justify-between p-2 bg-slate-50 border border-slate-100 rounded text-sm">
                            <div className="flex items-center gap-2 truncate">
                                <FileInput size={14} className="text-blue-500 flex-shrink-0"/>
                                <span className="truncate max-w-[180px]" title={file.name}>{file.name}</span>
                            </div>
                            <button onClick={() => removeFile(idx)} className="text-slate-400 hover:text-red-500">
                                <XCircle size={16} />
                            </button>
                        </div>
                    ))}
                </div>

                <button 
                    onClick={runCheck}
                    disabled={loading || uploadedFiles.length === 0 || !selectedTenderId}
                    className="w-full bg-blue-600 text-white py-3 rounded-xl font-bold hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed flex justify-center items-center gap-2 shadow-md transition-all"
                >
                    {loading ? <Loader2 className="animate-spin" /> : <FileCheck size={20} />}
                    {loading ? 'ИИ Проверка...' : 'Проверить формы'}
                </button>
            </div>
        </div>

        {/* RIGHT COLUMN: Results */}
        <div className="lg:col-span-8">
            <div className="bg-white rounded-xl border border-slate-200 shadow-sm min-h-[600px] p-6">
                <h3 className="font-bold text-slate-800 border-b border-slate-100 pb-3 mb-4">Результат проверки заявки</h3>

                {!result && !loading && (
                    <div className="h-96 flex flex-col items-center justify-center text-slate-300">
                        <FileCheck size={64} className="mb-4 opacity-50" />
                        <p>Загрузите файлы слева для анализа</p>
                    </div>
                )}

                {loading && (
                    <div className="h-96 flex flex-col items-center justify-center text-slate-400">
                        <Loader2 size={48} className="animate-spin text-blue-500 mb-4" />
                        <p className="font-medium text-slate-600">ИИ "читает" заполненные формы...</p>
                        <p className="text-sm">Сверка сумм, дат и подписей</p>
                    </div>
                )}

                {result && !loading && (
                    <div className="space-y-6 animate-in fade-in slide-in-from-bottom-4">
                        
                        {/* Summary Status */}
                        <div className={`p-4 rounded-lg border flex items-start gap-3 ${
                            result.overallStatus === 'passed' ? 'bg-emerald-50 border-emerald-200 text-emerald-800' :
                            result.overallStatus === 'failed' ? 'bg-red-50 border-red-200 text-red-800' :
                            'bg-amber-50 border-amber-200 text-amber-800'
                        }`}>
                            {result.overallStatus === 'passed' ? <CheckCircle className="flex-shrink-0 mt-0.5" /> : <AlertCircle className="flex-shrink-0 mt-0.5" />}
                            <div>
                                <h4 className="font-bold text-lg">
                                    {result.overallStatus === 'passed' ? 'Формы заполнены корректно' : 
                                     result.overallStatus === 'failed' ? 'Критические ошибки в заявке' : 'Требуется внимание'}
                                </h4>
                                <p className="text-sm mt-1 opacity-90">{result.summary}</p>
                            </div>
                        </div>

                        {/* Missing Documents Section */}
                        {result.missingDocuments.length > 0 && (
                            <div className="bg-white border border-red-100 rounded-xl overflow-hidden shadow-sm">
                                <div className="bg-red-50 px-4 py-2 border-b border-red-100 flex items-center gap-2">
                                    <XCircle size={16} className="text-red-600" />
                                    <h4 className="font-bold text-red-800 text-sm uppercase">Не хватает обязательных форм</h4>
                                </div>
                                <div className="p-4 bg-red-50/30">
                                    <ul className="space-y-2">
                                        {result.missingDocuments.map((doc, i) => (
                                            <li key={i} className="flex items-center gap-2 text-red-700 text-sm font-medium">
                                                <span className="w-1.5 h-1.5 rounded-full bg-red-500"></span>
                                                {doc}
                                            </li>
                                        ))}
                                    </ul>
                                </div>
                            </div>
                        )}

                        {/* Checked Files List */}
                        <div>
                             <h4 className="font-bold text-slate-700 mb-3 flex items-center gap-2">
                                 <FileText size={18} />
                                 Детализация по файлам
                             </h4>
                             <div className="space-y-3">
                                 {result.checkedFiles.map((file, i) => (
                                     <div key={i} className="border border-slate-200 rounded-lg p-4 hover:bg-slate-50 transition-colors">
                                         <div className="flex justify-between items-start mb-2">
                                             <div className="flex items-center gap-2 font-medium text-slate-800">
                                                 <FileInput size={16} className="text-slate-400" />
                                                 {file.fileName}
                                             </div>
                                             <span className={`px-2 py-0.5 rounded text-xs font-bold uppercase ${
                                                 file.status === 'valid' ? 'bg-emerald-100 text-emerald-700' :
                                                 file.status === 'invalid' ? 'bg-red-100 text-red-700' :
                                                 'bg-amber-100 text-amber-700'
                                             }`}>
                                                 {file.status === 'valid' ? 'OK' : 
                                                  file.status === 'invalid' ? 'Ошибка' : 'Проверка'}
                                             </span>
                                         </div>
                                         
                                         {file.comments.length > 0 && (
                                             <div className="mt-2 text-sm space-y-1 bg-white p-2 rounded border border-slate-100">
                                                 {file.comments.map((comment, cIdx) => (
                                                     <p key={cIdx} className={`flex items-start gap-2 ${
                                                         file.status === 'valid' ? 'text-emerald-600' : 
                                                         file.status === 'invalid' ? 'text-red-600' : 'text-amber-600'
                                                     }`}>
                                                         <span className="mt-1.5 w-1 h-1 rounded-full bg-current opacity-60"></span>
                                                         {comment}
                                                     </p>
                                                 ))}
                                             </div>
                                         )}
                                     </div>
                                 ))}
                             </div>
                        </div>

                    </div>
                )}
            </div>
        </div>
      </div>
    </div>
  );
};

export default ComplianceCheck;