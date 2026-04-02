import React, { useState, useEffect } from 'react';
import { Package, MapPin, Tag, Download, Play, Loader2, ServerOff, Globe, Layers, ChevronDown, ChevronRight, FileText } from 'lucide-react';
import { Product } from '../types';
import { getProductsFromBackend, runBackendParser } from '../services/geminiService';

// Экспортируемая переменная для доступа из других модулей (для обратной совместимости)
export const MOCK_CATALOG: Product[] = [];

const ProductCatalog = () => {
  const [catalog, setCatalog] = useState<Product[]>([]);
  const [isParsing, setIsParsing] = useState(false);
  const [statusMsg, setStatusMsg] = useState<string>("Загрузка...");
  const [expandedCategories, setExpandedCategories] = useState<Record<string, boolean>>({});

  // Группировка товаров по СТАРШИМ категориям (до первого слеша)
  const groupedProducts = catalog.reduce((acc, product) => {
      const fullCategory = product.category || 'Без категории';
      // Разделяем "Рулонные ... / Изопласт" и берем только "Рулонные ..."
      const seniorCategory = fullCategory.split(' / ')[0];
      
      if (!acc[seniorCategory]) {
          acc[seniorCategory] = [];
      }
      acc[seniorCategory].push(product);
      return acc;
  }, {} as Record<string, Product[]>);

  // Сортируем товары внутри каждой категории по алфавиту
  Object.values(groupedProducts).forEach((list: Product[]) => {
      list.sort((a, b) => a.title.localeCompare(b.title));
  });

  const categories = Object.keys(groupedProducts).sort();

  useEffect(() => {
    loadProducts();
  }, []);

  const loadProducts = async () => {
    setStatusMsg("Загрузка базы данных...");
    try {
        const data = await getProductsFromBackend();
        setCatalog(data);
        updateGlobalMock(data);
        setStatusMsg(`Всего товаров: ${data.length}`);
    } catch (e) {
        console.error(e);
        setStatusMsg("Ошибка загрузки");
    }
  };

  const updateGlobalMock = (data: Product[]) => {
      MOCK_CATALOG.length = 0;
      MOCK_CATALOG.push(...data);
  };

  const handleRunParser = async () => {
    setIsParsing(true);
    setStatusMsg("Запущен парсер gidroizol.ru...");
    
    try {
        const data = await runBackendParser();
        setCatalog(data);
        updateGlobalMock(data);
        setStatusMsg(`Парсинг завершен. Найдено: ${data.length} шт.`);
    } catch (err) {
        console.error(err);
        setStatusMsg("Ошибка парсинга");
    } finally {
        setIsParsing(false);
    }
  };

  const handleExportJSON = () => {
    const jsonString = JSON.stringify(catalog, null, 2);
    const blob = new Blob([jsonString], { type: 'application/json' });
    const href = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = href;
    link.download = "gidroizol_database.json";
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(href);
  };

  const toggleCategory = (category: string) => {
    setExpandedCategories(prev => ({
        ...prev,
        [category]: !prev[category]
    }));
  };

  // Helper to nicely format keys if needed, but primarily we just display what's there
  const formatSpecKey = (key: string) => {
    const map: Record<string, string> = {
      thickness_mm: 'Толщина (мм)',
      weight_kg_m2: 'Вес (кг/м²)',
      flexibility_temp_c: 'Гибкость (°C)',
      tensile_strength_n: 'Разрывная сила (Н)',
      material_type: 'Тип материала'
    };
    // Return mapped key or capitalized original key
    return map[key] || key.charAt(0).toUpperCase() + key.slice(1);
  };

  return (
    <div className="p-6 pb-20">
      <div className="mb-6 flex flex-col md:flex-row justify-between md:items-center gap-4">
        <div>
          <h2 className="text-2xl font-bold text-slate-900">Каталог продукции</h2>
          <div className="flex items-center gap-2 mt-1">
             <span className="text-slate-500 text-sm flex items-center gap-1"><Globe size={12}/> Источник: gidroizol.ru</span>
             <span className="text-slate-300">|</span>
             <span className="text-slate-500 text-sm">Статус: <strong>{statusMsg}</strong></span>
          </div>
        </div>
        <div className="flex gap-2">
           <button 
             onClick={handleExportJSON}
             className="flex items-center gap-2 px-4 py-2 bg-white border border-slate-300 rounded-lg text-slate-700 text-sm font-medium hover:bg-slate-50 transition-colors"
           >
            <Download size={16} />
            Экспорт
          </button>
          <button 
            onClick={handleRunParser}
            disabled={isParsing}
            className={`flex items-center gap-2 px-4 py-2 text-white rounded-lg text-sm font-medium transition-all ${
                isParsing ? 'bg-blue-400 cursor-not-allowed' : 'bg-blue-600 hover:bg-blue-700 shadow-md hover:shadow-lg'
            }`}
          >
            {isParsing ? <Loader2 size={16} className="animate-spin" /> : <Play size={16} />}
            {isParsing ? 'Парсинг...' : 'Обновить базу (Real/Demo)'}
          </button>
        </div>
      </div>

      {/* Render by Category */}
      {catalog.length === 0 ? (
          <div className="flex flex-col items-center justify-center h-64 text-slate-400">
              <Package size={48} className="mb-4 opacity-30" />
              <p>Каталог пуст. Нажмите "Обновить базу".</p>
          </div>
      ) : (
          <div className="space-y-4">
              {categories.map(category => (
                  <div key={category} className="bg-white rounded-xl border border-slate-200 shadow-sm overflow-hidden animate-in fade-in duration-300">
                      <button 
                        onClick={() => toggleCategory(category)}
                        className="w-full flex items-center justify-between p-4 bg-slate-50 hover:bg-blue-50/50 transition-colors"
                      >
                          <div className="flex items-center gap-3">
                              {expandedCategories[category] ? <ChevronDown size={20} className="text-blue-600"/> : <ChevronRight size={20} className="text-slate-400"/>}
                              <Layers className="text-blue-600" size={20} />
                              <h3 className="text-lg font-bold text-slate-800">{category}</h3>
                              <span className="bg-white border border-slate-200 text-slate-500 text-xs font-semibold px-2 py-0.5 rounded-full">
                                  {groupedProducts[category].length}
                              </span>
                          </div>
                      </button>
                      
                      {expandedCategories[category] && (
                          <div className="p-4 border-t border-slate-100 bg-white">
                              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                                {groupedProducts[category].map((product) => {
                                  // Извлекаем подкатегорию для отображения в карточке
                                  const catParts = (product.category || '').split(' / ');
                                  const subCategory = catParts.length > 1 ? catParts.slice(1).join(' / ') : '';

                                  return (
                                    <div key={product.id} className="bg-white rounded-xl border border-slate-200 shadow-sm overflow-hidden hover:shadow-md transition-shadow group animate-in fade-in duration-500 flex flex-col h-full">
                                      <div className="p-4 border-b border-slate-100 flex justify-between items-start bg-slate-50 group-hover:bg-blue-50/30 transition-colors">
                                        <div>
                                          <h3 className="font-bold text-slate-800 leading-tight line-clamp-2 text-sm" title={product.title}>{product.title}</h3>
                                          <div className="flex flex-wrap gap-1 mt-2">
                                              <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full bg-blue-100 text-blue-700 text-[10px] font-medium uppercase tracking-wide">
                                                <Tag size={10} />
                                                {product.material_type}
                                              </span>
                                              {subCategory && (
                                                  <span className="inline-flex items-center px-2 py-0.5 rounded-full bg-slate-100 text-slate-500 text-[10px] font-medium truncate max-w-[120px]">
                                                      {subCategory}
                                                  </span>
                                              )}
                                          </div>
                                        </div>
                                        <div className="text-right flex-shrink-0 ml-2">
                                          <p className="text-lg font-bold text-emerald-600 whitespace-nowrap">
                                              {product.price > 0 ? `₽${product.price}` : 'По запросу'}
                                          </p>
                                        </div>
                                      </div>
                                      
                                      <div className="p-4 space-y-3 flex-1 flex flex-col">
                                        <div className="text-sm mb-2">
                                          <h4 className="font-semibold text-slate-700 mb-2 text-xs uppercase tracking-wider">Характеристики</h4>
                                          <div className="grid grid-cols-1 sm:grid-cols-2 gap-y-2 gap-x-4">
                                            {product.specs && Object.entries(product.specs).map(([key, value]) => (
                                              <div key={key} className="flex justify-between items-start border-b border-slate-100 pb-1">
                                                <span className="text-[10px] text-slate-500 truncate pr-2 max-w-[100px]" title={formatSpecKey(key)}>{formatSpecKey(key)}</span>
                                                <span className="text-[11px] font-medium text-slate-900 text-right truncate max-w-[100px]" title={String(value)}>{String(value)}</span>
                                              </div>
                                            ))}
                                            {(!product.specs || Object.keys(product.specs).length === 0) && (
                                                <span className="text-xs text-slate-400 italic col-span-2">Нет данных</span>
                                            )}
                                          </div>
                                        </div>

                                        {/* Product Description Block */}
                                        {product.description && (
                                            <div className="mt-auto pt-3 border-t border-slate-100">
                                                <div className="flex items-center gap-1.5 mb-1.5 text-slate-400">
                                                    <FileText size={12} />
                                                    <span className="text-[10px] font-bold uppercase tracking-wider">Описание</span>
                                                </div>
                                                <p className="text-xs text-slate-500 leading-relaxed line-clamp-4 hover:line-clamp-none transition-all cursor-help" title="Нажмите, чтобы развернуть (если поддерживается)">
                                                    {product.description}
                                                </p>
                                            </div>
                                        )}
                                      </div>
                                      
                                      <div className="px-4 py-3 flex items-center justify-between border-t border-slate-50 mt-auto bg-slate-50/50">
                                          {product.url && (
                                              <a 
                                                  href={product.url} 
                                                  target="_blank" 
                                                  rel="noreferrer"
                                                  className="text-xs text-blue-600 hover:underline flex items-center gap-1"
                                              >
                                                  Открыть на сайте <Globe size={10}/>
                                              </a>
                                          )}
                                          <div className="flex items-center gap-1.5 text-xs text-slate-400 ml-auto">
                                            <MapPin size={14} />
                                            <span>РФ</span>
                                          </div>
                                      </div>
                                    </div>
                                  );
                                })}
                              </div>
                          </div>
                      )}
                  </div>
              ))}
          </div>
      )}
    </div>
  );
};

export default ProductCatalog;