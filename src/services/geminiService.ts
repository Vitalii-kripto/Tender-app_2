import { Product, AnalysisResult, Tender, DashboardStats, ComplianceResult, Employee, CompanyProfile, LegalAnalysisResult } from "../types";

// =========================================================================================
// КОНФИГУРАЦИЯ
// =========================================================================================

// Включаем принудительный демо-режим для деплоя без бэкенда.
// Если false - пытается подключиться к API (localhost:8000).
const IS_DEMO_MODE = false; 

export const API_BASE_URL = 'http://localhost:8000'; 

const LOCAL_STORAGE_KEY_CRM = 'TENDER_SMART_CRM_DATA';
const LOCAL_STORAGE_KEY_PRODUCTS = 'TENDER_SMART_PRODUCTS_DATA';
const LOCAL_STORAGE_KEY_EMPLOYEES = 'TENDER_SMART_EMPLOYEES';
const LOCAL_STORAGE_KEY_COMPANY = 'TENDER_SMART_COMPANY';

// --- HELPERS ---
const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

export const checkBackendHealth = async (): Promise<boolean> => {
    if (IS_DEMO_MODE) return false;
    try {
        const res = await fetch(`${API_BASE_URL}/`);
        return res.ok;
    } catch (e) {
        return false;
    }
};

// --- SETTINGS PERSISTENCE ---

export const getEmployees = (): Employee[] => {
    try {
        const stored = localStorage.getItem(LOCAL_STORAGE_KEY_EMPLOYEES);
        if (stored) return JSON.parse(stored);
    } catch (e) {
        console.error("Error reading employees", e);
    }
    const defaults: Employee[] = [
        { id: 'emp_1', name: 'Алексей Иванов', role: 'admin', email: 'alex@company.ru' },
        { id: 'emp_2', name: 'Мария Петрова', role: 'manager', email: 'maria@company.ru' },
        { id: 'emp_3', name: 'Иван Сидоров', role: 'analyst', email: 'ivan@company.ru' }
    ];
    localStorage.setItem(LOCAL_STORAGE_KEY_EMPLOYEES, JSON.stringify(defaults));
    return defaults;
};

export const saveEmployee = (employee: Employee) => {
    const current = getEmployees();
    const index = current.findIndex(e => e.id === employee.id);
    if (index >= 0) current[index] = employee;
    else current.push(employee);
    localStorage.setItem(LOCAL_STORAGE_KEY_EMPLOYEES, JSON.stringify(current));
};

export const deleteEmployee = (id: string) => {
    const current = getEmployees();
    const filtered = current.filter(e => e.id !== id);
    localStorage.setItem(LOCAL_STORAGE_KEY_EMPLOYEES, JSON.stringify(filtered));
};

export const getCompanyProfile = (): CompanyProfile => {
    try {
        const stored = localStorage.getItem(LOCAL_STORAGE_KEY_COMPANY);
        if (stored) return JSON.parse(stored);
    } catch (e) { console.error(e); }
    
    const defaults: CompanyProfile = {
        name: 'ООО "ГидроСтройКомплект"',
        inn: '7701234567',
        kpp: '770101001',
        ogrn: '1127746000000',
        address: 'г. Москва, ул. Строителей, д. 10',
        ceo: 'Иванов А.А.',
        documents: []
    };
    return defaults;
};

export const saveCompanyProfile = (profile: CompanyProfile) => {
    localStorage.setItem(LOCAL_STORAGE_KEY_COMPANY, JSON.stringify(profile));
};


// --- PRODUCTS PERSISTENCE ---

const getLocalProducts = (): Product[] => {
    try {
        const stored = localStorage.getItem(LOCAL_STORAGE_KEY_PRODUCTS);
        if (stored) return JSON.parse(stored);
    } catch (e) {
        console.error("Error reading local products", e);
    }
    return [
        {
            id: '1',
            title: 'Техноэласт ЭПП (Пример)',
            category: 'Битумно-полимерные',
            material_type: 'Рулонный',
            price: 340,
            specs: { thickness_mm: 4.0, weight_kg_m2: 4.95, flexibility_temp_c: -25, tensile_strength_n: 600 },
            description: "Техноэласт ЭПП – это СБС-модифицированный битумно-полимерный материал для устройства нижнего слоя кровельного ковра и гидроизоляции строительных конструкций."
        }
    ];
};

const saveLocalProducts = (products: Product[]) => {
    localStorage.setItem(LOCAL_STORAGE_KEY_PRODUCTS, JSON.stringify(products));
};

// --- CRM PERSISTENCE ---

const getLocalTenders = (): Tender[] => {
    try {
        const stored = localStorage.getItem(LOCAL_STORAGE_KEY_CRM);
        if (stored) return JSON.parse(stored);
    } catch (e) {
        console.error("Error reading local storage", e);
    }
    
    const defaults: Tender[] = [
        {
            id: 'crm_demo_1',
            eis_number: '0348100086023000012',
            title: 'Капитальный ремонт кровли школы №5 (ДЕМО CRM)',
            description: 'Полная замена кровельного пирога. Требуется:\n1. Техноэласт ЭПП - 2000м2\n2. Техноэласт ЭКП - 2000м2\n3. Праймер битумный - 500кг',
            initial_price: 3200000,
            deadline: '15.11.2024',
            status: 'Calculation',
            risk_level: 'Low',
            region: 'Москва',
            law_type: '44-ФЗ',
            responsible_id: 'emp_2'
        },
        {
            id: 'crm_demo_2',
            eis_number: '0123100004523000101',
            title: 'Поставка Техноэласт ЭПП (ДЕМО CRM)',
            description: 'Поставка на объект строительства ЖК "Новый Горизонт".',
            initial_price: 1850000,
            deadline: '20.11.2024',
            status: 'Applied',
            risk_level: 'Medium',
            region: 'Казань',
            law_type: '223-ФЗ',
            responsible_id: 'emp_1'
        }
    ];
    localStorage.setItem(LOCAL_STORAGE_KEY_CRM, JSON.stringify(defaults));
    return defaults;
};

const saveLocalTender = (tender: Tender) => {
    let current: Tender[] = [];
    try {
        const stored = localStorage.getItem(LOCAL_STORAGE_KEY_CRM);
        if (stored) {
            current = JSON.parse(stored);
        } else {
            current = getLocalTenders(); 
        }
    } catch(e) {
        current = getLocalTenders();
    }

    const index = current.findIndex(t => t.id === tender.id);
    if (index >= 0) {
        current[index] = tender;
    } else {
        current.push(tender);
    }
    localStorage.setItem(LOCAL_STORAGE_KEY_CRM, JSON.stringify(current));
};

const deleteLocalTender = (id: string) => {
    let current: Tender[] = [];
    try {
        const stored = localStorage.getItem(LOCAL_STORAGE_KEY_CRM);
        if (stored) current = JSON.parse(stored);
    } catch(e) {}
    
    const filtered = current.filter(t => t.id !== id);
    localStorage.setItem(LOCAL_STORAGE_KEY_CRM, JSON.stringify(filtered));
};

// --- API CLIENT ---

export const fetchDashboardStats = async (): Promise<DashboardStats> => {
    const demoStats: DashboardStats = {
        active_tenders: 124,
        margin_val: "₽14.2M",
        risks_count: 5,
        contracts_count: 12,
        chart_data: [
            { name: 'Пн', Тендеры: 12, Выиграно: 2 },
            { name: 'Вт', Тендеры: 19, Выиграно: 4 },
            { name: 'Ср', Тендеры: 15, Выиграно: 1 },
            { name: 'Чт', Тендеры: 22, Выиграно: 5 },
            { name: 'Пт', Тендеры: 28, Выиграно: 7 },
            { name: 'Сб', Тендеры: 10, Выиграно: 2 },
            { name: 'Вс', Тендеры: 5, Выиграно: 0 },
        ],
        tasks: [
            { id: '1', title: 'Подать заявку: Ремонт кровли МКД', time: 'Сегодня, 14:00', type: 'urgent' },
            { id: '2', title: 'Отправить КП: Технониколь', time: 'Завтра, 10:00', type: 'warning' },
            { id: '3', title: 'Проверить контракт №44-ФЗ', time: '12 окт', type: 'info' }
        ],
        is_demo: true
    };

    if (IS_DEMO_MODE) {
        await delay(500);
        return demoStats;
    }

    try {
        const response = await fetch(`${API_BASE_URL}/api/dashboard-stats`);
        if (!response.ok) throw new Error("Backend error");
        return await response.json();
    } catch (e) {
        return demoStats;
    }
};

// --- PRODUCT CATALOG ---

export const getProductsFromBackend = async (): Promise<Product[]> => {
    if (IS_DEMO_MODE) {
        return getLocalProducts();
    }
    try {
        const res = await fetch(`${API_BASE_URL}/api/products`);
        if (!res.ok) throw new Error("Backend error");
        const products = await res.json();
        if (products.length > 0) saveLocalProducts(products);
        return products;
    } catch (e) {
        return getLocalProducts();
    }
};

export const runBackendParser = async (): Promise<Product[]> => {
    const demoData: Product[] = [
        {
            id: 'demo_cat_1',
            title: 'Техноэласт ЭПП (ДЕМО)',
            category: 'Битумно-полимерные',
            material_type: 'Рулонный',
            price: 385,
            specs: { thickness_mm: 4.0, weight_kg_m2: 4.95, flexibility_temp_c: -25, tensile_strength_n: 600 },
            url: 'https://gidroizol.ru',
            description: "Техноэласт ЭПП – это СБС-модифицированный битумно-полимерный материал..."
        },
        {
            id: 'demo_cat_2',
            title: 'Унифлекс ТКП (ДЕМО)',
            category: 'Битумно-полимерные',
            material_type: 'Рулонный',
            price: 295,
            specs: { thickness_mm: 3.8, weight_kg_m2: 4.0, flexibility_temp_c: -20, tensile_strength_n: 500 },
            url: 'https://gidroizol.ru',
            description: "Унифлекс ТКП используется для устройства верхнего слоя кровельного ковра."
        },
        {
            id: 'demo_cat_3',
            title: 'Биполь ЭПП (ДЕМО)',
            category: 'Битумные',
            material_type: 'Рулонный',
            price: 220,
            specs: { thickness_mm: 3.0, weight_kg_m2: 3.5, flexibility_temp_c: -15, tensile_strength_n: 400 },
            url: 'https://gidroizol.ru',
            description: "Биполь ЭПП предназначен для устройства нижнего слоя кровельного ковра."
        }
    ];

    if (IS_DEMO_MODE) {
        await delay(1500);
        saveLocalProducts(demoData);
        return demoData;
    }

    try {
        const res = await fetch(`${API_BASE_URL}/api/parse-catalog`);
        if (!res.ok) throw new Error("Backend error");
        const products = await res.json();
        saveLocalProducts(products);
        return products;
    } catch (e) {
        saveLocalProducts(demoData);
        return demoData;
    }
};

// --- OTHER API CALLS ---

export const cancelSearch = async (): Promise<void> => {
    if (IS_DEMO_MODE) return;
    try {
        await fetch(`${API_BASE_URL}/api/search-tenders/cancel`, { method: 'POST' });
    } catch (error) {
        console.error("Failed to cancel search", error);
    }
};

export const processSelectedTenders = async (tenders: Tender[]): Promise<void> => {
    if (IS_DEMO_MODE) {
        await delay(1000);
        tenders.forEach(t => saveLocalTender(t));
        return;
    }
    try {
        const response = await fetch(`${API_BASE_URL}/api/search-tenders/process`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(tenders)
        });
        if (!response.ok) {
            const errorData = await response.json().catch(() => ({}));
            throw new Error(errorData.detail || `Server error: ${response.status}`);
        }
    } catch (error) {
        console.error("Failed to process tenders", error);
        throw error;
    }
};

export const searchTenders = async (
    query: string, 
    catalogContext: string, 
    isActiveOnly: boolean,
    fz44: boolean = true,
    fz223: boolean = true,
    publishDaysBack: number = 30,
    signal?: AbortSignal
): Promise<Tender[]> => {
    const demoTenders: Tender[] = [
        {
            id: 'demo_1',
            eis_number: '0373200041521000001',
            title: 'Капитальный ремонт кровли здания поликлиники №1 (ДЕМО)',
            description: 'Выполнение работ по капитальному ремонту мягкой кровли с использованием битумно-полимерных материалов в два слоя. Требуется Техноэласт ЭПП.',
            initial_price: 4500000,
            deadline: '25.12.2023',
            status: 'Found',
            risk_level: 'Medium',
            region: 'Москва',
            law_type: '44-ФЗ',
            url: 'https://zakupki.gov.ru'
        },
        {
            id: 'demo_2',
            eis_number: '0123200000321000055',
            title: 'Поставка гидроизоляционных материалов для нужд ГУП "Водоканал" (ДЕМО)',
            description: 'Требуется поставка: Техноэласт ЭПП или эквивалент. Объем: 5000 м2.',
            initial_price: 1250000,
            deadline: '28.12.2023',
            status: 'Found',
            risk_level: 'Low',
            region: 'Санкт-Петербург',
            law_type: '223-ФЗ',
            url: 'https://zakupki.gov.ru'
        }
    ];

    if (IS_DEMO_MODE) {
        await delay(1000);
        return demoTenders;
    }

    try {
        const params = new URLSearchParams({
            query,
            fz44: fz44.toString(),
            fz223: fz223.toString(),
            only_application_stage: isActiveOnly.toString(),
            publish_days_back: publishDaysBack.toString()
        });
        const response = await fetch(`${API_BASE_URL}/api/search-tenders?${params.toString()}`, {
            signal
        });
        if (!response.ok) {
            const errorData = await response.json().catch(() => ({}));
            throw new Error(errorData.detail || `Server error: ${response.status}`);
        }
        return await response.json();
    } catch (error: any) {
        if (error.name === 'AbortError') {
            console.log('Search aborted');
            return [];
        }
        console.error('Search error:', error);
        throw error;
    }
};

export const skipTender = async (tender: Tender): Promise<void> => {
    if (IS_DEMO_MODE) return;
    try {
        await fetch(`${API_BASE_URL}/api/search-tenders/skip`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(tender)
        });
    } catch (error) {
        console.error("Error skipping tender", error);
    }
};

export const fetchTenderDocsText = async (tenderUrl: string, eisNumber: string): Promise<string> => {
    // В реальной системе нужно сделать эндпоинт, который качает PDF
    // Пока возвращаем заглушку, так как функционал скачивания сложен
    await delay(1500);
    return `[ДЕМО РЕЖИМ] Текст документации для закупки №${eisNumber}.
    
    ТЕХНИЧЕСКОЕ ЗАДАНИЕ
    1. Наименование товара: Гидроизоляционный материал рулонный наплавляемый битумно-полимерный.
    2. Характеристики:
       - Основа: полиэфир
       - Вес 1 м2: не менее 5.0 кг
       - Гибкость на брусе R=25мм: не выше -25 С
    3. Требования к гарантии: 5 лет.`;
};

// --- AI CALLS (VIA BACKEND) ---

export const startBatchAnalysisJob = async (tenderIds: string[], selectedFiles?: Record<string, string[]>): Promise<string> => {
    try {
        const response = await fetch(`${API_BASE_URL}/api/ai/analyze-tenders-batch`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ 
                tender_ids: tenderIds,
                selected_files: selectedFiles || {}
            })
        });
        if(!response.ok) throw new Error("Backend error");
        const data = await response.json();
        return data.job_id;
    } catch (e) {
        console.error("Error starting batch analysis job:", e);
        throw e;
    }
};

export const getJobStatus = async (jobId: string): Promise<any> => {
    try {
        const response = await fetch(`${API_BASE_URL}/api/ai/jobs/${jobId}`);
        if(!response.ok) throw new Error("Backend error");
        return await response.json();
    } catch (e) {
        console.error("Error getting job status:", e);
        throw e;
    }
};

export const analyzeTendersBatch = async (tenderIds: string[], selectedFiles?: Record<string, string[]>): Promise<LegalAnalysisResult[]> => {
    try {
        const jobId = await startBatchAnalysisJob(tenderIds, selectedFiles);
        return new Promise((resolve, reject) => {
            const interval = setInterval(async () => {
                try {
                    const job = await getJobStatus(jobId);
                    if (job.status === 'completed') {
                        clearInterval(interval);
                        const results: LegalAnalysisResult[] = [];
                        for (const tid in job.tenders) {
                            results.push({
                                id: tid,
                                ...job.tenders[tid]
                            });
                        }
                        resolve(results);
                    }
                } catch (e) {
                    clearInterval(interval);
                    reject(e);
                }
            }, 3000);
        });
    } catch (e) {
        console.error("Error analyzing tenders batch:", e);
        return tenderIds.map(id => ({
            id,
            status: "error",
            file_statuses: []
        }));
    }
};



export const findProductEquivalent = async (tenderSpecs: string): Promise<any> => {
    const demoMatch = {
             is_equivalent: true,
             confidence: 0.95,
             recommended_product_id: '1',
             reasoning: "Материал 'Техноэласт ЭПП' полностью соответствует заявленным требованиям ТЗ: полиэфирная основа, толщина ~4мм, гибкость -25С. Это прямой аналог.",
             critical_mismatches: [],
             all_matches: [
                 {
                     id: '1',
                     title: 'Техноэласт ЭПП',
                     similarity_score: 95,
                     match_reason: "Полное соответствие по основе (полиэфир), толщине (4мм) и гибкости на брусе (-25С).",
                     price: 250,
                     specs: {
                         "Основа": "Полиэфир",
                         "Толщина, мм": 4.0,
                         "Гибкость на брусе, °C": -25
                     }
                 },
                 {
                     id: '2',
                     title: 'Унифлекс ЭПП',
                     similarity_score: 82,
                     match_reason: "Подходит по основе (полиэфир), но имеет меньшую толщину (3.8мм) и гибкость (-20С).",
                     price: 180,
                     specs: {
                         "Основа": "Полиэфир",
                         "Толщина, мм": 3.8,
                         "Гибкость на брусе, °C": -20
                     }
                 }
             ]
    };

    if (IS_DEMO_MODE) {
        await delay(1500);
        return demoMatch;
    }

    try {
        const response = await fetch(`${API_BASE_URL}/api/ai/match-product`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ specs: tenderSpecs, mode: 'database' })
        });
        if(!response.ok) throw new Error("Backend error");
        const data = await response.json();
        
        if (data.matches && data.matches.length > 0) {
             const best = data.matches[0];
             return {
                 is_equivalent: best.similarity_score > 80,
                 confidence: best.similarity_score / 100,
                 recommended_product_id: best.id,
                 reasoning: best.match_reason,
                 critical_mismatches: [],
                 all_matches: data.matches
             };
        }
        return { is_equivalent: false, confidence: 0, reasoning: "Ничего не найдено", critical_mismatches: [], all_matches: [] };

    } catch (e) {
        return demoMatch;
    }
};

export const searchProductsInternet = async (tenderSpecs: string): Promise<any> => {
    const demoResult = { 
        text: `[РЕЖИМ ДЕМО] Backend недоступен.\n\nИмитация ответа:\n1. Изопласт К-ЭПП-4.0 (~280 руб/м2)\n2. Филизол Супер ЭПП (~310 руб/м2)` 
    };

    if (IS_DEMO_MODE) {
        await delay(2000);
        return demoResult;
    }

    try {
        const response = await fetch(`${API_BASE_URL}/api/ai/match-product`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ specs: tenderSpecs, mode: 'internet' })
        });
        if(!response.ok) throw new Error("Backend error");
        return await response.json(); 
    } catch (e) {
        return demoResult;
    }
};

export const validateProductCompliance = async (docText: string): Promise<any> => {
    if (IS_DEMO_MODE) {
        await delay(1500);
        return { is_compliant: false, issues: ["[ДЕМО] Ошибка проверки"], score: 0 };
    }
     try {
        const response = await fetch(`${API_BASE_URL}/api/ai/validate-compliance`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ requirements: docText, proposal: "[]" })
        });
        if(!response.ok) throw new Error("Backend error");
        return await response.json();
    } catch (e) {
        return { is_compliant: false, issues: ["[ДЕМО] Ошибка проверки"], score: 0 };
    }
};

// --- NEW METHODS FOR COMPLEX VALIDATION ---

export const extractProductsFromText = async (text: string): Promise<any[]> => {
    const demoProducts = [
        { name: "Техноэласт ЭПП", quantity: "2000 м2", specs: "Толщина 4.0мм, Полиэфир, Гибкость -25С" },
        { name: "Праймер битумный №01", quantity: "500 кг", specs: "Ведро 20л, Расход 0.3 кг/м2" }
    ];

    if (IS_DEMO_MODE) {
        await delay(1500);
        return demoProducts;
    }

    try {
        const res = await fetch(`${API_BASE_URL}/api/ai/extract-products`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ text })
        });
        if(!res.ok) throw new Error("Backend error");
        return await res.json();
    } catch (e) {
        return demoProducts;
    }
};

export const enrichProductSpecs = async (productName: string): Promise<string> => {
    if (IS_DEMO_MODE) {
        await delay(1000);
        return "[ДЕМО] Основа: Полиэфир, Толщина: 4.0мм, Вес: 4.95кг/м2, Гибкость: -25С.";
    }
    try {
        const res = await fetch(`${API_BASE_URL}/api/ai/enrich-specs`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ product_name: productName })
        });
        if (!res.ok) throw new Error("Backend error");
        const data = await res.json();
        return data.specs;
    } catch (e) {
        return "[ДЕМО] Backend недоступен. Невозможно выполнить поиск в Google.";
    }
};

export const validateComplexCompliance = async (requirements: string, proposalItems: any[]): Promise<any> => {
    const demoCompliance = {
        score: 45,
        summary: "[ДЕМО] Предложение не полностью соответствует требованиям ТЗ.",
        items: [
            {
                requirement_name: "Гидроизоляция (Полиэфир, -25С)",
                proposal_name: "Техноэласт ЭПП",
                real_specs_found: "В интернете: Техноэласт ЭПП имеет гибкость -25С.",
                status: "OK",
                comment: "Характеристики соответствуют."
            },
            {
                requirement_name: "Мастика битумная (расход 1кг/м2)",
                proposal_name: "Мастика №21",
                real_specs_found: "В интернете: Расход 1.2кг/м2",
                status: "FAIL",
                comment: "Расход превышает требуемый по ТЗ."
            },
            {
                requirement_name: "Праймер битумный",
                proposal_name: "",
                real_specs_found: "",
                status: "MISSING",
                comment: "Позиция отсутствует в предложении."
            }
        ]
    };

    if (IS_DEMO_MODE) {
        await delay(2000);
        return demoCompliance;
    }

    try {
        const res = await fetch(`${API_BASE_URL}/api/ai/validate-compliance`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ 
                requirements: requirements,
                proposal: JSON.stringify(proposalItems)
            })
        });
        if(!res.ok) throw new Error("Backend error");
        return await res.json();
    } catch (e) {
        return demoCompliance;
    }
};


export const checkTenderCompliance = async (title: string, description: string, fileNames: string[]): Promise<ComplianceResult> => {
    const demoCompliance: ComplianceResult = {
        overallStatus: 'warning',
        summary: '[ДЕМО] Обнаружены пропуски в комплекте документов.',
        missingDocuments: ['Декларация о соответствии (СТ-1)', 'Решение об одобрении крупной сделки'],
        checkedFiles: fileNames.map(name => ({
            fileName: name,
            status: name.toLowerCase().includes('form2') || name.includes('форма') ? 'valid' : 'warning',
            comments: name.toLowerCase().includes('form2') ? ['Форма заполнена корректно'] : ['Требуется проверка']
        }))
    };

    if (IS_DEMO_MODE) {
        await delay(2000);
        return demoCompliance;
    }

    try {
        const response = await fetch(`${API_BASE_URL}/api/ai/check-compliance`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ title, description, filenames: fileNames })
        });
        if(!response.ok) throw new Error("Backend error");
        return await response.json();
    } catch (e) {
        return demoCompliance;
    }
};

// --- NEW HELPER FUNCTIONS FOR MANUAL ADD ---

export const uploadTenderFile = async (file: File): Promise<{text: string, path: string}> => {
    if (IS_DEMO_MODE) {
        await delay(1000);
        return { 
            text: `[DEMO TEXT FROM FILE: ${file.name}]\n\nТендерная документация.\nОбъект: Школа №5.\nТребуется ремонт кровли.`, 
            path: "fake_path/doc.pdf" 
        };
    }
    try {
        const formData = new FormData();
        formData.append('file', file);
        const res = await fetch(`${API_BASE_URL}/api/tenders/upload`, {
            method: 'POST',
            body: formData
        });
        if(!res.ok) throw new Error("Upload failed");
        return await res.json();
    } catch(e) {
        console.error(e);
        return { text: "Ошибка загрузки или распознавания файла. Введите данные вручную.", path: "" };
    }
};

export const extractDetailsFromText = async (text: string): Promise<any> => {
    if (IS_DEMO_MODE) {
        await delay(1500);
        return {
            eis_number: '1234567890123456789',
            title: 'Закупка из загруженного файла (Демо)',
            initial_price: 5000000,
            deadline: '31.12.2024',
            description: 'Автоматически извлеченное описание из файла (Демо режим).'
        };
    }
    try {
        const res = await fetch(`${API_BASE_URL}/api/ai/extract-details`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ text })
        });
        if(!res.ok) throw new Error("AI Extraction failed");
        return await res.json();
    } catch(e) {
        console.error(e);
        return {};
    }
};


// --- CRM SYNC ---

export const getTendersFromBackend = async (): Promise<Tender[]> => {
    if (IS_DEMO_MODE) {
        return getLocalTenders();
    }
    try {
        const res = await fetch(`${API_BASE_URL}/api/crm/tenders`);
        if(!res.ok) throw new Error("Backend error");
        const data = await res.json();
        localStorage.setItem(LOCAL_STORAGE_KEY_CRM, JSON.stringify(data));
        return data;
    } catch (e) {
        return getLocalTenders();
    }
};

export const addOrUpdateTender = async (tender: Tender) => {
    saveLocalTender(tender);
    if (!IS_DEMO_MODE) {
        try {
            await fetch(`${API_BASE_URL}/api/crm/tenders`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(tender)
            });
        } catch(e) {
            console.warn("Backend sync failed");
        }
    }
};

export const deleteTenderFromBackend = async (id: string) => {
    deleteLocalTender(id);
    if (!IS_DEMO_MODE) {
        try {
            await fetch(`${API_BASE_URL}/api/crm/tenders/${id}`, {
                method: 'DELETE'
            });
        } catch(e) {
            console.warn("Backend sync failed");
        }
    }
};