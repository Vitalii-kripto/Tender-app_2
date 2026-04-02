
export interface ProductCharacteristic {
  thickness_mm?: number;
  weight_kg_m2?: number;
  flexibility_temp_c?: number;
  tensile_strength_n?: number;
  [key: string]: string | number | undefined;
}

export interface Product {
  id: string;
  title: string;
  category: string;
  material_type: string;
  price: number;
  specs: ProductCharacteristic;
  similarity_score?: number; // Mocking the vector DB score
  url?: string;
  description?: string;
}

export type TenderStatus = 'Found' | 'Calculation' | 'Applied' | 'Auction' | 'Contract' | 'Lost';

export interface Tender {
  id: string;
  eis_number: string;
  title: string;
  description: string;
  initial_price: number | string;
  initial_price_text?: string;
  initial_price_value?: number;
  deadline: string;
  status: TenderStatus;
  risk_level: 'Low' | 'Medium' | 'High';
  region: string;
  law_type?: '44-ФЗ' | '223-ФЗ' | 'Коммерч.';
  url?: string;
  responsible_id?: string; // ID сотрудника
  docs_url?: string;
  search_url?: string;
  keyword?: string;
  seen?: boolean;
  ntype?: string;
}

export interface LegalRisk {
  document: string;
  requirement: string;
  deadline: string;
  risk_level: 'Low' | 'Medium' | 'High';
  description: string;
}

export type FileTechnicalStatus = 'file_not_read' | 'ocr_required' | 'unsupported_format' | 'empty_file' | 'extract_error' | 'ok';

export interface LegalAnalysisResult {
  id: string;
  status: 'success' | 'error' | 'partial';
  final_report_markdown?: string;
  error_message?: string;
  file_statuses: { filename: string, status: FileTechnicalStatus, message: string }[];
  docText?: string;
  showDoc?: boolean;
  stage?: string;
  progress?: number;
  selected_files_count?: number;
  structured_data?: any;
}

export type AnalysisStage = 'Подготовка документов' | 'Извлечение текста' | 'Классификация' | 'Анализ договора' | 'Анализ остальной документации' | 'Формирование отчета' | 'Готово' | 'Ошибка';

export interface AnalysisStageStatus {
  stage: AnalysisStage;
  status: 'pending' | 'loading' | 'done' | 'error';
}

export interface AnalysisResult {
  is_equivalent: boolean;
  confidence: number;
  recommended_product_id?: string;
  reasoning: string;
  critical_mismatches: string[];
}

export interface ComplianceFileStatus {
  fileName: string;
  status: 'valid' | 'invalid' | 'warning';
  comments: string[];
}

export interface ComplianceResult {
  missingDocuments: string[];
  checkedFiles: ComplianceFileStatus[];
  overallStatus: 'passed' | 'failed' | 'warning';
  summary: string;
}

export interface DashboardTask {
  id: string;
  title: string;
  time: string;
  type: 'urgent' | 'warning' | 'info';
}

export interface DashboardChartData {
  name: string;
  Тендеры: number;
  Выиграно: number;
}

export interface DashboardStats {
  active_tenders: number;
  margin_val: string;
  risks_count: number;
  contracts_count: number;
  chart_data: DashboardChartData[];
  tasks: DashboardTask[];
  is_demo?: boolean;
}

// --- NEW SETTINGS TYPES ---

export interface Employee {
  id: string;
  name: string;
  role: string; // 'admin' | 'manager' | 'analyst'
  email: string;
  avatar?: string;
}

export interface CompanyDocument {
  id: string;
  name: string;
  type: 'charter' | 'egrul' | 'finance' | 'license' | 'other';
  uploadDate: string;
  size: string;
}

export interface CompanyProfile {
  name: string;
  inn: string;
  kpp: string;
  ogrn: string;
  address: string;
  ceo: string;
  documents: CompanyDocument[];
}