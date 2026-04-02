import React from 'react';
import { MemoryRouter as Router, Routes, Route, Navigate } from 'react-router-dom';
import Sidebar from './components/Sidebar';
import Header from './Header';
import Dashboard from './pages/Dashboard';
import ProductCatalog from './pages/ProductCatalog';
import TenderCRM from './pages/TenderCRM';
import Analysis from './pages/Analysis';
import TenderSearch from './pages/TenderSearch';
import ProductMatching from './pages/ProductMatching';
import ComplianceCheck from './pages/ComplianceCheck';
import Settings from './pages/Settings';
import Calendar from './pages/Calendar';

function App() {
  return (
    <Router>
      <div className="min-h-screen bg-slate-50 font-sans text-slate-900 flex">
        <Sidebar />
        <div className="flex-1 flex flex-col md:ml-64 transition-all duration-300">
          <Header />
          <main className="flex-1 overflow-auto">
            <Routes>
              <Route path="/" element={<Dashboard />} />
              <Route path="/catalog" element={<ProductCatalog />} />
              <Route path="/crm" element={<TenderCRM />} />
              <Route path="/matching" element={<ProductMatching />} />
              <Route path="/compliance" element={<ComplianceCheck />} />
              <Route path="/analysis" element={<Analysis />} />
              <Route path="/tenders" element={<TenderSearch />} />
              <Route path="/calendar" element={<Calendar />} />
              <Route path="/settings" element={<Settings />} />
              <Route path="*" element={<Navigate to="/" replace />} />
            </Routes>
          </main>
        </div>
      </div>
    </Router>
  );
}

export default App;