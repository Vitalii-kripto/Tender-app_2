import React from 'react';
import { NavLink } from 'react-router-dom';
import { LayoutDashboard, FileText, ShoppingCart, Scale, Briefcase, Layers, FileCheck, Settings, Calendar } from 'lucide-react';

const Sidebar = () => {
  const navClasses = ({ isActive }: { isActive: boolean }) =>
    `flex items-center gap-3 px-4 py-3 rounded-lg transition-colors duration-200 ${
      isActive
        ? 'bg-blue-600 text-white shadow-md'
        : 'text-slate-400 hover:bg-slate-800 hover:text-white'
    }`;

  return (
    <div className="w-64 bg-slate-900 h-screen flex flex-col fixed left-0 top-0 border-r border-slate-800 z-20 hidden md:flex">
      <div className="p-6 border-b border-slate-800">
        <h1 className="text-xl font-bold text-white tracking-tight flex flex-col">
          <span className="text-blue-500">TenderSmart</span>
          <span className="text-xs font-normal text-slate-400">Gidroizol AI System</span>
        </h1>
      </div>

      <nav className="flex-1 p-4 space-y-2 overflow-y-auto">
        <NavLink to="/" className={navClasses}>
          <LayoutDashboard size={20} />
          <span>Дашборд</span>
        </NavLink>
        <NavLink to="/tenders" className={navClasses}>
          <FileText size={20} />
          <span>Поиск тендеров</span>
        </NavLink>
        <NavLink to="/crm" className={navClasses}>
          <Briefcase size={20} />
          <span>CRM</span>
        </NavLink>
        <NavLink to="/calendar" className={navClasses}>
          <Calendar size={20} />
          <span>Календарь</span>
        </NavLink>
        <NavLink to="/matching" className={navClasses}>
          <Layers size={20} />
          <span>Подбор аналогов</span>
        </NavLink>
        <NavLink to="/compliance" className={navClasses}>
          <FileCheck size={20} />
          <span>Проверка форм</span>
        </NavLink>
        <NavLink to="/analysis" className={navClasses}>
          <Scale size={20} />
          <span>ИИ Юрист</span>
        </NavLink>
        <NavLink to="/catalog" className={navClasses}>
          <ShoppingCart size={20} />
          <span>Каталог продукции</span>
        </NavLink>
        <NavLink to="/settings" className={navClasses}>
          <Settings size={20} />
          <span>Настройки</span>
        </NavLink>
      </nav>

      <div className="p-4 border-t border-slate-800">
        <div className="bg-slate-800 rounded-lg p-3">
          <p className="text-xs text-slate-400 mb-1">Статус системы</p>
          <div className="flex items-center gap-2">
            <span className="w-2 h-2 rounded-full bg-emerald-500 animate-pulse"></span>
            <span className="text-sm text-slate-200 font-medium">В сети</span>
          </div>
        </div>
      </div>
    </div>
  );
};

export default Sidebar;