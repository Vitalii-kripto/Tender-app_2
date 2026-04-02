import React, { useState, useEffect } from 'react';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend } from 'recharts';
import { TrendingUp, AlertTriangle, FileCheck, DollarSign, RefreshCw, ServerOff, Wifi } from 'lucide-react';
import { fetchDashboardStats } from '../services/geminiService';
import { DashboardStats } from '../types';

import { useNavigate } from 'react-router-dom';

const StatCard = ({ title, value, subtext, icon: Icon, color }: any) => (
  <div className="bg-white p-6 rounded-xl border border-slate-200 shadow-sm flex items-start justify-between">
    <div>
      <p className="text-sm font-medium text-slate-500 mb-1">{title}</p>
      <h3 className="text-2xl font-bold text-slate-800">{value}</h3>
      <p className={`text-xs mt-2 ${subtext.includes('+') ? 'text-emerald-600' : 'text-slate-400'}`}>
        {subtext}
      </p>
    </div>
    <div className={`p-3 rounded-lg ${color}`}>
      <Icon size={24} className="text-white" />
    </div>
  </div>
);

const Dashboard = () => {
  const navigate = useNavigate();
  const [stats, setStats] = useState<DashboardStats | null>(null);
  const [loading, setLoading] = useState(true);

  const loadData = async () => {
    setLoading(true);
    try {
        const data = await fetchDashboardStats();
        setStats(data);
    } catch (e) {
        console.error(e);
    } finally {
        setLoading(false);
    }
  };

  useEffect(() => {
    loadData();
  }, []);

  if (!stats) return null;

  return (
    <div className="p-6 space-y-6">
      <div className="flex flex-col sm:flex-row justify-between items-start sm:items-center gap-4">
        <div>
          <h2 className="text-2xl font-bold text-slate-900">Дашборд</h2>
          <p className="text-slate-500 text-sm">Добро пожаловать! Обзор активности по тендерам.</p>
        </div>
        <button 
            onClick={loadData}
            disabled={loading}
            className="flex items-center gap-2 bg-blue-600 text-white px-4 py-2 rounded-lg font-medium text-sm hover:bg-blue-700 transition-colors shadow-sm disabled:opacity-70"
        >
          <RefreshCw size={16} className={loading ? 'animate-spin' : ''} />
          {loading ? 'Обновление...' : 'Обновить данные'}
        </button>
      </div>

      {/* Demo Mode Alert Banner */}
      {stats.is_demo && (
         <div className="bg-amber-50 border border-amber-200 rounded-xl p-4 flex items-start gap-3 animate-in fade-in slide-in-from-top-2">
            <ServerOff className="text-amber-600 flex-shrink-0 mt-0.5" size={20} />
            <div>
               <h3 className="font-bold text-amber-800 text-sm">Режим ДЕМО: Backend недоступен</h3>
               <p className="text-xs text-amber-700 mt-1">
                 Не удалось подключиться к серверу Python (127.0.0.1:8000). Показаны демонстрационные данные. 
                 Для отображения реальной статистики запустите <code>python backend.py</code>.
               </p>
            </div>
         </div>
      )}

      {!stats.is_demo && (
         <div className="bg-emerald-50 border border-emerald-100 rounded-xl p-3 flex items-center gap-2 animate-in fade-in">
             <div className="bg-emerald-100 p-1 rounded-full"><Wifi size={14} className="text-emerald-600"/></div>
             <span className="text-xs text-emerald-700 font-medium">Система работает в штатном режиме. Данные обновлены.</span>
         </div>
      )}

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        <StatCard 
          title="Активные тендеры" 
          value={stats.active_tenders} 
          subtext="+5 новых сегодня" 
          icon={TrendingUp} 
          color="bg-blue-500" 
        />
        <StatCard 
          title="Потенциальная маржа" 
          value={stats.margin_val} 
          subtext="+8% к прошлой неделе" 
          icon={DollarSign} 
          color="bg-emerald-500" 
        />
        <StatCard 
          title="Критические риски" 
          value={stats.risks_count} 
          subtext="Требует проверки юристом" 
          icon={AlertTriangle} 
          color="bg-amber-500" 
        />
        <StatCard 
          title="Подписано контрактов" 
          value={stats.contracts_count} 
          subtext="В этом месяце" 
          icon={FileCheck} 
          color="bg-indigo-500" 
        />
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        <div className="lg:col-span-2 bg-white p-6 rounded-xl border border-slate-200 shadow-sm">
          <h3 className="text-lg font-bold text-slate-800 mb-6">Активность</h3>
          <div className="h-80 w-full">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={stats.chart_data} margin={{ top: 10, right: 30, left: 0, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#e2e8f0" />
                <XAxis dataKey="name" axisLine={false} tickLine={false} tick={{fill: '#64748b'}} />
                <YAxis axisLine={false} tickLine={false} tick={{fill: '#64748b'}} />
                <Tooltip 
                  contentStyle={{ backgroundColor: '#fff', borderRadius: '8px', border: '1px solid #e2e8f0', boxShadow: '0 4px 6px -1px rgb(0 0 0 / 0.1)' }}
                  cursor={{fill: '#f1f5f9'}}
                />
                <Legend />
                <Bar dataKey="Тендеры" fill="#3b82f6" radius={[4, 4, 0, 0]} barSize={32} />
                <Bar dataKey="Выиграно" fill="#10b981" radius={[4, 4, 0, 0]} barSize={32} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>

        <div className="bg-white p-6 rounded-xl border border-slate-200 shadow-sm">
          <h3 className="text-lg font-bold text-slate-800 mb-4">Срочные задачи</h3>
          <div className="space-y-4">
            {stats.tasks.map((item) => (
              <div key={item.id} className="flex items-start gap-3 p-3 rounded-lg hover:bg-slate-50 transition-colors border border-transparent hover:border-slate-100 cursor-pointer">
                <div className={`w-2 h-2 mt-2 rounded-full flex-shrink-0 ${
                  item.type === 'urgent' ? 'bg-red-500' : item.type === 'warning' ? 'bg-amber-500' : 'bg-blue-400'
                }`} />
                <div>
                  <p className="text-sm font-medium text-slate-800">{item.title}</p>
                  <p className="text-xs text-slate-500">{item.time}</p>
                </div>
              </div>
            ))}
          </div>
          <button 
            onClick={() => navigate('/calendar')}
            className="w-full mt-6 py-2 text-sm text-blue-600 font-medium bg-blue-50 rounded-lg hover:bg-blue-100 transition-colors"
          >
            Календарь и задачи
          </button>
        </div>
      </div>
    </div>
  );
};

export default Dashboard;