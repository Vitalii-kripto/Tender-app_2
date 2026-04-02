import React, { useState, useEffect } from 'react';
import { Calendar as CalendarIcon, Clock, AlertCircle, CheckCircle, ChevronLeft, ChevronRight, Bell } from 'lucide-react';
import { Tender } from '../types';
import { getTendersFromBackend } from '../services/geminiService';

const Calendar = () => {
  const [tenders, setTenders] = useState<Tender[]>([]);
  const [currentDate, setCurrentDate] = useState(new Date());
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const loadTenders = async () => {
      try {
        const data = await getTendersFromBackend();
        setTenders(data);
      } catch (error) {
        console.error("Failed to load tenders for calendar", error);
      } finally {
        setLoading(false);
      }
    };
    loadTenders();
  }, []);

  const getDaysInMonth = (year: number, month: number) => {
    return new Date(year, month + 1, 0).getDate();
  };

  const getFirstDayOfMonth = (year: number, month: number) => {
    const day = new Date(year, month, 1).getDay();
    return day === 0 ? 6 : day - 1; // Adjust for Monday start
  };

  const prevMonth = () => {
    setCurrentDate(new Date(currentDate.getFullYear(), currentDate.getMonth() - 1, 1));
  };

  const nextMonth = () => {
    setCurrentDate(new Date(currentDate.getFullYear(), currentDate.getMonth() + 1, 1));
  };

  const monthNames = [
    'Январь', 'Февраль', 'Март', 'Апрель', 'Май', 'Июнь',
    'Июль', 'Август', 'Сентябрь', 'Октябрь', 'Ноябрь', 'Декабрь'
  ];

  const daysOfWeek = ['Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Вс'];

  const year = currentDate.getFullYear();
  const month = currentDate.getMonth();
  const daysInMonth = getDaysInMonth(year, month);
  const firstDay = getFirstDayOfMonth(year, month);

  // Helper to parse date string (assuming format DD.MM.YYYY or similar)
  const parseDate = (dateStr: string) => {
    if (!dateStr || dateStr === '-' || dateStr === 'См. ЕИС') return null;
    // Simple parsing for DD.MM.YYYY
    const parts = dateStr.split('.');
    if (parts.length === 3) {
      return new Date(parseInt(parts[2]), parseInt(parts[1]) - 1, parseInt(parts[0]));
    }
    return new Date(dateStr); // Fallback
  };

  const getEventsForDay = (day: number) => {
    const targetDate = new Date(year, month, day);
    return tenders.filter(t => {
      const d = parseDate(t.deadline);
      return d && d.getDate() === targetDate.getDate() && d.getMonth() === targetDate.getMonth() && d.getFullYear() === targetDate.getFullYear();
    });
  };

  const renderCalendarDays = () => {
    const days = [];
    for (let i = 0; i < firstDay; i++) {
      days.push(<div key={`empty-${i}`} className="h-24 border border-slate-100 bg-slate-50/50"></div>);
    }

    for (let day = 1; day <= daysInMonth; day++) {
      const events = getEventsForDay(day);
      const isToday = new Date().getDate() === day && new Date().getMonth() === month && new Date().getFullYear() === year;

      days.push(
        <div key={day} className={`h-24 border border-slate-100 p-2 overflow-y-auto custom-scrollbar ${isToday ? 'bg-blue-50/30' : 'bg-white'}`}>
          <div className={`text-xs font-medium mb-1 ${isToday ? 'text-blue-600 font-bold' : 'text-slate-500'}`}>
            {day}
          </div>
          <div className="space-y-1">
            {events.map(event => (
              <div key={event.id} className="text-[10px] p-1 rounded bg-blue-100 text-blue-800 truncate border border-blue-200" title={event.title}>
                {event.eis_number}: {event.title}
              </div>
            ))}
          </div>
        </div>
      );
    }
    return days;
  };

  // Generate mock notifications based on tenders
  const notifications = tenders.slice(0, 5).map(t => ({
    id: t.id,
    title: `Дедлайн подачи заявок: ${t.eis_number}`,
    time: t.deadline,
    type: t.risk_level === 'High' ? 'urgent' : 'warning'
  }));

  return (
    <div className="p-6 max-w-7xl mx-auto pb-20">
      <div className="mb-8">
        <h2 className="text-2xl font-bold text-slate-900 flex items-center gap-2">
          <CalendarIcon className="text-blue-600" />
          Календарь и Уведомления
        </h2>
        <p className="text-slate-500 text-sm mt-1">
          Отслеживание дедлайнов и важных событий по тендерам.
        </p>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-4 gap-6">
        {/* Calendar Section */}
        <div className="lg:col-span-3 bg-white rounded-xl border border-slate-200 shadow-sm overflow-hidden">
          <div className="p-4 border-b border-slate-200 flex justify-between items-center bg-slate-50">
            <h3 className="text-lg font-bold text-slate-800">
              {monthNames[month]} {year}
            </h3>
            <div className="flex gap-2">
              <button onClick={prevMonth} className="p-2 rounded-lg hover:bg-slate-200 text-slate-600 transition-colors">
                <ChevronLeft size={20} />
              </button>
              <button onClick={nextMonth} className="p-2 rounded-lg hover:bg-slate-200 text-slate-600 transition-colors">
                <ChevronRight size={20} />
              </button>
            </div>
          </div>
          <div className="p-4">
            <div className="grid grid-cols-7 gap-px mb-2">
              {daysOfWeek.map(day => (
                <div key={day} className="text-center text-xs font-bold text-slate-400 uppercase tracking-wider py-2">
                  {day}
                </div>
              ))}
            </div>
            <div className="grid grid-cols-7 gap-px bg-slate-200 border border-slate-200 rounded-lg overflow-hidden">
              {loading ? (
                <div className="col-span-7 h-64 flex items-center justify-center bg-white">
                  <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600"></div>
                </div>
              ) : (
                renderCalendarDays()
              )}
            </div>
          </div>
        </div>

        {/* Notifications Section */}
        <div className="bg-white rounded-xl border border-slate-200 shadow-sm flex flex-col h-[600px]">
          <div className="p-4 border-b border-slate-200 bg-slate-50 flex items-center gap-2">
            <Bell size={18} className="text-slate-600" />
            <h3 className="font-bold text-slate-800">Уведомления</h3>
          </div>
          <div className="flex-1 overflow-y-auto p-4 space-y-3">
            {notifications.length === 0 ? (
              <div className="text-center text-slate-400 mt-10">
                <CheckCircle size={32} className="mx-auto mb-2 opacity-50" />
                <p className="text-sm">Нет новых уведомлений</p>
              </div>
            ) : (
              notifications.map((notif, idx) => (
                <div key={idx} className="p-3 rounded-lg border border-slate-100 bg-slate-50 hover:bg-slate-100 transition-colors">
                  <div className="flex items-start gap-3">
                    <div className={`mt-0.5 w-2 h-2 rounded-full flex-shrink-0 ${notif.type === 'urgent' ? 'bg-red-500' : 'bg-amber-500'}`} />
                    <div>
                      <p className="text-sm font-medium text-slate-800 leading-tight">{notif.title}</p>
                      <div className="flex items-center gap-1 mt-1.5 text-xs text-slate-500">
                        <Clock size={12} />
                        <span>{notif.time || 'Дата не указана'}</span>
                      </div>
                    </div>
                  </div>
                </div>
              ))
            )}
          </div>
          <div className="p-3 border-t border-slate-200 bg-slate-50">
            <button className="w-full py-2 text-sm text-blue-600 font-medium hover:bg-blue-50 rounded-lg transition-colors">
              Настроить Telegram бота
            </button>
          </div>
        </div>
      </div>
    </div>
  );
};

export default Calendar;
