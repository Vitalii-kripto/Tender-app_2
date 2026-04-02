import React, { useState, useEffect, useRef } from 'react';
import { Bell, Search, User, Menu, Settings, LogOut, Check, X, Briefcase, ShoppingCart, Loader2, FileText, ChevronRight } from 'lucide-react';
import { Link, useNavigate } from 'react-router-dom';
import { getEmployees, getTendersFromBackend, getProductsFromBackend } from './services/geminiService';
import { Tender, Product } from './types';

const Header = () => {
  const navigate = useNavigate();
  const [showNotifications, setShowNotifications] = useState(false);
  const [showProfileMenu, setShowProfileMenu] = useState(false);
  const [currentUser, setCurrentUser] = useState({ name: 'Алексей Иванов', role: 'admin' });
  
  // Search State
  const [searchQuery, setSearchQuery] = useState('');
  const [isSearching, setIsSearching] = useState(false);
  const [showSearchResults, setShowSearchResults] = useState(false);
  const [searchResults, setSearchResults] = useState<{tenders: Tender[], products: Product[]}>({ tenders: [], products: [] });

  // Refs for click outside
  const notifRef = useRef<HTMLDivElement>(null);
  const profileRef = useRef<HTMLDivElement>(null);
  const searchRef = useRef<HTMLDivElement>(null);

  // Mock Notifications
  const [notifications, setNotifications] = useState([
    { id: 1, text: 'Найден новый тендер по 44-ФЗ (Кровля)', time: '5 мин назад', read: false, type: 'info' },
    { id: 2, text: '⚠️ Высокий риск: Закупка №03732...', time: '1 час назад', read: false, type: 'alert' },
    { id: 3, text: 'ИИ Анализ завершен: Школа №5', time: '2 часа назад', read: true, type: 'success' }
  ]);

  useEffect(() => {
    // Simulate getting the "logged in" user (taking the first one from settings)
    const employees = getEmployees();
    if (employees.length > 0) {
      setCurrentUser({ name: employees[0].name, role: employees[0].role });
    }

    // Click outside handler
    const handleClickOutside = (event: MouseEvent) => {
      if (notifRef.current && !notifRef.current.contains(event.target as Node)) {
        setShowNotifications(false);
      }
      if (profileRef.current && !profileRef.current.contains(event.target as Node)) {
        setShowProfileMenu(false);
      }
      if (searchRef.current && !searchRef.current.contains(event.target as Node)) {
        setShowSearchResults(false);
      }
    };

    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  // --- GLOBAL SEARCH LOGIC ---
  useEffect(() => {
      const performSearch = async () => {
          if (searchQuery.length < 2) {
              setSearchResults({ tenders: [], products: [] });
              return;
          }

          setIsSearching(true);
          try {
              // Parallel fetch (in real app, this would be a single search endpoint)
              const [allTenders, allProducts] = await Promise.all([
                  getTendersFromBackend(),
                  getProductsFromBackend()
              ]);

              const lowerQuery = searchQuery.toLowerCase();

              const foundTenders = allTenders.filter(t => 
                  t.title.toLowerCase().includes(lowerQuery) || 
                  t.eis_number.includes(lowerQuery)
              ).slice(0, 5); // Limit to 5

              const foundProducts = allProducts.filter(p => 
                  p.title.toLowerCase().includes(lowerQuery)
              ).slice(0, 5); // Limit to 5

              setSearchResults({ tenders: foundTenders, products: foundProducts });
              setShowSearchResults(true);
          } catch (error) {
              console.error(error);
          } finally {
              setIsSearching(false);
          }
      };

      const debounce = setTimeout(performSearch, 500);
      return () => clearTimeout(debounce);
  }, [searchQuery]);


  const handleSearchResultClick = (path: string) => {
      navigate(path);
      setShowSearchResults(false);
      setSearchQuery('');
  };

  const unreadCount = notifications.filter(n => !n.read).length;

  const markAsRead = (id: number) => {
    setNotifications(prev => prev.map(n => n.id === id ? { ...n, read: true } : n));
  };

  const clearNotifications = () => {
      setNotifications([]);
      setShowNotifications(false);
  };

  const getRoleLabel = (role: string) => {
      switch(role) {
          case 'admin': return 'Администратор';
          case 'manager': return 'Менеджер';
          case 'analyst': return 'Аналитик';
          default: return role;
      }
  };

  return (
    <header className="h-16 bg-white border-b border-slate-200 flex items-center justify-between px-4 md:px-8 md:ml-64 sticky top-0 z-30 shadow-sm">
      <div className="flex items-center gap-4">
        <button className="md:hidden p-2 text-slate-600 hover:bg-slate-100 rounded-md">
          <Menu size={24} />
        </button>
        
        {/* GLOBAL SEARCH BAR */}
        <div className="relative hidden sm:block" ref={searchRef}>
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-400" size={18} />
          <input 
            type="text" 
            placeholder="Поиск (Тендер, №ЕИС, Товар)..." 
            className="pl-10 pr-4 py-2 w-64 lg:w-96 rounded-full border border-slate-300 bg-white text-slate-900 placeholder-slate-400 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent text-sm transition-all"
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            onFocus={() => { if(searchQuery.length >= 2) setShowSearchResults(true); }}
          />
          {isSearching && (
              <div className="absolute right-3 top-1/2 -translate-y-1/2">
                  <Loader2 size={16} className="animate-spin text-blue-500"/>
              </div>
          )}

          {/* SEARCH RESULTS DROPDOWN */}
          {showSearchResults && (searchResults.tenders.length > 0 || searchResults.products.length > 0) && (
              <div className="absolute top-full left-0 mt-2 w-full lg:w-[450px] bg-white border border-slate-200 rounded-xl shadow-xl overflow-hidden z-50 animate-in fade-in slide-in-from-top-2">
                  {searchResults.tenders.length > 0 && (
                      <div>
                          <div className="bg-slate-50 px-4 py-2 text-xs font-bold text-slate-500 uppercase flex items-center gap-2">
                              <Briefcase size={12}/> Тендеры
                          </div>
                          {searchResults.tenders.map(t => (
                              <button 
                                key={t.id} 
                                onClick={() => handleSearchResultClick('/crm')}
                                className="w-full text-left px-4 py-3 hover:bg-blue-50 border-b border-slate-50 transition-colors group"
                              >
                                  <div className="flex justify-between items-start">
                                      <span className="font-semibold text-sm text-slate-800 line-clamp-1 group-hover:text-blue-600">{t.title}</span>
                                      <span className="text-[10px] text-slate-400 bg-slate-100 px-1.5 py-0.5 rounded ml-2 whitespace-nowrap">{t.eis_number}</span>
                                  </div>
                                  <div className="flex justify-between mt-1 text-xs text-slate-500">
                                      <span>₽{(Number(t.initial_price)/1000).toLocaleString()}k</span>
                                      <span className={`px-1.5 rounded ${t.status === 'Found' ? 'bg-blue-100 text-blue-700' : 'bg-emerald-100 text-emerald-700'}`}>{t.status}</span>
                                  </div>
                              </button>
                          ))}
                      </div>
                  )}

                  {searchResults.products.length > 0 && (
                      <div>
                          <div className="bg-slate-50 px-4 py-2 text-xs font-bold text-slate-500 uppercase flex items-center gap-2 border-t border-slate-100">
                              <ShoppingCart size={12}/> Продукция
                          </div>
                          {searchResults.products.map(p => (
                              <button 
                                key={p.id} 
                                onClick={() => handleSearchResultClick('/catalog')}
                                className="w-full text-left px-4 py-2.5 hover:bg-blue-50 border-b border-slate-50 transition-colors flex justify-between items-center group"
                              >
                                  <div>
                                      <p className="font-medium text-sm text-slate-800 group-hover:text-blue-600">{p.title}</p>
                                      <p className="text-xs text-slate-400">{p.category}</p>
                                  </div>
                                  <ChevronRight size={14} className="text-slate-300 group-hover:text-blue-500"/>
                              </button>
                          ))}
                      </div>
                  )}
                  
                  {searchResults.tenders.length === 0 && searchResults.products.length === 0 && !isSearching && (
                      <div className="p-4 text-center text-slate-400 text-sm">
                          Ничего не найдено
                      </div>
                  )}
              </div>
          )}
        </div>
      </div>

      <div className="flex items-center gap-6">
        
        {/* Notifications */}
        <div className="relative" ref={notifRef}>
            <button 
                onClick={() => setShowNotifications(!showNotifications)}
                className={`relative p-2 rounded-full transition-colors ${showNotifications ? 'bg-blue-50 text-blue-600' : 'text-slate-600 hover:bg-slate-100'}`}
            >
                <Bell size={20} />
                {unreadCount > 0 && (
                    <span className="absolute top-1 right-1 w-2.5 h-2.5 bg-red-500 rounded-full border-2 border-white animate-pulse"></span>
                )}
            </button>

            {showNotifications && (
                <div className="absolute right-0 top-full mt-3 w-80 bg-white border border-slate-200 rounded-xl shadow-xl z-50 overflow-hidden animate-in fade-in slide-in-from-top-2">
                    <div className="p-3 border-b border-slate-100 flex justify-between items-center bg-slate-50">
                        <h4 className="font-bold text-sm text-slate-700">Уведомления</h4>
                        {notifications.length > 0 && (
                            <button onClick={clearNotifications} className="text-xs text-slate-400 hover:text-red-500">Очистить</button>
                        )}
                    </div>
                    <div className="max-h-80 overflow-y-auto">
                        {notifications.length === 0 ? (
                            <div className="p-6 text-center text-slate-400 text-sm">Нет новых уведомлений</div>
                        ) : (
                            <div className="divide-y divide-slate-50">
                                {notifications.map(note => (
                                    <div key={note.id} className={`p-3 hover:bg-slate-50 transition-colors ${!note.read ? 'bg-blue-50/30' : ''}`}>
                                        <div className="flex gap-3">
                                            <div className={`mt-1 w-2 h-2 rounded-full flex-shrink-0 ${note.type === 'alert' ? 'bg-red-500' : note.type === 'success' ? 'bg-emerald-500' : 'bg-blue-500'}`}></div>
                                            <div className="flex-1">
                                                <p className={`text-sm ${!note.read ? 'font-semibold text-slate-800' : 'text-slate-600'}`}>{note.text}</p>
                                                <p className="text-xs text-slate-400 mt-1">{note.time}</p>
                                            </div>
                                            {!note.read && (
                                                <button onClick={() => markAsRead(note.id)} className="text-blue-400 hover:text-blue-600 self-start" title="Прочитано">
                                                    <Check size={14} />
                                                </button>
                                            )}
                                        </div>
                                    </div>
                                ))}
                            </div>
                        )}
                    </div>
                    <div className="p-2 border-t border-slate-100 text-center">
                        <Link to="/crm" onClick={() => setShowNotifications(false)} className="text-xs font-medium text-blue-600 hover:underline">Показать все задачи</Link>
                    </div>
                </div>
            )}
        </div>

        {/* User Profile */}
        <div className="relative border-l border-slate-200 pl-6" ref={profileRef}>
            <button 
                onClick={() => setShowProfileMenu(!showProfileMenu)}
                className="flex items-center gap-3 group focus:outline-none"
            >
                <div className="text-right hidden sm:block">
                    <p className="text-sm font-semibold text-slate-800 group-hover:text-blue-600 transition-colors">{currentUser.name}</p>
                    <p className="text-xs text-slate-500">{getRoleLabel(currentUser.role)}</p>
                </div>
                <div className={`w-10 h-10 rounded-full flex items-center justify-center border transition-all ${showProfileMenu ? 'bg-blue-600 text-white border-blue-600 shadow-md ring-2 ring-blue-100' : 'bg-blue-50 text-blue-600 border-blue-200 group-hover:bg-blue-100'}`}>
                    <User size={20} />
                </div>
            </button>

            {showProfileMenu && (
                <div className="absolute right-0 top-full mt-3 w-56 bg-white border border-slate-200 rounded-xl shadow-xl z-50 overflow-hidden animate-in fade-in slide-in-from-top-2">
                    <div className="p-4 border-b border-slate-100 bg-slate-50">
                        <p className="font-bold text-slate-800">{currentUser.name}</p>
                        <p className="text-xs text-slate-500">{currentUser.role === 'admin' ? 'superuser' : 'user'}@company.com</p>
                    </div>
                    <div className="p-1">
                        <Link 
                            to="/settings" 
                            onClick={() => setShowProfileMenu(false)}
                            className="flex items-center gap-2 w-full px-3 py-2 text-sm text-slate-700 hover:bg-slate-50 rounded-lg transition-colors"
                        >
                            <Settings size={16} className="text-slate-400" />
                            Настройки профиля
                        </Link>
                        <button className="flex items-center gap-2 w-full px-3 py-2 text-sm text-red-600 hover:bg-red-50 rounded-lg transition-colors">
                            <LogOut size={16} />
                            Выйти из системы
                        </button>
                    </div>
                </div>
            )}
        </div>

      </div>
    </header>
  );
};

export default Header;