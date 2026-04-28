import React, { useEffect, useMemo, useState } from 'react';
import { MemoryRouter as Router, Navigate, useLocation } from 'react-router-dom';
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

const APP_ROUTES = [
  { path: '/', Component: Dashboard },
  { path: '/catalog', Component: ProductCatalog },
  { path: '/crm', Component: TenderCRM },
  { path: '/matching', Component: ProductMatching },
  { path: '/compliance', Component: ComplianceCheck },
  { path: '/analysis', Component: Analysis },
  { path: '/tenders', Component: TenderSearch },
  { path: '/calendar', Component: Calendar },
  { path: '/settings', Component: Settings },
] as const;

const normalizePath = (pathname: string) =>
  pathname !== '/' && pathname.endsWith('/') ? pathname.slice(0, -1) : pathname;

function PersistentAppContent() {
  const location = useLocation();
  const currentPath = normalizePath(location.pathname);
  const knownPaths = useMemo<Set<string>>(
    () => new Set(APP_ROUTES.map((route) => route.path)),
    []
  );

  const [mountedPaths, setMountedPaths] = useState<Record<string, boolean>>(() => ({
    '/': true,
    ...(knownPaths.has(currentPath) ? { [currentPath]: true } : {}),
  }));

  useEffect(() => {
    if (!knownPaths.has(currentPath)) {
      return;
    }

    setMountedPaths((prev) => (
      prev[currentPath]
        ? prev
        : { ...prev, [currentPath]: true }
    ));
  }, [currentPath, knownPaths]);

  if (!knownPaths.has(currentPath)) {
    return <Navigate to="/" replace />;
  }

  return (
    <div className="min-h-screen bg-slate-50 font-sans text-slate-900 flex">
      <Sidebar />
      <div className="flex-1 flex flex-col md:ml-64 transition-all duration-300">
        <Header />
        <main className="flex-1 overflow-auto">
          {APP_ROUTES.map(({ path, Component }) => {
            if (!mountedPaths[path]) {
              return null;
            }

            const isActive = path === currentPath;

            return (
              <section
                key={path}
                aria-hidden={!isActive}
                className={isActive ? 'block h-full' : 'hidden h-full'}
              >
                <Component />
              </section>
            );
          })}
        </main>
      </div>
    </div>
  );
}

function App() {
  return (
    <Router>
      <PersistentAppContent />
    </Router>
  );
}

export default App;
