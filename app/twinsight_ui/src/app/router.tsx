import { Navigate, createBrowserRouter, RouterProvider } from 'react-router-dom';
import { AppShell } from '../components/AppShell';
import { RouteGuard } from './route-guard';
import { DashboardPage } from '../pages/DashboardPage';
import { CohortBuilderPage } from '../pages/CohortBuilderPage';
import { CohortExplorerPage } from '../pages/CohortExplorerPage';
import { CohortDetailPage } from '../pages/CohortDetailPage';
import { ModelCatalogPage } from '../pages/ModelCatalogPage';
import { RunHistoryPage } from '../pages/RunHistoryPage';
import { SettingsPage } from '../pages/SettingsPage';
import { CopilotPage } from '../pages/CopilotPage';
import { NotFoundPage } from '../pages/NotFoundPage';

const router = createBrowserRouter([
  {
    path: '/',
    element: (
      <RouteGuard>
        <AppShell />
      </RouteGuard>
    ),
    children: [
      {
        index: true,
        element: <Navigate to="/dashboard" replace />,
      },
      { path: '/dashboard', element: <DashboardPage /> },
      { path: '/cohorts', element: <CohortExplorerPage /> },
      { path: '/cohorts/new', element: <CohortBuilderPage /> },
      { path: '/cohorts/:cohortId', element: <CohortDetailPage /> },
      { path: '/models', element: <ModelCatalogPage /> },
      { path: '/runs', element: <RunHistoryPage /> },
      { path: '/settings', element: <SettingsPage /> },
      { path: '/copilot', element: <CopilotPage /> },
      { path: '*', element: <NotFoundPage /> },
    ],
  },
]);

export function AppRouter() {
  return <RouterProvider router={router} />;
}
