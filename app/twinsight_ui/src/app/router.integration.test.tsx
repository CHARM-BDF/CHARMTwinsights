import { QueryClient } from '@tanstack/react-query';
import { render, screen } from '@testing-library/react';
import { RouterProvider, createMemoryRouter } from 'react-router-dom';
import { AppProviders } from './Providers';
import { AppShell } from '../components/AppShell';
import { DashboardPage } from '../pages/DashboardPage';
import { SettingsPage } from '../pages/SettingsPage';

describe('router integration', () => {
  function createTestRouter(initialEntry: '/dashboard' | '/settings') {
    return createMemoryRouter(
      [
        {
          path: '/',
          element: <AppShell />,
          children: [
            { path: '/dashboard', element: <DashboardPage /> },
            { path: '/settings', element: <SettingsPage /> },
          ],
        },
      ],
      { initialEntries: [initialEntry] },
    );
  }

  it('loads dashboard route', async () => {
    const queryClient = new QueryClient({
      defaultOptions: {
        queries: { retry: false },
      },
    });

    const router = createTestRouter('/dashboard');

    render(
      <AppProviders queryClient={queryClient}>
        <RouterProvider router={router} />
      </AppProviders>,
    );

    expect(await screen.findByRole('heading', { name: 'Dashboard' })).toBeInTheDocument();
  });

  it('loads settings route', async () => {
    const queryClient = new QueryClient({
      defaultOptions: {
        queries: { retry: false },
      },
    });

    const router = createTestRouter('/settings');

    render(
      <AppProviders queryClient={queryClient}>
        <RouterProvider router={router} />
      </AppProviders>,
    );

    expect(await screen.findByRole('heading', { name: 'Settings' })).toBeInTheDocument();
  });
});
