import React from 'react';
import ReactDOM from 'react-dom/client';
import { QueryClient } from '@tanstack/react-query';
import { AppProviders } from './app/Providers';
import { AppRouter } from './app/router';
import { initializeMocking } from './mocks/browser';
import './styles/reset.css';
import './styles/tokens.css';
import './styles/global.css';

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 30_000,
      refetchOnWindowFocus: false,
      retry: 1,
    },
  },
});

initializeMocking().catch(() => {
  // App rendering should never be blocked by mock startup failures.
});

ReactDOM.createRoot(document.getElementById('root') as HTMLElement).render(
  <React.StrictMode>
    <AppProviders queryClient={queryClient}>
      <AppRouter />
    </AppProviders>
  </React.StrictMode>,
);
