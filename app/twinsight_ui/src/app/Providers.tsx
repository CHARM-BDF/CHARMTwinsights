import { QueryClientProvider, type QueryClient } from '@tanstack/react-query';
import type { ReactNode } from 'react';
import { SessionProvider } from './session-context';
import { SettingsProvider } from './settings-context';
import { ApiServicesProvider } from './api-services-context';

type Props = {
  children: ReactNode;
  queryClient: QueryClient;
};

export function AppProviders({ children, queryClient }: Props) {
  return (
    <QueryClientProvider client={queryClient}>
      <SessionProvider>
        <SettingsProvider>
          <ApiServicesProvider>{children}</ApiServicesProvider>
        </SettingsProvider>
      </SessionProvider>
    </QueryClientProvider>
  );
}
