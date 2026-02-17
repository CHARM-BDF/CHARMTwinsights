import { createContext, useContext, useMemo } from 'react';
import type { ReactNode } from 'react';
import { createApiServices, type ApiServices } from '../lib/api/serviceFactory';
import { useSettings } from './settings-context';

const ApiServicesContextValue = createContext<ApiServices | null>(null);

type Props = {
  children: ReactNode;
};

export function ApiServicesProvider({ children }: Props) {
  const { serviceMode } = useSettings();
  const services = useMemo(() => createApiServices(serviceMode), [serviceMode]);

  return <ApiServicesContextValue.Provider value={services}>{children}</ApiServicesContextValue.Provider>;
}

export function useApiServices() {
  const context = useContext(ApiServicesContextValue);
  if (!context) {
    throw new Error('useApiServices must be used within ApiServicesProvider');
  }
  return context;
}
