import { createContext, useContext, useMemo, useState } from 'react';
import type { ReactNode } from 'react';
import type { ServiceMode } from '../lib/api/endpointRegistry';

export type FeatureFlags = {
  uiPrimary: boolean;
  copilotEnabled: boolean;
};

type SettingsState = {
  serviceMode: ServiceMode;
  setServiceMode: (mode: ServiceMode) => void;
  featureFlags: FeatureFlags;
  setFeatureFlag: (flag: keyof FeatureFlags, value: boolean) => void;
  persona: 'research';
};

const STORAGE_KEY = 'twinsight_ui_settings';

const defaultState = {
  serviceMode: 'mock' as ServiceMode,
  featureFlags: {
    uiPrimary: import.meta.env.VITE_UI_PRIMARY === 'true',
    copilotEnabled: false,
  },
};

const SettingsContextValue = createContext<SettingsState | null>(null);

function loadInitialState() {
  const stored = localStorage.getItem(STORAGE_KEY);
  if (!stored) {
    return defaultState;
  }

  try {
    const parsed = JSON.parse(stored);
    return {
      serviceMode: (parsed.serviceMode ?? defaultState.serviceMode) as ServiceMode,
      featureFlags: {
        ...defaultState.featureFlags,
        ...(parsed.featureFlags ?? {}),
      },
    };
  } catch {
    return defaultState;
  }
}

type Props = {
  children: ReactNode;
};

export function SettingsProvider({ children }: Props) {
  const [state, setState] = useState(loadInitialState);

  const value = useMemo<SettingsState>(
    () => ({
      serviceMode: state.serviceMode,
      setServiceMode: (mode: ServiceMode) => {
        setState((previous) => {
          const next = { ...previous, serviceMode: mode };
          localStorage.setItem(STORAGE_KEY, JSON.stringify(next));
          return next;
        });
      },
      featureFlags: state.featureFlags,
      setFeatureFlag: (flag: keyof FeatureFlags, value: boolean) => {
        setState((previous) => {
          const next = {
            ...previous,
            featureFlags: {
              ...previous.featureFlags,
              [flag]: value,
            },
          };
          localStorage.setItem(STORAGE_KEY, JSON.stringify(next));
          return next;
        });
      },
      persona: 'research',
    }),
    [state],
  );

  return <SettingsContextValue.Provider value={value}>{children}</SettingsContextValue.Provider>;
}

export function useSettings() {
  const context = useContext(SettingsContextValue);
  if (!context) {
    throw new Error('useSettings must be used within SettingsProvider');
  }
  return context;
}
