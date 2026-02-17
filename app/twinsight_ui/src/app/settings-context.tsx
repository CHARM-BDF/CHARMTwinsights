import { createContext, useContext, useMemo, useState } from 'react';
import type { ReactNode } from 'react';
import { isServiceMode, type ServiceMode } from '../lib/api/endpointRegistry';

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
const configuredServiceMode = import.meta.env.VITE_SERVICE_MODE;
const envServiceMode: ServiceMode = isServiceMode(configuredServiceMode) ? configuredServiceMode : 'mock';

const defaultState = {
  serviceMode: envServiceMode,
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
    const parsed = JSON.parse(stored) as unknown;
    const parsedRecord =
      typeof parsed === 'object' && parsed !== null ? (parsed as Record<string, unknown>) : {};
    const parsedFlags =
      typeof parsedRecord.featureFlags === 'object' && parsedRecord.featureFlags !== null
        ? (parsedRecord.featureFlags as Record<string, unknown>)
        : {};
    const parsedMode = isServiceMode(parsedRecord.serviceMode) ? parsedRecord.serviceMode : defaultState.serviceMode;
    return {
      serviceMode: parsedMode,
      featureFlags: {
        uiPrimary:
          typeof parsedFlags.uiPrimary === 'boolean'
            ? parsedFlags.uiPrimary
            : defaultState.featureFlags.uiPrimary,
        copilotEnabled:
          typeof parsedFlags.copilotEnabled === 'boolean'
            ? parsedFlags.copilotEnabled
            : defaultState.featureFlags.copilotEnabled,
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
