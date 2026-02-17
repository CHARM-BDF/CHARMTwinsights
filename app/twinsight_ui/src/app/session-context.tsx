import { createContext, useContext } from 'react';
import type { ReactNode } from 'react';
import type { SessionContext, UserRole } from '../lib/contracts/auth';

const defaultSession: SessionContext = {
  user: {
    id: 'research-user',
    displayName: 'Research Analyst',
    role: 'research',
  },
  isAuthenticated: true,
};

const SessionContextValue = createContext<SessionContext>(defaultSession);

type Props = {
  children: ReactNode;
};

export function SessionProvider({ children }: Props) {
  return <SessionContextValue.Provider value={defaultSession}>{children}</SessionContextValue.Provider>;
}

export function useSession() {
  return useContext(SessionContextValue);
}

export function useUserRole(): UserRole {
  return useSession().user.role;
}
