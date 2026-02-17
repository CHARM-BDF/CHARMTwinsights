export type UserRole = 'research';

export type SessionUser = {
  id: string;
  displayName: string;
  role: UserRole;
};

export type SessionContext = {
  user: SessionUser;
  isAuthenticated: boolean;
};

export type RouteGuardPolicy = {
  requiresAuthentication: boolean;
  allowedRoles: UserRole[];
};
