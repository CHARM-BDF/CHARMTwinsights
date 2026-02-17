import type { ReactNode } from 'react';
import { useSession } from './session-context';
import type { RouteGuardPolicy } from '../lib/contracts/auth';

const defaultPolicy: RouteGuardPolicy = {
  requiresAuthentication: true,
  allowedRoles: ['research'],
};

type Props = {
  children: ReactNode;
  policy?: RouteGuardPolicy;
};

export function RouteGuard({ children, policy = defaultPolicy }: Props) {
  const session = useSession();

  if (policy.requiresAuthentication && !session.isAuthenticated) {
    return <p>Authentication is required to access this workspace.</p>;
  }

  if (!policy.allowedRoles.includes(session.user.role)) {
    return <p>Your account does not have access to this section.</p>;
  }

  return <>{children}</>;
}
