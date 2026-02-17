import { Link } from 'react-router-dom';
import { EmptyState } from '../components/EmptyState';

export function NotFoundPage() {
  return (
    <EmptyState
      title="Page not found"
      description="The requested route is not available in the current workspace."
      action={<Link to="/dashboard">Return to dashboard</Link>}
    />
  );
}
