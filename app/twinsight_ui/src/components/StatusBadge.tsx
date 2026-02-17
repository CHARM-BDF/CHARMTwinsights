import styles from './StatusBadge.module.css';

export type BadgeStatus = 'queued' | 'running' | 'completed' | 'failed' | 'cancelled' | 'active' | 'archived';

type Props = {
  status: BadgeStatus;
};

export function StatusBadge({ status }: Props) {
  return <span className={`${styles.badge} ${styles[status]}`}>{status}</span>;
}
