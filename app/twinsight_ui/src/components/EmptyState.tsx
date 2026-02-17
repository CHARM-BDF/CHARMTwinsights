import type { ReactNode } from 'react';
import styles from './EmptyState.module.css';

type Props = {
  title: string;
  description: string;
  action?: ReactNode;
};

export function EmptyState({ title, description, action }: Props) {
  return (
    <section className={styles.empty}>
      <h2>{title}</h2>
      <p>{description}</p>
      {action}
    </section>
  );
}
