import type { ReactNode } from 'react';
import styles from './FilterPanel.module.css';

type Props = {
  title: string;
  children: ReactNode;
};

export function FilterPanel({ title, children }: Props) {
  return (
    <section className={styles.panel} aria-label={`${title} filters`}>
      <h2>{title}</h2>
      <div className={styles.content}>{children}</div>
    </section>
  );
}
