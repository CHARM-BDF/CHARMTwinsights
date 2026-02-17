import styles from './JsonPreview.module.css';

type Props = {
  title: string;
  value: unknown;
};

export function JsonPreview({ title, value }: Props) {
  return (
    <section className={styles.wrapper} aria-label={title}>
      <h3>{title}</h3>
      <pre>{JSON.stringify(value, null, 2)}</pre>
    </section>
  );
}
