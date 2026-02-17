import styles from './ActivityTimeline.module.css';

type Item = {
  id: string;
  title: string;
  timestamp: string;
  detail: string;
};

type Props = {
  items: Item[];
};

export function ActivityTimeline({ items }: Props) {
  return (
    <section className={styles.wrapper} aria-label="Activity timeline">
      <h3>Activity Timeline</h3>
      <ol>
        {items.map((item) => (
          <li key={item.id}>
            <div>
              <strong>{item.title}</strong>
              <span>{new Date(item.timestamp).toLocaleString()}</span>
            </div>
            <p>{item.detail}</p>
          </li>
        ))}
      </ol>
    </section>
  );
}
