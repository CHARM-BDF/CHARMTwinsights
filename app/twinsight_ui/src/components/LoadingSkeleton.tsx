import styles from './LoadingSkeleton.module.css';

type Props = {
  lines?: number;
};

export function LoadingSkeleton({ lines = 3 }: Props) {
  return (
    <div aria-hidden="true" className={styles.container}>
      {Array.from({ length: lines }).map((_, index) => (
        <div key={index} className={styles.line} />
      ))}
    </div>
  );
}
