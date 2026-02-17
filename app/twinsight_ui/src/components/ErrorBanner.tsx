import styles from './ErrorBanner.module.css';

type Props = {
  message: string;
};

export function ErrorBanner({ message }: Props) {
  return (
    <div className={styles.banner} role="alert">
      <strong>Request failed.</strong> {message}
    </div>
  );
}
