import styles from './KeyboardShortcutHint.module.css';

type Props = {
  keys: string;
  action: string;
};

export function KeyboardShortcutHint({ keys, action }: Props) {
  return (
    <span className={styles.hint} aria-label={`Shortcut ${keys} for ${action}`}>
      <kbd>{keys}</kbd> {action}
    </span>
  );
}
