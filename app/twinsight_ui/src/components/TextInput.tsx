import { forwardRef, type InputHTMLAttributes } from 'react';
import styles from './Field.module.css';

type Props = {
  id: string;
  label: string;
  hint?: string;
  error?: string;
} & InputHTMLAttributes<HTMLInputElement>;

export const TextInput = forwardRef<HTMLInputElement, Props>(function TextInput(
  { id, label, hint, error, ...rest },
  ref,
) {
  return (
    <div className={styles.field}>
      <label className={styles.label} htmlFor={id}>
        {label}
      </label>
      <input id={id} ref={ref} className={styles.input} aria-invalid={Boolean(error)} {...rest} />
      {hint ? <span className={styles.hint}>{hint}</span> : null}
      {error ? <span className={styles.error}>{error}</span> : null}
    </div>
  );
});
