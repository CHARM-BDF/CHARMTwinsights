import { forwardRef, type SelectHTMLAttributes } from 'react';
import styles from './Field.module.css';

type Option = {
  value: string;
  label: string;
};

type Props = {
  id: string;
  label: string;
  options: Option[];
  hint?: string;
  error?: string;
} & SelectHTMLAttributes<HTMLSelectElement>;

export const SelectInput = forwardRef<HTMLSelectElement, Props>(function SelectInput(
  { id, label, options, hint, error, ...rest },
  ref,
) {
  return (
    <div className={styles.field}>
      <label className={styles.label} htmlFor={id}>
        {label}
      </label>
      <select id={id} ref={ref} className={styles.select} aria-invalid={Boolean(error)} {...rest}>
        {options.map((option) => (
          <option key={option.value} value={option.value}>
            {option.label}
          </option>
        ))}
      </select>
      {hint ? <span className={styles.hint}>{hint}</span> : null}
      {error ? <span className={styles.error}>{error}</span> : null}
    </div>
  );
});
