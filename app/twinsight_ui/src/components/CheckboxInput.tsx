import { forwardRef, type InputHTMLAttributes } from 'react';
import styles from './Field.module.css';

type Props = {
  id: string;
  label: string;
  hint?: string;
} & InputHTMLAttributes<HTMLInputElement>;

export const CheckboxInput = forwardRef<HTMLInputElement, Props>(function CheckboxInput(
  { id, label, hint, ...rest },
  ref,
) {
  return (
    <div className={styles.field}>
      <div className={styles.checkboxRow}>
        <input id={id} ref={ref} type="checkbox" {...rest} />
        <label htmlFor={id}>{label}</label>
      </div>
      {hint ? <span className={styles.hint}>{hint}</span> : null}
    </div>
  );
});
