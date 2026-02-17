import type { ButtonHTMLAttributes } from 'react';
import styles from './Button.module.css';

type Props = ButtonHTMLAttributes<HTMLButtonElement>;

export function PrimaryButton(props: Props) {
  const { className = '', ...rest } = props;
  return <button type="button" className={`${styles.button} ${styles.primary} ${className}`} {...rest} />;
}
