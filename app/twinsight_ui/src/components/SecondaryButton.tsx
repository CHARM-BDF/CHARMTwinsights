import type { ButtonHTMLAttributes } from 'react';
import styles from './Button.module.css';

type Props = ButtonHTMLAttributes<HTMLButtonElement>;

export function SecondaryButton(props: Props) {
  const { className = '', ...rest } = props;
  return <button type="button" className={`${styles.button} ${styles.secondary} ${className}`} {...rest} />;
}
