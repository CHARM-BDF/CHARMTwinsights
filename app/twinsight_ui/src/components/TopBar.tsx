import { useSettings } from '../app/settings-context';
import { useSession } from '../app/session-context';
import styles from './TopBar.module.css';

export function TopBar() {
  const { serviceMode, persona } = useSettings();
  const { user } = useSession();

  return (
    <header className={styles.topBar}>
      <p>
        Workspace mode: <strong>{serviceMode}</strong>
      </p>
      <p>
        Persona: <strong>{persona}</strong>
      </p>
      <p>
        Signed in as <strong>{user.displayName}</strong>
      </p>
    </header>
  );
}
