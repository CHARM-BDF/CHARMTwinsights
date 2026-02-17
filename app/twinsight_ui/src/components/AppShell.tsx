import { Outlet } from 'react-router-dom';
import { SideNav } from './SideNav';
import { TopBar } from './TopBar';
import { SkipToContentLink } from './SkipToContentLink';
import styles from './AppShell.module.css';

export function AppShell() {
  return (
    <div className={styles.shell}>
      <SkipToContentLink />
      <aside>
        <SideNav />
      </aside>
      <div className={styles.mainColumn}>
        <TopBar />
        <main id="main-content" className={styles.main} tabIndex={-1}>
          <Outlet />
        </main>
      </div>
    </div>
  );
}
