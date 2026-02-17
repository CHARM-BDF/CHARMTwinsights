import { NavLink } from 'react-router-dom';
import styles from './SideNav.module.css';

const links = [
  { path: '/dashboard', label: 'Dashboard' },
  { path: '/cohorts/new', label: 'Cohort Builder' },
  { path: '/cohorts', label: 'Cohort Explorer' },
  { path: '/models', label: 'Model Catalog' },
  { path: '/runs', label: 'Run History' },
  { path: '/settings', label: 'Settings' },
  { path: '/copilot', label: 'Copilot' },
];

export function SideNav() {
  return (
    <nav className={styles.nav} aria-label="Main navigation">
      <h1>CHARMTwinsight Studio</h1>
      <ul>
        {links.map((link) => (
          <li key={link.path}>
            <NavLink
              to={link.path}
              className={({ isActive }) => (isActive ? styles.active : '')}
              end={link.path === '/dashboard'}
            >
              {link.label}
            </NavLink>
          </li>
        ))}
      </ul>
    </nav>
  );
}
