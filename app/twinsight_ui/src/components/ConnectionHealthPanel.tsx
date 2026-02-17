import { useMemo } from 'react';
import { useSettings } from '../app/settings-context';
import { useConnectionHealth } from '../features/system/hooks';
import { ErrorBanner } from './ErrorBanner';
import { LoadingSkeleton } from './LoadingSkeleton';
import { SecondaryButton } from './SecondaryButton';
import styles from './ConnectionHealthPanel.module.css';

function statusLabel(status: 'healthy' | 'degraded' | 'unreachable') {
  if (status === 'healthy') {
    return 'Healthy';
  }
  if (status === 'degraded') {
    return 'Degraded';
  }
  return 'Unreachable';
}

export function ConnectionHealthPanel() {
  const { serviceMode } = useSettings();
  const healthQuery = useConnectionHealth(serviceMode);

  const summary = useMemo(() => {
    if (healthQuery.isLoading) {
      return 'Running endpoint checks...';
    }

    const checks = healthQuery.data ?? [];
    if (checks.length === 0) {
      return 'No checks available.';
    }

    const unhealthyCount = checks.filter((item) => item.status !== 'healthy').length;
    if (unhealthyCount === 0) {
      return 'All configured endpoints are reachable.';
    }

    return `${unhealthyCount} endpoint check(s) need attention.`;
  }, [healthQuery.data, healthQuery.isLoading]);

  return (
    <section className={styles.panel} aria-label="Connection health">
      <div className={styles.headerRow}>
        <div>
          <h2>Connection Health</h2>
          <p>Mode: <strong>{serviceMode}</strong>. {summary}</p>
          {serviceMode === 'direct' ? (
            <p className={styles.readOnlyHint}>
              Direct mode currently enables live read workflows for cohorts and models. Write intents remain mock-only in this phase.
            </p>
          ) : null}
        </div>
        <SecondaryButton onClick={() => healthQuery.refetch()} disabled={healthQuery.isFetching}>
          {healthQuery.isFetching ? 'Checking...' : 'Refresh Checks'}
        </SecondaryButton>
      </div>

      {healthQuery.isLoading ? <LoadingSkeleton lines={4} /> : null}
      {healthQuery.isError ? <ErrorBanner message="Unable to run connection health checks." /> : null}

      {!healthQuery.isLoading && !healthQuery.isError ? (
        <div className={styles.tableWrap}>
          <table>
            <caption>Endpoint connectivity checks</caption>
            <thead>
              <tr>
                <th scope="col">Target</th>
                <th scope="col">Endpoint</th>
                <th scope="col">Status</th>
                <th scope="col">Detail</th>
                <th scope="col">Latency</th>
              </tr>
            </thead>
            <tbody>
              {healthQuery.data?.map((item) => (
                <tr key={item.id}>
                  <td>{item.label}</td>
                  <td><code>{item.endpoint}</code></td>
                  <td>
                    <span className={`${styles.status} ${styles[item.status]}`}>{statusLabel(item.status)}</span>
                  </td>
                  <td>{item.detail}</td>
                  <td>{item.latencyMs === null ? '-' : `${item.latencyMs} ms`}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : null}
    </section>
  );
}
