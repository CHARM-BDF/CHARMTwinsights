import { Link } from 'react-router-dom';
import { PageHeader } from '../components/PageHeader';
import { LoadingSkeleton } from '../components/LoadingSkeleton';
import { ErrorBanner } from '../components/ErrorBanner';
import { useCohorts, useGenerationJobs } from '../features/cohorts/hooks';
import { useRuns } from '../features/runs/hooks';
import styles from './DashboardPage.module.css';

export function DashboardPage() {
  const cohortsQuery = useCohorts();
  const jobsQuery = useGenerationJobs();
  const runsQuery = useRuns();

  if (cohortsQuery.isLoading || jobsQuery.isLoading || runsQuery.isLoading) {
    return <LoadingSkeleton lines={6} />;
  }

  if (cohortsQuery.isError || jobsQuery.isError || runsQuery.isError) {
    return <ErrorBanner message="Unable to load one or more dashboard data feeds." />;
  }

  const activeJobs = jobsQuery.data?.filter((item) => item.status === 'running').length ?? 0;
  const completedRuns = runsQuery.data?.filter((item) => item.status === 'completed').length ?? 0;

  return (
    <>
      <PageHeader
        title="Dashboard"
        description="Operational overview for cohort generation, model execution, and analytic run activity."
      />

      <section className={styles.cardGrid} aria-label="Dashboard metrics">
        <article>
          <h2>Cohorts</h2>
          <p>{cohortsQuery.data?.length ?? 0}</p>
          <span>Registered cohort collections</span>
        </article>
        <article>
          <h2>Active Generation Jobs</h2>
          <p>{activeJobs}</p>
          <span>Jobs currently processing</span>
        </article>
        <article>
          <h2>Completed Runs</h2>
          <p>{completedRuns}</p>
          <span>Model runs with available outputs</span>
        </article>
      </section>

      <section className={styles.quickActions} aria-label="Quick actions">
        <h2>Quick Actions</h2>
        <ul>
          <li>
            <Link to="/cohorts/new">Create cohort generation intent</Link>
          </li>
          <li>
            <Link to="/cohorts">Inspect cohort inventory</Link>
          </li>
          <li>
            <Link to="/models">Inspect model schemas</Link>
          </li>
          <li>
            <Link to="/runs">Create run intent</Link>
          </li>
        </ul>
      </section>
    </>
  );
}
