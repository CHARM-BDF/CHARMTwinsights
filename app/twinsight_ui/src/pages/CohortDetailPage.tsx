import { useMemo } from 'react';
import { useParams } from 'react-router-dom';
import { ActivityTimeline } from '../components/ActivityTimeline';
import { DataTable } from '../components/DataTable';
import { EmptyState } from '../components/EmptyState';
import { ErrorBanner } from '../components/ErrorBanner';
import { LoadingSkeleton } from '../components/LoadingSkeleton';
import { PageHeader } from '../components/PageHeader';
import { StatusBadge } from '../components/StatusBadge';
import { useCohortById } from '../features/cohorts/hooks';
import { usePatientsByCohort } from '../features/patients/hooks';
import styles from './CohortDetailPage.module.css';

export function CohortDetailPage() {
  const { cohortId = '' } = useParams();
  const cohortQuery = useCohortById(cohortId);
  const patientsQuery = usePatientsByCohort(cohortId);

  const timelineItems = useMemo(
    () => [
      {
        id: 'activity-1',
        title: 'Cohort created',
        timestamp: cohortQuery.data?.createdAt ?? new Date().toISOString(),
        detail: 'Cohort metadata registered with source and initial patient count.',
      },
      {
        id: 'activity-2',
        title: 'Patient table synchronized',
        timestamp: new Date().toISOString(),
        detail: 'Structured patient summaries refreshed for current cohort context.',
      },
    ],
    [cohortQuery.data?.createdAt],
  );

  if (cohortQuery.isLoading || patientsQuery.isLoading) {
    return <LoadingSkeleton lines={6} />;
  }

  if (cohortQuery.isError || patientsQuery.isError) {
    return <ErrorBanner message="Unable to load cohort details." />;
  }

  if (!cohortQuery.data) {
    return (
      <EmptyState
        title="Cohort not found"
        description="The requested cohort ID does not exist in the current data source."
      />
    );
  }

  return (
    <>
      <PageHeader
        title={`Cohort Detail: ${cohortQuery.data.cohortId}`}
        description="Review cohort metadata, patient summaries, and recent workflow events."
      />

      <section className={styles.summaryRow} aria-label="Cohort summary">
        <div>
          <h2>Source</h2>
          <p>{cohortQuery.data.source}</p>
        </div>
        <div>
          <h2>Patient Count</h2>
          <p>{cohortQuery.data.patientCount}</p>
        </div>
        <div>
          <h2>Status</h2>
          <StatusBadge status={cohortQuery.data.status} />
        </div>
      </section>

      <div className={styles.layout}>
        <DataTable
          caption="Patients"
          rows={patientsQuery.data ?? []}
          rowKey={(row) => row.patientId}
          columns={[
            {
              key: 'patientId',
              header: 'Patient ID',
              render: (row) => row.patientId,
            },
            {
              key: 'name',
              header: 'Name',
              render: (row) => `${row.givenName} ${row.familyName}`,
            },
            {
              key: 'gender',
              header: 'Gender',
              render: (row) => row.gender,
            },
            {
              key: 'birthDate',
              header: 'Birth Date',
              render: (row) => row.birthDate,
            },
          ]}
        />

        <ActivityTimeline items={timelineItems} />
      </div>
    </>
  );
}
