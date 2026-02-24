import { useEffect, useState } from 'react';
import { useParams } from 'react-router-dom';
import { DataTable } from '../components/DataTable';
import { EmptyState } from '../components/EmptyState';
import { ErrorBanner } from '../components/ErrorBanner';
import { LoadingSkeleton } from '../components/LoadingSkeleton';
import { PageHeader } from '../components/PageHeader';
import { StatusBadge } from '../components/StatusBadge';
import { useCohortById } from '../features/cohorts/hooks';
import { usePatientsByCohort } from '../features/patients/hooks';
import styles from './CohortDetailPage.module.css';

const PAGE_SIZE = 20;

export function CohortDetailPage() {
  const { cohortId = '' } = useParams();
  const [currentPage, setCurrentPage] = useState(1);
  const cohortQuery = useCohortById(cohortId);
  const shouldLoadPatients = cohortQuery.isSuccess && Boolean(cohortQuery.data);
  const patientsQuery = usePatientsByCohort(cohortId, { enabled: shouldLoadPatients, page: currentPage, pageSize: PAGE_SIZE });
  const totalPatients = patientsQuery.data?.total ?? 0;
  const displayedPatientCount = patientsQuery.data?.total ?? cohortQuery.data?.patientCount ?? 0;
  const totalPages = Math.max(1, Math.ceil(totalPatients / PAGE_SIZE));
  const boundedPage = Math.max(1, Math.min(currentPage, totalPages));
  const pagedPatients = patientsQuery.data?.rows ?? [];
  const sliceStart = (boundedPage - 1) * PAGE_SIZE;
  const pageStartNumber = totalPatients === 0 ? 0 : sliceStart + 1;
  const pageEndNumber = Math.min(sliceStart + PAGE_SIZE, totalPatients);

  useEffect(() => {
    setCurrentPage(1);
  }, [cohortId]);

  useEffect(() => {
    if (currentPage > totalPages) {
      setCurrentPage(totalPages);
    }
  }, [currentPage, totalPages]);

  if (cohortQuery.isLoading || (shouldLoadPatients && patientsQuery.isLoading)) {
    return <LoadingSkeleton lines={6} />;
  }

  if (cohortQuery.isError) {
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
        description="Review cohort metadata and patient summaries."
      />

      <section className={styles.summaryRow} aria-label="Cohort summary">
        <div>
          <h2>Source</h2>
          <p>{cohortQuery.data.source}</p>
        </div>
        <div>
          <h2>Patient Count</h2>
          <p>{displayedPatientCount}</p>
        </div>
        <div>
          <h2>Status</h2>
          <StatusBadge status={cohortQuery.data.status} />
        </div>
      </section>

      {patientsQuery.isError ? (
        <ErrorBanner message="Unable to load patient summaries for this cohort. Cohort metadata is still available." />
      ) : null}

      <div className={styles.tableColumn}>
        <DataTable
          caption="Patients"
          rows={pagedPatients}
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
              render: (row) => {
                const given = row.givenName.trim();
                const family = row.familyName.trim();
                const fullName = `${given} ${family}`.trim();
                return fullName.length > 0 ? fullName : `Patient ${row.patientId}`;
              },
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

        <div className={styles.paginationRow} aria-label="Patient table pagination">
          <p>
            Showing {pageStartNumber}-{pageEndNumber} of {totalPatients} patients
          </p>

          <div className={styles.paginationControls}>
            <button type="button" onClick={() => setCurrentPage((value) => Math.max(1, value - 1))} disabled={boundedPage <= 1}>
              Previous
            </button>
            <span>
              Page {boundedPage} of {totalPages}
            </span>
            <button
              type="button"
              onClick={() => setCurrentPage((value) => Math.min(totalPages, value + 1))}
              disabled={boundedPage >= totalPages}
            >
              Next
            </button>
          </div>
        </div>
      </div>
    </>
  );
}
