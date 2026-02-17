import { useMemo, useState } from 'react';
import { Link } from 'react-router-dom';
import { DataTable } from '../components/DataTable';
import { ErrorBanner } from '../components/ErrorBanner';
import { FilterPanel } from '../components/FilterPanel';
import { LoadingSkeleton } from '../components/LoadingSkeleton';
import { PageHeader } from '../components/PageHeader';
import { SelectInput } from '../components/SelectInput';
import { StatusBadge } from '../components/StatusBadge';
import { TextInput } from '../components/TextInput';
import { useCohorts } from '../features/cohorts/hooks';

export function CohortExplorerPage() {
  const { data, isLoading, isError } = useCohorts();
  const [nameFilter, setNameFilter] = useState('');
  const [sourceFilter, setSourceFilter] = useState('all');

  const filtered = useMemo(() => {
    return (data ?? []).filter((cohort) => {
      const matchesName = cohort.cohortId.toLowerCase().includes(nameFilter.toLowerCase());
      const matchesSource = sourceFilter === 'all' || cohort.source === sourceFilter;
      return matchesName && matchesSource;
    });
  }, [data, nameFilter, sourceFilter]);

  return (
    <>
      <PageHeader
        title="Cohort Explorer"
        description="Review cohort inventory, filter by source, and drill down to patient-level context."
      />

      <FilterPanel title="Cohort Filters">
        <TextInput
          id="cohortNameFilter"
          label="Filter by cohort ID"
          value={nameFilter}
          onChange={(event) => setNameFilter(event.target.value)}
        />
        <SelectInput
          id="sourceFilter"
          label="Source"
          value={sourceFilter}
          onChange={(event) => setSourceFilter(event.target.value)}
          options={[
            { value: 'all', label: 'All sources' },
            { value: 'synthetic', label: 'Synthetic' },
            { value: 'external', label: 'External' },
            { value: 'mixed', label: 'Mixed' },
          ]}
        />
      </FilterPanel>

      {isLoading ? <LoadingSkeleton lines={5} /> : null}
      {isError ? <ErrorBanner message="Unable to load cohort inventory." /> : null}

      {!isLoading && !isError ? (
        <DataTable
          caption="Cohort Inventory"
          rows={filtered}
          rowKey={(row) => row.cohortId}
          columns={[
            {
              key: 'cohortId',
              header: 'Cohort ID',
              render: (row) => <Link to={`/cohorts/${row.cohortId}`}>{row.cohortId}</Link>,
            },
            {
              key: 'source',
              header: 'Source',
              render: (row) => row.source,
            },
            {
              key: 'patients',
              header: 'Patients',
              render: (row) => row.patientCount,
            },
            {
              key: 'status',
              header: 'Status',
              render: (row) => <StatusBadge status={row.status} />,
            },
            {
              key: 'createdAt',
              header: 'Created',
              render: (row) => new Date(row.createdAt).toLocaleDateString(),
            },
          ]}
        />
      ) : null}
    </>
  );
}
