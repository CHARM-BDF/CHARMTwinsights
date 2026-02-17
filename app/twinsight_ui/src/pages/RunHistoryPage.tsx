import { zodResolver } from '@hookform/resolvers/zod';
import { useForm } from 'react-hook-form';
import { DataTable } from '../components/DataTable';
import { ErrorBanner } from '../components/ErrorBanner';
import { FilterPanel } from '../components/FilterPanel';
import { LoadingSkeleton } from '../components/LoadingSkeleton';
import { PageHeader } from '../components/PageHeader';
import { PrimaryButton } from '../components/PrimaryButton';
import { RunResultPanel } from '../components/RunResultPanel';
import { SelectInput } from '../components/SelectInput';
import { StatusBadge } from '../components/StatusBadge';
import { TextInput } from '../components/TextInput';
import { useModels } from '../features/models/hooks';
import { useCreateRunIntent, useRuns } from '../features/runs/hooks';
import { modelRunIntentSchema, type ModelRunIntent } from '../lib/contracts/schemas';
import { useSettings } from '../app/settings-context';
import styles from './RunHistoryPage.module.css';

export function RunHistoryPage() {
  const { serviceMode } = useSettings();
  const runsQuery = useRuns();
  const modelsQuery = useModels();
  const createRunMutation = useCreateRunIntent();
  const mutationErrorMessage =
    createRunMutation.error instanceof Error ? createRunMutation.error.message : 'Unable to queue run intent.';

  const form = useForm<ModelRunIntent>({
    resolver: zodResolver(modelRunIntentSchema),
    defaultValues: {
      imageTag: 'coxcopdmodel:latest',
      scope: 'cohort',
      cohortId: 'copd-risk-2026-q1',
      patientId: '',
      input: [{ cohort_id: 'copd-risk-2026-q1' }],
    },
  });

  const onSubmit = form.handleSubmit(async (values) => {
    await createRunMutation.mutateAsync(values);
  });

  const selectedRun = runsQuery.data?.[0] ?? null;

  return (
    <>
      <PageHeader
        title="Run History"
        description="Track model runs, inspect result previews, and create new execution intents from validated payloads."
      />

      {createRunMutation.isError ? <ErrorBanner message={mutationErrorMessage} /> : null}

      <FilterPanel title="Create Run Intent">
        <form className={styles.intentForm} onSubmit={onSubmit}>
          <SelectInput
            id="imageTag"
            label="Model Image"
            options={(modelsQuery.data ?? []).map((model) => ({ value: model.imageTag, label: model.title }))}
            {...form.register('imageTag')}
            error={form.formState.errors.imageTag?.message}
          />

          <SelectInput
            id="scope"
            label="Scope"
            options={[
              { value: 'cohort', label: 'Cohort' },
              { value: 'single-patient', label: 'Single Patient' },
            ]}
            {...form.register('scope')}
            error={form.formState.errors.scope?.message}
          />

          <TextInput id="cohortId" label="Cohort ID" {...form.register('cohortId')} />
          <TextInput id="patientId" label="Patient ID" {...form.register('patientId')} />

          <PrimaryButton type="submit" disabled={createRunMutation.isPending || serviceMode === 'direct'}>
            {createRunMutation.isPending ? 'Submitting...' : 'Create Run Intent'}
          </PrimaryButton>

          {serviceMode === 'direct' ? (
            <p className={styles.modeHint}>Switch to mock mode to submit run intents in this phase.</p>
          ) : null}
        </form>
      </FilterPanel>

      {runsQuery.isLoading ? <LoadingSkeleton lines={5} /> : null}
      {runsQuery.isError ? <ErrorBanner message="Unable to load run records." /> : null}

      {!runsQuery.isLoading && !runsQuery.isError ? (
        <DataTable
          caption="Run Records"
          rows={runsQuery.data ?? []}
          rowKey={(row) => row.runId}
          columns={[
            { key: 'runId', header: 'Run ID', render: (row) => row.runId },
            { key: 'modelTitle', header: 'Model', render: (row) => row.modelTitle },
            { key: 'scope', header: 'Scope', render: (row) => row.scope },
            { key: 'status', header: 'Status', render: (row) => <StatusBadge status={row.status} /> },
            {
              key: 'createdAt',
              header: 'Created',
              render: (row) => new Date(row.createdAt).toLocaleString(),
            },
          ]}
        />
      ) : null}

      {selectedRun ? <RunResultPanel run={selectedRun} /> : null}
    </>
  );
}
