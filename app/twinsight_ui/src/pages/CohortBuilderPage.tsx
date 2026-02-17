import { zodResolver } from '@hookform/resolvers/zod';
import { useForm } from 'react-hook-form';
import { PageHeader } from '../components/PageHeader';
import { PrimaryButton } from '../components/PrimaryButton';
import { CheckboxInput } from '../components/CheckboxInput';
import { SelectInput } from '../components/SelectInput';
import { TextInput } from '../components/TextInput';
import { JsonPreview } from '../components/JsonPreview';
import { ErrorBanner } from '../components/ErrorBanner';
import { cohortGenerationIntentSchema, type CohortGenerationIntent } from '../lib/contracts/schemas';
import { useCreateGenerationIntent } from '../features/cohorts/hooks';
import { useSettings } from '../app/settings-context';
import styles from './CohortBuilderPage.module.css';

const stateOptions = [
  { value: '', label: 'No state constraint' },
  { value: 'Massachusetts', label: 'Massachusetts' },
  { value: 'North Carolina', label: 'North Carolina' },
  { value: 'California', label: 'California' },
];

export function CohortBuilderPage() {
  const { serviceMode } = useSettings();
  const createMutation = useCreateGenerationIntent();
  const mutationErrorMessage =
    createMutation.error instanceof Error ? createMutation.error.message : 'Unable to queue generation intent.';

  const form = useForm<CohortGenerationIntent>({
    resolver: zodResolver(cohortGenerationIntentSchema),
    defaultValues: {
      cohortId: 'research-cohort-001',
      numPatients: 120,
      numYears: 5,
      minAge: 35,
      maxAge: 85,
      gender: 'both',
      state: '',
      city: '',
      usePopulationSampling: true,
    },
  });

  const onSubmit = form.handleSubmit(async (values) => {
    await createMutation.mutateAsync(values);
  });

  return (
    <>
      <PageHeader
        title="Cohort Builder"
        description="Create synthetic cohort generation intents with demographic constraints and audit-ready payload previews."
      />

      {createMutation.isError ? <ErrorBanner message={mutationErrorMessage} /> : null}

      <div className={styles.layout}>
        <form className={styles.form} onSubmit={onSubmit} noValidate>
          <TextInput
            id="cohortId"
            label="Cohort ID"
            {...form.register('cohortId')}
            error={form.formState.errors.cohortId?.message}
          />

          <TextInput
            id="numPatients"
            type="number"
            label="Number of Patients"
            {...form.register('numPatients', { valueAsNumber: true })}
            error={form.formState.errors.numPatients?.message}
          />

          <TextInput
            id="numYears"
            type="number"
            label="Years of History"
            {...form.register('numYears', { valueAsNumber: true })}
            error={form.formState.errors.numYears?.message}
          />

          <TextInput
            id="minAge"
            type="number"
            label="Minimum Age"
            {...form.register('minAge', { valueAsNumber: true })}
            error={form.formState.errors.minAge?.message}
          />

          <TextInput
            id="maxAge"
            type="number"
            label="Maximum Age"
            {...form.register('maxAge', { valueAsNumber: true })}
            error={form.formState.errors.maxAge?.message}
          />

          <SelectInput
            id="gender"
            label="Gender Distribution"
            options={[
              { value: 'both', label: 'Both' },
              { value: 'male', label: 'Male' },
              { value: 'female', label: 'Female' },
            ]}
            {...form.register('gender')}
            error={form.formState.errors.gender?.message}
          />

          <SelectInput
            id="state"
            label="State"
            options={stateOptions}
            {...form.register('state')}
            error={form.formState.errors.state?.message}
          />

          <TextInput id="city" label="City" {...form.register('city')} error={form.formState.errors.city?.message} />

          <CheckboxInput
            id="usePopulationSampling"
            label="Use population-weighted state sampling"
            checked={form.watch('usePopulationSampling')}
            onChange={(event) => form.setValue('usePopulationSampling', event.target.checked)}
          />

          <PrimaryButton type="submit" disabled={createMutation.isPending || serviceMode === 'direct'}>
            {createMutation.isPending ? 'Submitting...' : 'Create Generation Intent'}
          </PrimaryButton>

          {serviceMode === 'direct' ? (
            <p className={styles.hint}>Switch to mock mode to submit generation intents in this phase.</p>
          ) : null}
        </form>

        <aside className={styles.previewColumn}>
          <JsonPreview title="Submission Preview" value={form.watch()} />
          {createMutation.data ? (
            <JsonPreview title="Queued Job" value={createMutation.data} />
          ) : (
            <p className={styles.hint}>Submit the form to preview queued job metadata.</p>
          )}
        </aside>
      </div>
    </>
  );
}
