import type {
  CohortSummary,
  GenerationJob,
  ModelDescriptor,
  ModelRunRecord,
  PatientSummary,
} from '../contracts/types';

export function mapCohortSummary(input: Record<string, unknown>): CohortSummary {
  return {
    cohortId: String(input.cohortId ?? input.cohort_id ?? 'unknown'),
    patientCount: Number(input.patientCount ?? input.patient_count ?? 0),
    source: (input.source as CohortSummary['source']) ?? 'synthetic',
    createdAt: String(input.createdAt ?? input.created_at ?? new Date().toISOString()),
    status: (input.status as CohortSummary['status']) ?? 'active',
  };
}

export function mapGenerationJob(input: Record<string, unknown>): GenerationJob {
  const rawEta = input.estimatedRemainingSeconds ?? input.estimated_remaining_seconds;
  return {
    jobId: String(input.jobId ?? input.job_id ?? 'unknown'),
    cohortId: String(input.cohortId ?? input.cohort_id ?? 'unknown'),
    status: (input.status as GenerationJob['status']) ?? 'queued',
    progress: Number(input.progress ?? 0),
    currentPhase: String(input.currentPhase ?? input.current_phase ?? 'queued'),
    createdAt: String(input.createdAt ?? input.created_at ?? new Date().toISOString()),
    estimatedRemainingSeconds: rawEta === null || rawEta === undefined ? null : Number(rawEta),
  };
}

export function mapPatientSummary(input: Record<string, unknown>): PatientSummary {
  return {
    patientId: String(input.patientId ?? input.patient_id ?? input.id ?? 'unknown'),
    givenName: String(input.givenName ?? input.given_name ?? 'Unknown'),
    familyName: String(input.familyName ?? input.family_name ?? 'Unknown'),
    gender: String(input.gender ?? 'unknown'),
    birthDate: String(input.birthDate ?? input.birth_date ?? 'unknown'),
    cohortId: String(input.cohortId ?? input.cohort_id ?? 'unknown'),
  };
}

export function mapModelDescriptor(input: Record<string, unknown>): ModelDescriptor {
  return {
    imageTag: String(input.imageTag ?? input.image ?? 'unknown:latest'),
    title: String(input.title ?? 'Unknown Model'),
    shortDescription: String(input.shortDescription ?? input.short_description ?? 'No description available'),
    authors: String(input.authors ?? 'Unknown'),
    examples: (input.examples as Record<string, unknown>[]) ?? [],
    inputSchema: {
      className: String((input.inputSchema as Record<string, unknown>)?.className ?? 'InputRecord'),
      fields: ((input.inputSchema as Record<string, unknown>)?.fields as ModelDescriptor['inputSchema']['fields']) ?? [],
    },
    outputSchema: {
      className: String((input.outputSchema as Record<string, unknown>)?.className ?? 'OutputRecord'),
      fields: ((input.outputSchema as Record<string, unknown>)?.fields as ModelDescriptor['outputSchema']['fields']) ?? [],
    },
  };
}

export function mapRunRecord(input: Record<string, unknown>): ModelRunRecord {
  return {
    runId: String(input.runId ?? input.run_id ?? 'unknown'),
    imageTag: String(input.imageTag ?? input.image ?? 'unknown:latest'),
    modelTitle: String(input.modelTitle ?? input.model_title ?? 'Unknown model'),
    scope: (input.scope as ModelRunRecord['scope']) ?? 'cohort',
    status: (input.status as ModelRunRecord['status']) ?? 'queued',
    createdAt: String(input.createdAt ?? input.created_at ?? new Date().toISOString()),
    resultPreview: String(input.resultPreview ?? input.result_preview ?? 'Pending'),
    inputSnapshot: (input.inputSnapshot as Record<string, unknown>[]) ?? [],
    outputSnapshot: (input.outputSnapshot as Record<string, unknown>[]) ?? [],
  };
}
