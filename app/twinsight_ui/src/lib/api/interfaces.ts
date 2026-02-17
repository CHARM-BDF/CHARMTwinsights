import type {
  CohortSummary,
  CopilotMessage,
  CopilotStatus,
  GenerationJob,
  ModelDescriptor,
  ModelRunRecord,
  ModelRunRequest,
  PatientRecord,
  PatientSummary,
} from '../contracts/types';
import type { CohortGenerationIntent } from '../contracts/schemas';

export type CohortService = {
  listCohorts: () => Promise<CohortSummary[]>;
  getCohortById: (cohortId: string) => Promise<CohortSummary | null>;
  listGenerationJobs: () => Promise<GenerationJob[]>;
  createGenerationIntent: (payload: CohortGenerationIntent) => Promise<GenerationJob>;
};

export type PatientService = {
  listPatientsByCohort: (cohortId: string) => Promise<PatientSummary[]>;
  getPatientRecord: (patientId: string) => Promise<PatientRecord | null>;
};

export type ModelService = {
  listModels: () => Promise<ModelDescriptor[]>;
  getModelByImageTag: (imageTag: string) => Promise<ModelDescriptor | null>;
};

export type RunService = {
  listRuns: () => Promise<ModelRunRecord[]>;
  getRunById: (runId: string) => Promise<ModelRunRecord | null>;
  createRunIntent: (payload: ModelRunRequest) => Promise<ModelRunRecord>;
};

export type CopilotService = {
  getCopilotStatus: () => Promise<CopilotStatus>;
  listMessages: () => Promise<CopilotMessage[]>;
};
