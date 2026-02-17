export type StatusValue = 'queued' | 'running' | 'completed' | 'failed' | 'cancelled';

export type CohortSummary = {
  cohortId: string;
  patientCount: number;
  source: 'synthetic' | 'external' | 'mixed';
  createdAt: string;
  status: 'active' | 'archived';
};

export type GenerationJob = {
  jobId: string;
  cohortId: string;
  status: StatusValue;
  progress: number;
  currentPhase: string;
  createdAt: string;
  estimatedRemainingSeconds: number | null;
};

export type PatientSummary = {
  patientId: string;
  givenName: string;
  familyName: string;
  gender: string;
  birthDate: string;
  cohortId: string;
};

export type PatientRecord = PatientSummary & {
  conditions: string[];
  observations: string[];
  medications: string[];
  lastEncounterDate: string;
};

export type ModelSchemaField = {
  name: string;
  range: string;
  required: boolean;
  description: string;
  enumValues?: string[];
};

export type ModelSchemaInfo = {
  className: string;
  fields: ModelSchemaField[];
};

export type ModelDescriptor = {
  imageTag: string;
  title: string;
  shortDescription: string;
  authors: string;
  examples: Record<string, unknown>[];
  inputSchema: ModelSchemaInfo;
  outputSchema: ModelSchemaInfo;
};

export type ModelRunRequest = {
  imageTag: string;
  scope: 'single-patient' | 'cohort';
  cohortId?: string;
  patientId?: string;
  input: Record<string, unknown>[];
};

export type ModelRunRecord = {
  runId: string;
  imageTag: string;
  modelTitle: string;
  scope: 'single-patient' | 'cohort';
  status: StatusValue;
  createdAt: string;
  resultPreview: string;
  inputSnapshot: Record<string, unknown>[];
  outputSnapshot: Record<string, unknown>[];
};

export type CopilotMessage = {
  id: string;
  role: 'assistant' | 'user' | 'system';
  content: string;
  createdAt: string;
};

export type CopilotStatus = {
  enabled: boolean;
  transport: 'mcp-http' | 'disabled';
  endpoint: string;
};
