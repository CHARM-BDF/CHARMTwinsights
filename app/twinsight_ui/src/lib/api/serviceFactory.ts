import type { CohortGenerationIntent } from '../contracts/schemas';
import type { ModelRunRequest } from '../contracts/types';
import type { CohortService, CopilotService, ModelService, PatientService, RunService } from './interfaces';
import { endpointPathMap, getEndpointRegistry, type ServiceMode } from './endpointRegistry';
import { fetchJson } from './httpClient';
import { mapCohortSummary, mapGenerationJob, mapModelDescriptor, mapPatientSummary, mapRunRecord } from './dtoMappers';

export type ApiServices = {
  cohortService: CohortService;
  patientService: PatientService;
  modelService: ModelService;
  runService: RunService;
  copilotService: CopilotService;
};

function join(baseUrl: string, path: string) {
  if (path.startsWith('http')) {
    return path;
  }
  if (!baseUrl) {
    return path;
  }
  return `${baseUrl}${path}`;
}

function pathFor(mode: ServiceMode, key: keyof typeof endpointPathMap, cohortId?: string) {
  const template = endpointPathMap[key][mode];
  if (!cohortId) {
    return template;
  }
  return template.replace(':cohortId', cohortId);
}

function patientDetailPath(patientId: string) {
  return `/patients/${patientId}`;
}

function modelDetailPath(imageTag: string) {
  return `/models/${encodeURIComponent(imageTag)}`;
}

function runDetailPath(runId: string) {
  return `/runs/${runId}`;
}

function buildCohortService(mode: ServiceMode): CohortService {
  const registry = getEndpointRegistry(mode);

  return {
    async listCohorts() {
      const path = pathFor(mode, 'listCohorts');
      const response = await fetchJson<Record<string, unknown>>(join(registry.cohortsBaseUrl, path));
      const rows = (response.cohorts as Record<string, unknown>[]) ?? (response.data as Record<string, unknown>[]) ?? [];
      return rows.map(mapCohortSummary);
    },
    async getCohortById(cohortId) {
      const cohorts = await this.listCohorts();
      return cohorts.find((item) => item.cohortId === cohortId) ?? null;
    },
    async listGenerationJobs() {
      const path = pathFor(mode, 'listGenerationJobs');
      const response = await fetchJson<Record<string, unknown>>(join(registry.cohortsBaseUrl, path));
      const rows = (response.jobs as Record<string, unknown>[]) ?? (response.data as Record<string, unknown>[]) ?? (response as unknown as Record<string, unknown>[]);
      return rows.map(mapGenerationJob);
    },
    async createGenerationIntent(payload: CohortGenerationIntent) {
      if (mode === 'direct') {
        throw new Error('Direct mode is currently read-only. Switch to mock mode for generation intent creation.');
      }

      const path = pathFor(mode, 'createGenerationIntent');
      const response = await fetchJson<Record<string, unknown>>(join(registry.cohortsBaseUrl, path), {
        method: 'POST',
        body: JSON.stringify(payload),
      });
      return mapGenerationJob(response);
    },
  };
}

function buildPatientService(mode: ServiceMode): PatientService {
  const registry = getEndpointRegistry(mode);

  return {
    async listPatientsByCohort(cohortId) {
      const path = pathFor(mode, 'listPatientsByCohort', cohortId);
      const requestPath =
        mode === 'direct' ? `${path}?cohort_id=${encodeURIComponent(cohortId)}` : path;
      const response = await fetchJson<Record<string, unknown>>(join(registry.patientsBaseUrl, requestPath));
      const rows = (response.patients as Record<string, unknown>[]) ?? (response.data as Record<string, unknown>[]) ?? [];
      return rows.map(mapPatientSummary);
    },
    async getPatientRecord(patientId) {
      const response = await fetchJson<Record<string, unknown>>(
        join(registry.patientsBaseUrl, patientDetailPath(patientId)),
      );
      return {
        ...mapPatientSummary(response),
        conditions: (response.conditions as string[]) ?? [],
        observations: (response.observations as string[]) ?? [],
        medications: (response.medications as string[]) ?? [],
        lastEncounterDate: String(response.lastEncounterDate ?? response.last_encounter_date ?? 'unknown'),
      };
    },
  };
}

function buildModelService(mode: ServiceMode): ModelService {
  const registry = getEndpointRegistry(mode);

  return {
    async listModels() {
      const path = pathFor(mode, 'listModels');
      const response = await fetchJson<Record<string, unknown>>(join(registry.modelsBaseUrl, path));
      const rows = (response.models as Record<string, unknown>[]) ?? (response.data as Record<string, unknown>[]) ?? (response as unknown as Record<string, unknown>[]);
      return rows.map(mapModelDescriptor);
    },
    async getModelByImageTag(imageTag) {
      const response = await fetchJson<Record<string, unknown>>(
        join(registry.modelsBaseUrl, modelDetailPath(imageTag)),
      );
      return mapModelDescriptor(response);
    },
  };
}

function buildRunService(mode: ServiceMode): RunService {
  const registry = getEndpointRegistry(mode);

  return {
    async listRuns() {
      if (mode === 'direct') {
        return [];
      }

      const path = pathFor(mode, 'listRuns');
      const response = await fetchJson<Record<string, unknown>>(join(registry.runsBaseUrl, path));
      const rows = (response.runs as Record<string, unknown>[]) ?? (response.data as Record<string, unknown>[]) ?? [];
      return rows.map(mapRunRecord);
    },
    async getRunById(runId) {
      if (mode === 'direct') {
        return null;
      }

      const response = await fetchJson<Record<string, unknown>>(
        join(registry.runsBaseUrl, runDetailPath(runId)),
      );
      return mapRunRecord(response);
    },
    async createRunIntent(payload: ModelRunRequest) {
      if (mode === 'direct') {
        throw new Error('Direct mode is currently read-only. Switch to mock mode for run intent creation.');
      }

      const path = pathFor(mode, 'createRunIntent');
      const response = await fetchJson<Record<string, unknown>>(join(registry.runsBaseUrl, path), {
        method: 'POST',
        body: JSON.stringify(payload),
      });
      return mapRunRecord(response);
    },
  };
}

function buildCopilotService(mode: ServiceMode): CopilotService {
  const registry = getEndpointRegistry(mode);

  return {
    async getCopilotStatus() {
      const path = pathFor(mode, 'copilotStatus');
      const response = await fetchJson<Record<string, unknown>>(join(registry.copilotBaseUrl, path));
      return {
        enabled: Boolean(response.enabled),
        transport: (response.transport as 'mcp-http' | 'disabled') ?? 'disabled',
        endpoint: String(response.endpoint ?? registry.copilotBaseUrl),
      };
    },
    async listMessages() {
      if (mode === 'direct') {
        return [];
      }

      const path = pathFor(mode, 'copilotMessages');
      const response = await fetchJson<Record<string, unknown>>(join(registry.copilotBaseUrl, path));
      return ((response.messages as Array<Record<string, unknown>>) ?? []).map((item) => ({
        id: String(item.id ?? 'unknown'),
        role: (item.role as 'assistant' | 'user' | 'system') ?? 'assistant',
        content: String(item.content ?? ''),
        createdAt: String(item.createdAt ?? new Date().toISOString()),
      }));
    },
  };
}

export function createApiServices(mode: ServiceMode): ApiServices {
  return {
    cohortService: buildCohortService(mode),
    patientService: buildPatientService(mode),
    modelService: buildModelService(mode),
    runService: buildRunService(mode),
    copilotService: buildCopilotService(mode),
  };
}
