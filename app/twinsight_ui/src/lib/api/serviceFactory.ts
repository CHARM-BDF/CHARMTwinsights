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

function patientDetailPath(mode: ServiceMode, patientId: string) {
  if (mode === 'router') {
    return `/stats/patients/${patientId}`;
  }
  return `/patients/${patientId}`;
}

function modelDetailPath(mode: ServiceMode, imageTag: string) {
  if (mode === 'router') {
    return `/modeling/models/${encodeURIComponent(imageTag)}`;
  }
  return `/models/${encodeURIComponent(imageTag)}`;
}

function runDetailPath(mode: ServiceMode, runId: string) {
  if (mode === 'router' || mode === 'hybrid') {
    return `/modeling/runs/${runId}`;
  }
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
      if (mode !== 'mock') {
        const basePath = mode === 'router' ? '/stats/patients' : '/patients';
        const response = await fetchJson<Record<string, unknown>>(
          join(registry.patientsBaseUrl, `${basePath}?cohort_id=${encodeURIComponent(cohortId)}`),
        );
        const rows =
          (response.patients as Record<string, unknown>[]) ??
          (response.data as Record<string, unknown>[]) ??
          [];
        return rows.map(mapPatientSummary);
      }

      const path = pathFor('mock', 'listPatientsByCohort', cohortId);
      const response = await fetchJson<Record<string, unknown>>(join(registry.patientsBaseUrl, path));
      const rows = (response.patients as Record<string, unknown>[]) ?? (response.data as Record<string, unknown>[]) ?? [];
      return rows.map(mapPatientSummary);
    },
    async getPatientRecord(patientId) {
      const response = await fetchJson<Record<string, unknown>>(
        join(registry.patientsBaseUrl, patientDetailPath(mode, patientId)),
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
        join(registry.modelsBaseUrl, modelDetailPath(mode, imageTag)),
      );
      return mapModelDescriptor(response);
    },
  };
}

function buildRunService(mode: ServiceMode): RunService {
  const registry = getEndpointRegistry(mode);

  return {
    async listRuns() {
      const path = pathFor(mode, 'listRuns');
      const response = await fetchJson<Record<string, unknown>>(join(registry.runsBaseUrl, path));
      const rows = (response.runs as Record<string, unknown>[]) ?? (response.data as Record<string, unknown>[]) ?? [];
      return rows.map(mapRunRecord);
    },
    async getRunById(runId) {
      const response = await fetchJson<Record<string, unknown>>(
        join(registry.runsBaseUrl, runDetailPath(mode, runId)),
      );
      return mapRunRecord(response);
    },
    async createRunIntent(payload: ModelRunRequest) {
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
