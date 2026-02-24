import { useQuery } from '@tanstack/react-query';
import { endpointPathMap, getEndpointRegistry, type ServiceMode } from '../../lib/api/endpointRegistry';

export type ConnectionStatus = 'healthy' | 'degraded' | 'unreachable';

export type ConnectionCheck = {
  id: string;
  label: string;
  endpoint: string;
  status: ConnectionStatus;
  detail: string;
  latencyMs: number | null;
};

type ConnectionTarget = {
  id: string;
  label: string;
  endpoint: string;
  kind: 'health' | 'read';
};

function join(baseUrl: string, path: string) {
  return `${baseUrl}${path}`;
}

function buildTargets(mode: ServiceMode): ConnectionTarget[] {
  if (mode === 'mock') {
    return [
      {
        id: 'mock-cohorts',
        label: 'Mock Cohort Feed',
        endpoint: '/mock-api/cohorts',
        kind: 'read',
      },
      {
        id: 'mock-models',
        label: 'Mock Model Feed',
        endpoint: '/mock-api/models',
        kind: 'read',
      },
    ];
  }

  const registry = getEndpointRegistry(mode);

  return [
    {
      id: 'direct-router-health',
      label: 'Router API',
      endpoint: join(registry.cohortsBaseUrl, '/health'),
      kind: 'health',
    },
    {
      id: 'direct-cohort-feed',
      label: 'Cohort Feed',
      endpoint: join(registry.cohortsBaseUrl, endpointPathMap.listCohorts.direct),
      kind: 'read',
    },
    {
      id: 'direct-patient-feed',
      label: 'Patient Feed',
      endpoint: join(registry.patientsBaseUrl, endpointPathMap.listPatientsByCohort.direct),
      kind: 'read',
    },
    {
      id: 'direct-model-feed',
      label: 'Model Feed',
      endpoint: join(registry.modelsBaseUrl, endpointPathMap.listModels.direct),
      kind: 'read',
    },
  ];
}

function describeReadPayload(payload: unknown): string {
  if (Array.isArray(payload)) {
    return `${payload.length} records`;
  }

  if (typeof payload !== 'object' || payload === null) {
    return 'reachable';
  }

  const data = payload as Record<string, unknown>;
  if (Array.isArray(data.cohorts)) {
    return `${data.cohorts.length} cohorts`;
  }
  if (Array.isArray(data.models)) {
    return `${data.models.length} models`;
  }
  if (Array.isArray(data.patients)) {
    return `${data.patients.length} patients`;
  }

  return 'reachable';
}

async function checkTarget(target: ConnectionTarget): Promise<ConnectionCheck> {
  const startedAt = performance.now();
  const timeoutMs = 4000;

  try {
    const response = (await Promise.race([
      fetch(target.endpoint, {
        method: 'GET',
      }),
      new Promise<Response>((_, reject) => {
        setTimeout(() => reject(new Error('request timed out')), timeoutMs);
      }),
    ])) as Response;

    const latencyMs = Math.round(performance.now() - startedAt);

    if (!response.ok) {
      return {
        id: target.id,
        label: target.label,
        endpoint: target.endpoint,
        status: 'unreachable',
        detail: `HTTP ${response.status}`,
        latencyMs,
      };
    }

    const payload = (await response.json().catch(() => null)) as unknown;

    if (target.kind === 'health') {
      const serviceStatus =
        payload && typeof payload === 'object' && 'status' in payload
          ? String((payload as Record<string, unknown>).status)
          : 'healthy';

      return {
        id: target.id,
        label: target.label,
        endpoint: target.endpoint,
        status: serviceStatus.toLowerCase() === 'healthy' ? 'healthy' : 'degraded',
        detail: `HTTP ${response.status}; ${serviceStatus}`,
        latencyMs,
      };
    }

    return {
      id: target.id,
      label: target.label,
      endpoint: target.endpoint,
      status: 'healthy',
      detail: `HTTP ${response.status}; ${describeReadPayload(payload)}`,
      latencyMs,
    };
  } catch (error) {
    const latencyMs = Math.round(performance.now() - startedAt);
    const detail = error instanceof Error ? error.message : 'request failed';

    return {
      id: target.id,
      label: target.label,
      endpoint: target.endpoint,
      status: 'unreachable',
      detail,
      latencyMs,
    };
  }
}

async function getConnectionHealth(mode: ServiceMode): Promise<ConnectionCheck[]> {
  const targets = buildTargets(mode);
  const checks = await Promise.all(targets.map((target) => checkTarget(target)));
  return checks;
}

export function useConnectionHealth(mode: ServiceMode) {
  return useQuery({
    queryKey: ['connection-health', mode],
    queryFn: () => getConnectionHealth(mode),
    refetchInterval: 30_000,
  });
}
