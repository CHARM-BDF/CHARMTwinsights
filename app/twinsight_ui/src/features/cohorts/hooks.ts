import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { useApiServices } from '../../app/api-services-context';
import type { CohortGenerationIntent } from '../../lib/contracts/schemas';
import type { GenerationJob } from '../../lib/contracts/types';

const JOB_CACHE_KEY = 'twinsight_ui_generation_jobs_cache';

function loadCachedJobs(): GenerationJob[] {
  if (typeof window === 'undefined') {
    return [];
  }

  const raw = window.localStorage.getItem(JOB_CACHE_KEY);
  if (!raw) {
    return [];
  }

  try {
    const parsed = JSON.parse(raw) as unknown;
    return Array.isArray(parsed) ? (parsed as GenerationJob[]) : [];
  } catch {
    return [];
  }
}

function storeCachedJobs(rows: GenerationJob[]) {
  if (typeof window === 'undefined') {
    return;
  }
  window.localStorage.setItem(JOB_CACHE_KEY, JSON.stringify(rows));
}

export function useCohorts() {
  const { cohortService } = useApiServices();
  return useQuery({
    queryKey: ['cohorts'],
    queryFn: () => cohortService.listCohorts(),
    retry: 0,
  });
}

export function useCohortById(cohortId: string) {
  const { cohortService } = useApiServices();
  return useQuery({
    queryKey: ['cohort', cohortId],
    queryFn: () => cohortService.getCohortById(cohortId),
    enabled: Boolean(cohortId),
  });
}

export function useGenerationJobs() {
  const { cohortService } = useApiServices();
  return useQuery({
    queryKey: ['generation-jobs'],
    queryFn: async () => {
      const rows = await cohortService.listGenerationJobs();
      storeCachedJobs(rows);
      return rows;
    },
    initialData: loadCachedJobs,
    placeholderData: (previousData) => previousData,
    refetchInterval: 5_000,
    refetchOnMount: 'always',
    retry: 3,
  });
}

export function useCreateGenerationIntent() {
  const queryClient = useQueryClient();
  const { cohortService } = useApiServices();

  return useMutation({
    mutationFn: (payload: CohortGenerationIntent) => cohortService.createGenerationIntent(payload),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: ['generation-jobs'] });
      await queryClient.invalidateQueries({ queryKey: ['cohorts'] });
    },
  });
}
