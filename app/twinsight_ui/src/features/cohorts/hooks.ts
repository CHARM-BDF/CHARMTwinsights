import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { useApiServices } from '../../app/api-services-context';
import type { CohortGenerationIntent } from '../../lib/contracts/schemas';

export function useCohorts() {
  const { cohortService } = useApiServices();
  return useQuery({
    queryKey: ['cohorts'],
    queryFn: () => cohortService.listCohorts(),
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
    queryFn: () => cohortService.listGenerationJobs(),
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
