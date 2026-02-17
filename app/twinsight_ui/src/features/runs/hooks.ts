import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { useApiServices } from '../../app/api-services-context';
import type { ModelRunRequest } from '../../lib/contracts/types';

export function useRuns() {
  const { runService } = useApiServices();
  return useQuery({
    queryKey: ['runs'],
    queryFn: () => runService.listRuns(),
  });
}

export function useRunById(runId: string | null) {
  const { runService } = useApiServices();
  return useQuery({
    queryKey: ['run', runId],
    queryFn: () => runService.getRunById(runId ?? ''),
    enabled: Boolean(runId),
  });
}

export function useCreateRunIntent() {
  const queryClient = useQueryClient();
  const { runService } = useApiServices();

  return useMutation({
    mutationFn: (payload: ModelRunRequest) => runService.createRunIntent(payload),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: ['runs'] });
    },
  });
}
