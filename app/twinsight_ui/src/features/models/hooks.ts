import { useQuery } from '@tanstack/react-query';
import { useApiServices } from '../../app/api-services-context';

export function useModels() {
  const { modelService } = useApiServices();
  return useQuery({
    queryKey: ['models'],
    queryFn: () => modelService.listModels(),
  });
}

export function useModelByImageTag(imageTag: string | null) {
  const { modelService } = useApiServices();
  return useQuery({
    queryKey: ['model', imageTag],
    queryFn: () => modelService.getModelByImageTag(imageTag ?? ''),
    enabled: Boolean(imageTag),
  });
}
