import { useQuery } from '@tanstack/react-query';
import { useApiServices } from '../../app/api-services-context';

export function useCopilotStatus() {
  const { copilotService } = useApiServices();
  return useQuery({
    queryKey: ['copilot-status'],
    queryFn: () => copilotService.getCopilotStatus(),
  });
}

export function useCopilotMessages() {
  const { copilotService } = useApiServices();
  return useQuery({
    queryKey: ['copilot-messages'],
    queryFn: () => copilotService.listMessages(),
  });
}
