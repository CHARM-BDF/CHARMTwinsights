import { useQuery } from '@tanstack/react-query';
import { useApiServices } from '../../app/api-services-context';

export function usePatientsByCohort(cohortId: string) {
  const { patientService } = useApiServices();
  return useQuery({
    queryKey: ['patients-by-cohort', cohortId],
    queryFn: () => patientService.listPatientsByCohort(cohortId),
    enabled: Boolean(cohortId),
  });
}
