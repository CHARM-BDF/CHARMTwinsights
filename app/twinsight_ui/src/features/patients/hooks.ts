import { useQuery } from '@tanstack/react-query';
import { useApiServices } from '../../app/api-services-context';

type UsePatientsByCohortOptions = {
  enabled?: boolean;
  page?: number;
  pageSize?: number;
};

export function usePatientsByCohort(cohortId: string, options?: UsePatientsByCohortOptions) {
  const { patientService } = useApiServices();
  const page = Math.max(1, options?.page ?? 1);
  const pageSize = Math.max(1, options?.pageSize ?? 20);
  const offset = (page - 1) * pageSize;

  return useQuery({
    queryKey: ['patients-by-cohort', cohortId, page, pageSize],
    queryFn: () => patientService.listPatientsByCohort(cohortId, { limit: pageSize, offset }),
    enabled: Boolean(cohortId) && (options?.enabled ?? true),
  });
}
