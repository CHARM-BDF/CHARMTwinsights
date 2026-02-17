import { z } from 'zod';

export const cohortGenerationIntentSchema = z
  .object({
    cohortId: z
      .string()
      .min(1, 'Cohort identifier is required')
      .max(64, 'Cohort identifier must be 64 characters or fewer')
      .regex(/^[A-Za-z0-9\-.]+$/, 'Use letters, numbers, hyphens, and periods only'),
    numPatients: z.number().int().min(1).max(5000),
    numYears: z.number().int().min(1).max(100),
    minAge: z.number().int().min(0).max(140),
    maxAge: z.number().int().min(0).max(140),
    gender: z.enum(['both', 'male', 'female']),
    state: z.string().optional(),
    city: z.string().optional(),
    usePopulationSampling: z.boolean(),
  })
  .refine((value) => value.maxAge >= value.minAge, {
    message: 'Maximum age must be greater than or equal to minimum age',
    path: ['maxAge'],
  });

export const modelRunIntentSchema = z.object({
  imageTag: z.string().min(1),
  scope: z.enum(['single-patient', 'cohort']),
  cohortId: z.string().optional(),
  patientId: z.string().optional(),
  input: z.array(z.record(z.unknown())).min(1),
});

export type CohortGenerationIntent = z.infer<typeof cohortGenerationIntentSchema>;
export type ModelRunIntent = z.infer<typeof modelRunIntentSchema>;
