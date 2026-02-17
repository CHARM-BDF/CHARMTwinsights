import type { ModelDescriptor } from '../../lib/contracts/types';

export const modelFixtures: ModelDescriptor[] = [
  {
    imageTag: 'coxcopdmodel:latest',
    title: 'COPD Survival Risk (Cox PH)',
    shortDescription: 'Estimates partial hazard and 5-year survival probability for COPD risk profiles.',
    authors: 'Lakshmi Anandan',
    examples: [
      {
        ethnicity: 'Not Hispanic or Latino',
        sex_at_birth: 'Female',
        obesity: 0,
        diabetes: 0,
        cardiovascular_disease: 0,
        smoking_status: 0,
        alcohol_use: 0,
        bmi: 25,
        age_at_time_0: 50,
      },
    ],
    inputSchema: {
      className: 'CoxCOPDInputItem',
      fields: [
        {
          name: 'ethnicity',
          range: 'EthnicityEnum',
          required: true,
          description: 'Patient ethnicity',
          enumValues: ['Not Hispanic or Latino', 'Hispanic or Latino'],
        },
        {
          name: 'sex_at_birth',
          range: 'SexAtBirthEnum',
          required: true,
          description: 'Biological sex at birth',
          enumValues: ['Female', 'Male'],
        },
        { name: 'bmi', range: 'float', required: true, description: 'Body mass index (kg/m2)' },
        { name: 'age_at_time_0', range: 'float', required: true, description: 'Age at baseline' },
      ],
    },
    outputSchema: {
      className: 'CoxCOPDOutputItem',
      fields: [
        {
          name: 'partial_hazard',
          range: 'float',
          required: false,
          description: 'Relative hazard score',
        },
        {
          name: 'survival_probability_5_years',
          range: 'float',
          required: false,
          description: 'Predicted survival at 5 years',
        },
      ],
    },
  },
  {
    imageTag: 'reachablefrommodel:latest',
    title: 'Ontology Reachable-From Demo',
    shortDescription: 'Demonstrates enum expansion behavior from ontology sources.',
    authors: 'CHARMTwinsight Team',
    examples: [{ biological_sex: 'PATO:0000383', age_years: 34 }],
    inputSchema: {
      className: 'InputRecord',
      fields: [
        {
          name: 'biological_sex',
          range: 'BiologicalSexEnum',
          required: true,
          description: 'Biological sex concept identifier',
        },
        { name: 'age_years', range: 'integer', required: true, description: 'Age in years' },
      ],
    },
    outputSchema: {
      className: 'OutputRecord',
      fields: [
        {
          name: 'normalized_sex',
          range: 'string',
          required: true,
          description: 'Normalized sex label',
        },
        {
          name: 'is_adult',
          range: 'boolean',
          required: true,
          description: 'Adult status at time of execution',
        },
      ],
    },
  },
];
