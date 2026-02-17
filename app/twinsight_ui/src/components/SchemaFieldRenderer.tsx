import type { ModelSchemaInfo } from '../lib/contracts/types';
import styles from './SchemaFieldRenderer.module.css';

type Props = {
  schema: ModelSchemaInfo;
};

export function SchemaFieldRenderer({ schema }: Props) {
  return (
    <section className={styles.wrapper}>
      <h3>{schema.className}</h3>
      <ul>
        {schema.fields.map((field) => (
          <li key={field.name}>
            <div>
              <strong>{field.name}</strong> <span>({field.range})</span>
            </div>
            <p>{field.description}</p>
            <p className={styles.meta}>
              Required: {field.required ? 'yes' : 'no'}
              {field.enumValues?.length ? ` | Allowed values: ${field.enumValues.join(', ')}` : ''}
            </p>
          </li>
        ))}
      </ul>
    </section>
  );
}
