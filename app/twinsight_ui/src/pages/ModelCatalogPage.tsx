import { useState } from 'react';
import { DataTable } from '../components/DataTable';
import { EmptyState } from '../components/EmptyState';
import { ErrorBanner } from '../components/ErrorBanner';
import { LoadingSkeleton } from '../components/LoadingSkeleton';
import { PageHeader } from '../components/PageHeader';
import { SchemaFieldRenderer } from '../components/SchemaFieldRenderer';
import { useModels } from '../features/models/hooks';
import styles from './ModelCatalogPage.module.css';

export function ModelCatalogPage() {
  const { data, isLoading, isError } = useModels();
  const [selectedImageTag, setSelectedImageTag] = useState<string | null>(null);

  const selected = data?.find((item) => item.imageTag === selectedImageTag) ?? data?.[0] ?? null;

  return (
    <>
      <PageHeader
        title="Model Catalog"
        description="Inspect registered model metadata, review schema requirements, and validate expected payload shape."
      />

      {isLoading ? <LoadingSkeleton lines={5} /> : null}
      {isError ? <ErrorBanner message="Unable to load model catalog." /> : null}

      {!isLoading && !isError && data && data.length > 0 ? (
        <div className={styles.layout}>
          <DataTable
            caption="Registered Models"
            rows={data}
            rowKey={(row) => row.imageTag}
            columns={[
              {
                key: 'title',
                header: 'Title',
                render: (row) => (
                  <button className={styles.linkButton} type="button" onClick={() => setSelectedImageTag(row.imageTag)}>
                    {row.title}
                  </button>
                ),
              },
              { key: 'imageTag', header: 'Image', render: (row) => row.imageTag },
              { key: 'authors', header: 'Authors', render: (row) => row.authors },
            ]}
          />

          {selected ? (
            <section className={styles.detail} aria-label="Selected model details">
              <h2>{selected.title}</h2>
              <p>{selected.shortDescription}</p>
              <SchemaFieldRenderer schema={selected.inputSchema} />
              <SchemaFieldRenderer schema={selected.outputSchema} />
            </section>
          ) : null}
        </div>
      ) : null}

      {!isLoading && !isError && (!data || data.length === 0) ? (
        <EmptyState title="No models registered" description="Register a model to inspect schemas in this catalog." />
      ) : null}
    </>
  );
}
