import { EmptyState } from '../components/EmptyState';
import { ErrorBanner } from '../components/ErrorBanner';
import { LoadingSkeleton } from '../components/LoadingSkeleton';
import { PageHeader } from '../components/PageHeader';
import { useSettings } from '../app/settings-context';
import { useCopilotMessages, useCopilotStatus } from '../features/copilot/hooks';
import styles from './CopilotPage.module.css';

export function CopilotPage() {
  const { featureFlags } = useSettings();
  const statusQuery = useCopilotStatus();
  const messagesQuery = useCopilotMessages();

  if (statusQuery.isLoading || messagesQuery.isLoading) {
    return <LoadingSkeleton lines={4} />;
  }

  if (statusQuery.isError || messagesQuery.isError) {
    return <ErrorBanner message="Unable to load copilot scaffold metadata." />;
  }

  return (
    <>
      <PageHeader
        title="Copilot"
        description="MCP-assisted copilot scaffold with auditable transcript layout and integration status visibility."
      />

      {!featureFlags.copilotEnabled ? (
        <EmptyState
          title="Copilot is disabled"
          description="Enable the Copilot scaffold in Settings to preview integration surfaces."
        />
      ) : (
        <section className={styles.panel} aria-label="Copilot transcript placeholder">
          <p>
            Integration status: <strong>{statusQuery.data?.enabled ? 'enabled' : 'coming soon'}</strong>
          </p>
          <p>
            Transport: <strong>{statusQuery.data?.transport}</strong>
          </p>
          <p>
            Endpoint: <code>{statusQuery.data?.endpoint}</code>
          </p>

          <h2>Transcript Placeholder</h2>
          <ul>
            {messagesQuery.data?.map((message) => (
              <li key={message.id}>
                <strong>{message.role}:</strong> {message.content}
              </li>
            ))}
          </ul>
        </section>
      )}
    </>
  );
}
