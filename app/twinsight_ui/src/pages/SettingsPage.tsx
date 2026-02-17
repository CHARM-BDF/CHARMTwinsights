import { PageHeader } from '../components/PageHeader';
import { CheckboxInput } from '../components/CheckboxInput';
import { SelectInput } from '../components/SelectInput';
import { useSettings } from '../app/settings-context';
import type { ServiceMode } from '../lib/api/endpointRegistry';
import styles from './SettingsPage.module.css';

export function SettingsPage() {
  const { serviceMode, setServiceMode, featureFlags, setFeatureFlag, persona } = useSettings();

  return (
    <>
      <PageHeader
        title="Settings"
        description="Configure endpoint mode selection, rollout flags, and persona scaffolding for future role variants."
      />

      <section className={styles.panel} aria-label="Runtime settings">
        <SelectInput
          id="serviceMode"
          label="Endpoint Mode"
          value={serviceMode}
          onChange={(event) => setServiceMode(event.target.value as ServiceMode)}
          options={[
            { value: 'mock', label: 'Mock' },
            { value: 'router', label: 'Router' },
            { value: 'direct', label: 'Direct' },
            { value: 'hybrid', label: 'Hybrid' },
          ]}
        />

        <CheckboxInput
          id="uiPrimary"
          label="Mark new UI as primary candidate"
          checked={featureFlags.uiPrimary}
          onChange={(event) => setFeatureFlag('uiPrimary', event.target.checked)}
        />

        <CheckboxInput
          id="copilotEnabled"
          label="Enable Copilot scaffold"
          checked={featureFlags.copilotEnabled}
          onChange={(event) => setFeatureFlag('copilotEnabled', event.target.checked)}
        />

        <p>Persona profile: <strong>{persona}</strong></p>
      </section>
    </>
  );
}
