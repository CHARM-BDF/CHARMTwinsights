import { screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { SettingsPage } from './SettingsPage';
import { renderWithProviders } from '../test/renderWithProviders';

describe('SettingsPage integration', () => {
  it('updates service mode and stores settings in localStorage', async () => {
    const user = userEvent.setup();
    renderWithProviders(<SettingsPage />);

    const endpointMode = screen.getByLabelText('Endpoint Mode');
    await user.selectOptions(endpointMode, 'router');

    const stored = localStorage.getItem('twinsight_ui_settings') ?? '{}';
    expect(JSON.parse(stored).serviceMode).toBe('router');
  });
});
