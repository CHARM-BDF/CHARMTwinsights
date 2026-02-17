import { screen } from '@testing-library/react';
import { axe } from 'jest-axe';
import { DashboardPage } from './DashboardPage';
import { renderWithProviders } from '../test/renderWithProviders';

describe('DashboardPage integration', () => {
  it('loads mock metrics and passes accessibility checks', async () => {
    const { container } = renderWithProviders(<DashboardPage />);

    expect(await screen.findByRole('heading', { name: 'Dashboard' })).toBeInTheDocument();
    expect(await screen.findByText('Registered cohort collections')).toBeInTheDocument();

    const results = await axe(container);
    expect(results).toHaveNoViolations();
  });
});
