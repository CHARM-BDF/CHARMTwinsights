import { screen } from '@testing-library/react';
import { ConnectionHealthPanel } from './ConnectionHealthPanel';
import { renderWithProviders } from '../test/renderWithProviders';

describe('ConnectionHealthPanel integration', () => {
  it('renders mock endpoint checks', async () => {
    renderWithProviders(<ConnectionHealthPanel />);

    expect(await screen.findByRole('heading', { name: 'Connection Health' })).toBeInTheDocument();
    expect(await screen.findByText('Mock Cohort Feed')).toBeInTheDocument();
    expect(await screen.findByText('Mock Model Feed')).toBeInTheDocument();
  });
});
