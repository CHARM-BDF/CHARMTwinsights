import { screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { CohortBuilderPage } from './CohortBuilderPage';
import { renderWithProviders } from '../test/renderWithProviders';

describe('CohortBuilderPage validation', () => {
  it('shows validation error when max age is lower than min age', async () => {
    const user = userEvent.setup();
    renderWithProviders(<CohortBuilderPage />);

    const minAgeInput = screen.getByLabelText('Minimum Age');
    const maxAgeInput = screen.getByLabelText('Maximum Age');

    await user.clear(minAgeInput);
    await user.type(minAgeInput, '90');
    await user.clear(maxAgeInput);
    await user.type(maxAgeInput, '40');

    await user.click(screen.getByRole('button', { name: 'Create Generation Intent' }));

    expect(
      await screen.findByText('Maximum age must be greater than or equal to minimum age'),
    ).toBeInTheDocument();
  });
});
