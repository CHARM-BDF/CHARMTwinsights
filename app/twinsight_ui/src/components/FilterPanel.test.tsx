import { render, screen } from '@testing-library/react';
import { FilterPanel } from './FilterPanel';

describe('FilterPanel', () => {
  it('renders semantic filter section', () => {
    render(
      <FilterPanel title="Cohort Filters">
        <label htmlFor="cohort">Cohort</label>
        <input id="cohort" />
      </FilterPanel>,
    );

    expect(screen.getByRole('region', { name: 'Cohort Filters filters' })).toBeInTheDocument();
    expect(screen.getByRole('heading', { name: 'Cohort Filters' })).toBeInTheDocument();
  });
});
