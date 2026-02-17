import { render, screen } from '@testing-library/react';
import { PageHeader } from './PageHeader';

describe('PageHeader', () => {
  it('renders title, description, and actions', () => {
    render(
      <PageHeader
        title="Model Catalog"
        description="Inspect model metadata"
        actions={<button type="button">Action</button>}
      />,
    );

    expect(screen.getByRole('heading', { name: 'Model Catalog' })).toBeInTheDocument();
    expect(screen.getByText('Inspect model metadata')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Action' })).toBeInTheDocument();
  });
});
