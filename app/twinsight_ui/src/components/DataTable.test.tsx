import { render, screen } from '@testing-library/react';
import { DataTable } from './DataTable';

describe('DataTable', () => {
  it('renders table caption and rows', () => {
    render(
      <DataTable
        caption="Run Records"
        rows={[
          { id: '1', name: 'Run A' },
          { id: '2', name: 'Run B' },
        ]}
        rowKey={(row) => row.id}
        columns={[
          {
            key: 'name',
            header: 'Name',
            render: (row) => row.name,
          },
        ]}
      />,
    );

    expect(screen.getByText('Run Records')).toBeInTheDocument();
    expect(screen.getByText('Run A')).toBeInTheDocument();
    expect(screen.getByText('Run B')).toBeInTheDocument();
  });
});
