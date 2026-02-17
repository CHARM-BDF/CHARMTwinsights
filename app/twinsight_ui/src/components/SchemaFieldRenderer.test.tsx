import { render, screen } from '@testing-library/react';
import { SchemaFieldRenderer } from './SchemaFieldRenderer';

describe('SchemaFieldRenderer', () => {
  it('renders schema fields and requirements', () => {
    render(
      <SchemaFieldRenderer
        schema={{
          className: 'InputRecord',
          fields: [
            {
              name: 'bmi',
              range: 'float',
              required: true,
              description: 'Body mass index',
            },
          ],
        }}
      />,
    );

    expect(screen.getByRole('heading', { name: 'InputRecord' })).toBeInTheDocument();
    expect(screen.getByText('bmi')).toBeInTheDocument();
    expect(screen.getByText(/Required: yes/)).toBeInTheDocument();
  });
});
