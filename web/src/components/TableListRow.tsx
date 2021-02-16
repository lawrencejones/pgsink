import React from 'react';

import { Table } from '../api';
import TableSyncToggle from './TableSyncToggle';

type TableListRowProps = {
  table: Table;
  triggerRefresh: () => void;
};

class TableListRow extends React.Component<TableListRowProps> {
  render(): JSX.Element {
    const { table, triggerRefresh } = this.props;

    return (
      <tr key={`${table.schema}.${table.name}`}>
        <td className="align-middle">
          <code>
            {table.schema}.{table.name}
          </code>
        </td>
        <td>{this.renderPublicationStatus(table)}</td>
        <td>{this.renderImportStatus(table)}</td>
        <td>
          <TableSyncToggle table={table} triggerRefresh={triggerRefresh} />
        </td>
      </tr>
    );
  }

  // publish (inactive -> active)
  renderPublicationStatus(table: Table): JSX.Element {
    switch (table.publication_status) {
      case 'inactive': {
        return this.renderBadge('Inactive', 'bg-secondary');
      }
      case 'active': {
        return this.renderBadge('Active', 'bg-success');
      }
      default: {
        return this.renderBadge('Unknown', 'bg-warning');
      }
    }
  }

  // imports (inactive -> scheduled -> active -> complete)
  renderImportStatus(table: Table): JSX.Element {
    switch (table.import_status) {
      case 'inactive': {
        return this.renderProgressBar(table, 'bg-secondary', 'NA', 0, '#000');
      }
      case 'scheduled': {
        return this.renderProgressBar(table, 'bg-warning', 'Scheduled', 0, '#000');
      }
      case 'in_progress': {
        return this.renderProgressBar(table, 'bg-warning');
      }
      case 'error': {
        return this.renderProgressBar(table, 'bg-danger', 'Error');
      }
      case 'complete': {
        return this.renderProgressBar(table, 'bg-success', 'Complete');
      }
      case 'expired': {
        return this.renderProgressBar(table, 'bg-secondary', 'Expired', 0, '#000');
      }
    }

    // We may have an explicit 'unknown' state, but we want to return our unknown in all
    // unknown cases.
    return this.renderProgressBar(table, 'bg-warning', 'Unknown');
  }

  renderProgressBar(table: Table, style: string, text?: string, progress?: number, textColor = '#fff'): JSX.Element {
    const processed = table.import_rows_processed_total,
      total = table.approximate_row_count;
    const measuredProgress = Math.floor((100 * processed) / total);

    if (progress == null) {
      progress = measuredProgress;
    }
    if (text == null) {
      text = `${processed} / ${total} rows`;
    }

    return (
      <div className="progress position-relative">
        <div
          className={`progress-bar ${style}`}
          role="progressbar"
          style={{ width: `${progress}%` }}
          aria-valuenow={progress}
          aria-valuemin={0}
          aria-valuemax={100}
        ></div>
        <small className="justify-content-center d-flex position-absolute w-100" style={{ color: textColor }}>
          <b>{text}</b>
        </small>
      </div>
    );
  }

  renderBadge(text: string, style: string): JSX.Element {
    return <span className={`badge ${style}`}>{text}</span>;
  }
}

export default TableListRow;
