import React from 'react';
import _ from "lodash"

import {Table} from '../api';
import TableSyncToggle from './TableSyncToggle';

type TableListRowProps = {
  table: Table;
  triggerRefresh: () => void;
}

class TableListRow extends React.Component<TableListRowProps> {
  render() {
    const {table, triggerRefresh} = this.props;

    return (
      <tr key={`${table.schema}.${table.name}`}>
        <td className="align-middle">
          <code>{table.schema}.{table.name}</code>
        </td>
        <td>{this.renderPublicationStatus(table)}</td>
        <td>{this.renderImportStatus(table)}</td>
        <td>
          <TableSyncToggle table={table} triggerRefresh={triggerRefresh}/>
        </td>
      </tr>
    );
  }

  // publish (inactive -> active)
  renderPublicationStatus(table: Table) {
    switch(table.publication_status) {
      case "inactive": {
        return this.renderBadge("Inactive", "bg-secondary");
      }
      case "active": {
        return this.renderBadge("Active", "bg-success");
      }
      default: {
        return this.renderBadge("Unknown", "bg-warning");
      }
    }
  }

  // imports (inactive -> scheduled -> active -> complete)
  renderImportStatus(table: Table) {
    switch(table.import_status) {
      case "inactive": {
        return this.renderBadge("Inactive", "bg-secondary");
      }
      case "scheduled": {
        return this.renderBadge("Scheduled", "bg-warning");
      }
      case "in_progress": {
        return this.renderBadge("In progress", "bg-warning");
      }
      case "error": {
        return this.renderBadge("Error", "bg-danger");
      }
      case "complete": {
        return this.renderBadge("Complete", "bg-success");
      }
      case "expired": {
        return this.renderBadge("Expired", "bg-secondary");
      }
      case "Unknown": {
        return this.renderBadge("Unknown", "bg-warning");
      }
    }
  }

  renderBadge(text: string, style: string) {
    return <span className={`badge ${style}`}>{text}</span>;
  }
}

export default TableListRow;
