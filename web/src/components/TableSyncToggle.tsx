import React from 'react';
import _ from "lodash"

import {Table} from '../api';

type TableSyncToggleProps = {
  table: Table;
  triggerRefresh: () => void;
}

class TableSyncToggle extends React.Component<TableSyncToggleProps> {
  render() {
    switch(this.props.table.publication_status) {
      case "inactive": {
        return <button onClick={(e) => { this.addTable() }} className="btn btn-sm btn-outline-secondary">Sync</button>;
      }
      case "active": {
        return <button onClick={(e) => { this.stopTable() }} className="btn btn-sm btn-danger">Stop</button>;
      }
    }
  }

  async addTable() {
    await fetch("/api/subscriptions/current/actions/add-table", {
      method: "post",
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify({
        schema: this.props.table.schema, name: this.props.table.name,
      }),
    });

    this.props.triggerRefresh();
  }

  async stopTable() {
    await fetch("/api/subscriptions/current/actions/stop-table", {
      method: "post",
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify({
        schema: this.props.table.schema, name: this.props.table.name,
      }),
    });

    this.props.triggerRefresh();
  }
}

export default TableSyncToggle;
