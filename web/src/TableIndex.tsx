import React from 'react';
import _ from "lodash"
import Fuse from 'fuse.js';

type TableIndexProps = {
  schema: string;
}

type TableIndexState = {
  tables: Table[];
  tableFilter: string;
}

type Table = {
  schema: string;
  name: string;
  publication_status: string;
  import_status: string;
}

// Renders a table of all the Postgres tables, fetching content from the pgsink
// API.
class TableIndex extends React.Component<TableIndexProps, TableIndexState> {
  state: TableIndexState = {
    tables: [],
    tableFilter: "",
  };

  render() {
    return (
      <div className="container">
        <div className="row mb-2">
          <div className="col-9">
            <h2>Tables</h2>
          </div>

          <div className="col-3">
            <div className="input-group flex-nowrap">
              <span className="input-group-text" id="addon-wrapping">
                <i className="bi bi-search"></i>
              </span>
              <input
                type="text"
                value={this.state.tableFilter}
                onChange={(e) => this.handleFilterChange(e)}
                className="form-control"
                placeholder="Table name"
                aria-label="Table"
                aria-describedby="addon-wrapping">
              </input>
            </div>
          </div>
        </div>

        <div className="col-12">
          <table className="table table-hover">
            <thead>
              <tr>
                <th scope="col">Table</th>
                <th scope="col">Publication status</th>
                <th scope="col">Import status</th>
                <th scope="col">Sync</th>
              </tr>
            </thead>
            <tbody>
              {this.renderRows()}
            </tbody>
          </table>
        </div>
      </div>
    );
  }

  renderRows() {
    let tables = _.orderBy(this.state.tables, ['schema', 'name']);

    // Only filter if we have a filter!
    if (this.state.tableFilter !== "") {
      const filter = new Fuse(tables, {
        keys: ['name'],
        minMatchCharLength: Math.min(this.state.tableFilter.length, 5),
        shouldSort: true,
      });

      tables = filter.search(this.state.tableFilter).map((result) => result.item);
    }

    return tables.map((table) => {
      return (
        <tr key={`${table.schema}.${table.name}`}>
          <td className="align-middle">
            <code>{table.schema}.{table.name}</code>
          </td>
          <td>{this.renderPublicationStatus(table)}</td>
          <td>{this.renderImportStatus(table)}</td>
          <td>{this.renderSyncButton(table)}</td>
        </tr>
      );
    });
  }

  renderSyncButton(table: Table) {
    switch(table.publication_status) {
      case "inactive": {
        return <button onClick={(e) => { this.addTable(table) }} className="btn btn-sm btn-outline-secondary">Sync</button>;
      }
      case "active": {
        return <button onClick={(e) => { this.stopTable(table) }} className="btn btn-sm btn-danger">Stop</button>;
      }
    }
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

  handleFilterChange(event: React.ChangeEvent<HTMLInputElement>) {
    this.setState({...this.state, tableFilter: event.target.value});
  }

  async addTable(table: Table) {
    await fetch("/api/subscriptions/current/actions/add-table", {
      method: "post",
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify({
        schema: table.schema, name: table.name,
      }),
    });

    return this.getTables();
  }

  async stopTable(table: Table) {
    await fetch("/api/subscriptions/current/actions/stop-table", {
      method: "post",
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify({
        schema: table.schema, name: table.name,
      }),
    });

    return this.getTables();
  }

  async componentDidMount() {
    return this.getTables()
  }

  async getTables() {
    const resp = await fetch("/api/tables");
    const tables = await resp.json();

    this.setState({...this.state, tables: tables});
  }
}

export default TableIndex;
