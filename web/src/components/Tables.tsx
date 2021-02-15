import React from 'react';

import {Table} from '../api';

import TableSearchBar from './TableSearchBar';
import TableList from './TableList';

type TablesProps = {
  schema: string;
}

type TablesState = {
  tables: Table[];
  searchFilter: string;
  ticker: ReturnType<typeof setTimeout>;
}

// Renders a table of all the Postgres tables, fetching content from the pgsink
// API.
class Tables extends React.Component<TablesProps, TablesState> {
  state: TablesState = {
    tables: [],
    searchFilter: "",
    ticker: setInterval(() => {}, 0),
  };

  render() {
    return (
      <div className="container">
        <div className="row mb-2">
          <div className="col-9">
            <h2>Tables</h2>
          </div>

          <div className="col-3">
            <TableSearchBar onSearchChange={(e) => this.onSearchChange(e)}/>
          </div>
        </div>

        <div className="col-12">
          <TableList
            tables={this.state.tables}
            searchFilter={this.state.searchFilter}
            triggerRefresh={() => this.triggerRefresh()}
          />
        </div>
      </div>
    );
  }

  onSearchChange(event: React.ChangeEvent<HTMLInputElement>) {
    this.setState({...this.state, searchFilter: event.target.value});
  }

  async componentDidMount() {
    this.triggerRefresh();
    this.state.ticker = setInterval(this.triggerRefresh.bind(this), 3000);
  }

  componentWillUnmount() {
    clearInterval(this.state.ticker);
  }

  async triggerRefresh() {
    const resp = await fetch("/api/tables");
    const tables = await resp.json();

    this.setState({...this.state, tables: tables});
  }
}

export default Tables;
