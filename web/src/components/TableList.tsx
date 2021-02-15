import React from 'react';
import _ from 'lodash';
import Fuse from 'fuse.js';

import { Table } from '../api';
import TableListRow from './TableListRow';

type TableListProps = {
  tables: Table[];
  searchFilter: string;
  triggerRefresh: () => void;
};

// Renders a table of all the Postgres tables, fetching content from the pgsink
// API.
class TableList extends React.Component<TableListProps> {
  render(): JSX.Element {
    return (
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
          {this.getFilteredTables().map((table) => {
            return (
              <TableListRow
                key={`${table.schema}.${table.name}`}
                table={table}
                triggerRefresh={this.props.triggerRefresh}
              />
            );
          })}
        </tbody>
      </table>
    );
  }

  // Filter the tables by the search term, as provided by the search bar
  getFilteredTables(): Table[] {
    let tables = _.orderBy(this.props.tables, ['schema', 'name']);

    // Only filter if we have a filter!
    if (this.props.searchFilter !== '') {
      const filter = new Fuse(tables, {
        keys: ['name'],
        minMatchCharLength: Math.min(this.props.searchFilter.length, 5),
        shouldSort: true,
      });

      tables = filter.search(this.props.searchFilter).map((result) => result.item);
    }

    return tables;
  }
}

export default TableList;
