import React from 'react';
import _ from "lodash"

type TableSearchBarProps = {
  onSearchChange: (event: React.ChangeEvent<HTMLInputElement>) => void
}

function TableSearchBar(props: TableSearchBarProps) {
  return (
    <div className="input-group flex-nowrap">
      <span className="input-group-text" id="addon-wrapping">
        <i className="bi bi-search"></i>
      </span>
      <input
        type="text"
        onChange={props.onSearchChange}
        className="form-control"
        placeholder="Table name"
        aria-label="Table"
        aria-describedby="addon-wrapping">
      </input>
    </div>
  );
}

export default TableSearchBar;
