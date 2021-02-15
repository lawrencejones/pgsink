import React from 'react';

type TableSearchBarProps = {
  onSearchChange: (event: React.ChangeEvent<HTMLInputElement>) => void;
};

function TableSearchBar(props: TableSearchBarProps): JSX.Element {
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
        aria-describedby="addon-wrapping"
      ></input>
    </div>
  );
}

export default TableSearchBar;
