import React from 'react';
import './App.css';
import TableIndex from './TableIndex';

function App() {
  return (
    <div className="App">
      <nav className="navbar navbar-expand-md navbar-dark bg-dark mb-4">
        <div className="container-fluid">
          <a className="navbar-brand" href="/#">pgsink admin</a>
        </div>
      </nav>

      <main>
        <TableIndex schema="public">
        </TableIndex>
      </main>
    </div>
  );
}

export default App;
