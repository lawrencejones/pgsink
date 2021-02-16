// API types that we should generate from the openapi schema, but the generators
// aren't very nice.

export type Table = {
  schema: string;
  name: string;
  publication_status: string;
  import_status: string;
  import_rows_processed_total: number;
  approximate_row_count: number;
};
