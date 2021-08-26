import {Client} from 'pg';
import type {Columns} from '../types';

export const generateTableName = (filename: string): string => {
  const prefix = 'db_' + Date.now();
  const nameStart = filename.lastIndexOf('/');
  if (nameStart) {
    filename = filename.substr(nameStart);
  }
  const cleanName = filename
    .split(/[^a-zA-Z0-9]/gi)
    .join('')
    .substr(0, 15);
  return prefix + cleanName;
};

export const saveBigFile = (client: Client, id: number): Promise<void> => {
  return client
    .query('INSERT INTO "Matches" (import_id, message) VALUES ($1, $2)', [
      id,
      'big-file',
    ])
    .then(() => {});
};

export const tableExists = (
  client: Client,
  tableName: string
): Promise<boolean> => {
  return client
    .query(
      `SELECT EXISTS (
      SELECT FROM pg_tables
      WHERE  schemaname = 'public'
      AND    tablename  = $1
      );
   `,
      [tableName]
    )
    .then(result => {
      if (result.rowCount === 1 && result.rows[0].exists === true) {
        return true;
      }
      return false;
    });
};

export const dropTable = (client: Client, tableName: string): Promise<void> => {
  return client.query('DROP TABLE $1', [tableName]).then(() => {});
};

export const getColumns = (
  client: Client,
  tableName: string
): Promise<Columns> => {
  return client
    .query(
      `SELECT
    column_name, data_type, udt_name
    FROM
      INFORMATION_SCHEMA.COLUMNS
    WHERE
      table_schema = 'public' AND
      table_name = $1`,
      [tableName]
    )
    .then(results => {
      return results.rows.map(r => {
        return {
          name: r.column_name,
          type: r.data_type,
          udt: r.udt_name,
        };
      });
    });
};
