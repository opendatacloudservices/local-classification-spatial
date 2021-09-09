import {Client} from 'pg';
import {dropImport} from '../import';

const systemTables = [
  'Collections',
  'Geometries',
  'GeometryAttributes',
  'GeometryProps',
  'Matches',
  'Sources',
  '_prisma_migrations',
  'spatial_ref_sys',
];

export const removeMatchTables = async (client: Client): Promise<void> => {
  const tables = await client
    .query(
      "SELECT * FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE' AND table_name LIKE 'db_%'"
    )
    .then(result => result.rows);

  for (let t = 0; t < tables.length; t += 1) {
    const name = tables[t].table_name;
    if (!systemTables.includes(name)) {
      await dropImport(client, tables[t].table_name);
    }
  }
};
