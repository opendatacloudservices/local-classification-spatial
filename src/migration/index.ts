import {Client} from 'pg';
import {dropImport, saveColumns} from '../import';
import * as fastcsv from 'fast-csv';
import {createWriteStream, unlinkSync} from 'fs';
import {exec} from 'child_process';

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

export const reset = async (
  client: Client,
  odcsClient: Client
): Promise<void> => {
  await client.query('TRUNCATE TABLE "CollectionRelations"');
  await client.query('TRUNCATE TABLE "Collections"');
  await client.query('TRUNCATE TABLE "Geometries"');
  await client.query('TRUNCATE TABLE "GeometryAttributes"');
  await client.query('TRUNCATE TABLE "Matches"');
  await client.query('TRUNCATE TABLE "Sources"');
  await odcsClient.query(
    'UPDATE "DownloadedFiles" SET spatial_classification = NULL'
  );
};

export const transformGeom = async (client: Client): Promise<void> => {
  const tables = await client
    .query(
      "SELECT * FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE' AND table_name LIKE 'db_%'"
    )
    .then(result => result.rows);

  for (let t = 0; t < tables.length; t += 1) {
    if (tables[t].table_name.indexOf('_cln') > -1) {
      const name = tables[t].table_name.split('_cln')[0];
      await client.query(
        `ALTER TABLE ${name} ADD COLUMN IF NOT EXISTS geom_3857 GEOMETRY(GEOMETRY, 3857)`
      );
      await client.query(
        `UPDATE ${name} SET geom_3857 = ST_Transform(ST_Force2D(geom), 3857)`
      );
      await client.query(
        `ALTER TABLE ${name}_cln ADD COLUMN IF NOT EXISTS buffer GEOMETRY(GEOMETRY, 3857)`
      );
      await client.query(
        `UPDATE ${name}_cln SET buffer = ST_Buffer(geom_3857, 5)`
      );
    }
  }
};

export const updateBuffer = async (client: Client): Promise<void> => {
  await client.query(
    'UPDATE "Geometries" SET buffer = ST_Buffer(geom_3857, 50)'
  );
  const tables = await client
    .query(
      "SELECT * FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE' AND table_name LIKE 'db_%'"
    )
    .then(result => result.rows);

  for (let t = 0; t < tables.length; t += 1) {
    if (tables[t].table_name.indexOf('_cln') > -1) {
      const name = tables[t].table_name.split('_cln')[0];
      await client.query(
        `UPDATE ${name}_cln SET buffer = ST_Buffer(geom_3857, 50)`
      );
    }
  }
};

export const exportCSV = async (client: Client): Promise<void> => {
  const sources = await client
    .query('SELECT id FROM "Sources"')
    .then(result => result.rows.map(r => r.id));

  for (let s = 0; s < sources.length; s += 1) {
    const columns: {[key: string]: string} = {};
    const data = await client
      .query('SELECT * FROM "GeometryAttributes" WHERE source_id = $1', [
        sources[s],
      ])
      .then(result => result.rows);
    const rows: {
      [key: string | number]: {
        [key: string]: number | string | boolean | null;
      };
    } = {};
    data.forEach(d => {
      if (!(d.fid in rows)) {
        rows[d.fid] = {};
      }
      if (!(d.key in columns)) {
        columns[d.key] = '';
      }
      let value: number | string | boolean | null = null;
      [
        'int_val',
        'int_a_val',
        'str_val',
        'str_a_val',
        'double_val',
        'double_a_val',
      ].forEach(type => {
        if (d[type]) {
          value = d[type];
          columns[d.key] = type;
        }
      });
      rows[d.fid][d.key] = value;
    });
    const csvStream = fastcsv.format({headers: true});
    const fileName = (process.env.DATA_LOCATION || '') + sources[s];
    csvStream.pipe(createWriteStream(fileName + '.csv'));
    Object.keys(rows).forEach((rFid: string | number) => {
      const v = rows[rFid];
      const row: {[key: string]: string | number | null | boolean} = {
        oFid: null,
        mFid: rFid,
      };
      Object.keys(v).forEach(key => {
        row[key] = v[key];
      });
      csvStream.write(row);
    });
    csvStream.end();
    await new Promise<void>(resolve => {
      exec(`zip -j ${fileName}.zip ${fileName}.csv`, (err, stdout, stderr) => {
        if (err) {
          console.log({err, stdout, stderr});
        }
        unlinkSync(fileName + '.csv');
        resolve();
      });
    });
    await saveColumns(
      client,
      Object.keys(columns).map(c => {
        return {name: c, type: columns[c]};
      }),
      sources[s]
    );
  }
};
