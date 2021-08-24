import {exec} from 'child_process';
import {Client} from 'pg';
import {toSingleGeomType, getGeometryType} from '../postgis/index';

export const ogr = (
  filename: string,
  tableName: string
): Promise<{
  out: string;
  err: string;
}> => {
  return new Promise((resolve, reject) => {
    exec(
      `ogr2ogr \
      -f "PostgreSQL" \
      "PG:\
        host=${process.env.PGHOST} \
        user=${process.env.PGUSER} \
        dbname=${process.env.PGDATABASE} \
        password=${process.env.PGPASSWORD}" \
      "${filename}" \
      -lco GEOMETRY_NAME=geom \
      -lco FID=fid \
      -lco SPATIAL_INDEX=GIST \
      -nln ${tableName} \
      -t_srs EPSG:4326`,
      (err, stdout, stderr) => {
        if (err) {
          reject(err);
        }
        resolve({
          out: stdout,
          err: stderr,
        });
      }
    );
  });
};

export const dropImport = async (
  client: Client,
  tableName: string
): Promise<void> => {
  return client.query(`DROP TABLE ${tableName}`).then(() => {});
};

// TODO: separate all collection functions into postgres/collection.ts

export const createCollection = async (
  client: Client,
  tableName: string,
  collectionName: string,
  nameColumn: string,
  spatColumn?: string | null
): Promise<number> => {
  const geomType = toSingleGeomType(await getGeometryType(client, tableName));
  const bbox = await client
    .query(`SELECT ST_AsText(ST_Extent(geom)) AS bbox FROM ${tableName}`)
    .then(result => result.rows[0].bbox);
  const collectionId = await client
    .query(
      'INSERT INTO "Collections" (name, type, bbox) VALUES ($1, $2, ST_GeomFromText($3)) RETURNING id',
      [collectionName, geomType, bbox]
    )
    .then(result => result.rows[0].id);

  await client.query(
    `INSERT INTO "Geometries" (geom, fid, collection_id) SELECT geom, fid, ${collectionId} FROM ${tableName}_cln`
  );

  await client.query(
    `INSERT INTO "GeometryProps" 
      (fid, collection_id, name, centroid, area, len, spat_id)
    SELECT
      fid,
      ${collectionId},
      ${nameColumn},
      ST_Centroid(geom),
      ${geomType === 'POLYGON' ? 'ST_Area(geom)' : 'NULL'},
      ${geomType === 'LINESTRING' ? 'ST_LENGTH(geom)' : 'NULL'},
      ${spatColumn ? spatColumn : 'NULL'}
    FROM
      ${tableName}`
  );

  return collectionId;
};

export const dropCollection = async (
  client: Client,
  id: number
): Promise<void> => {
  return client
    .query('DELETE FROM "Collections" WHERE id = $1', [id])
    .then(() =>
      client.query('DELETE FROM "Geometries" WHERE collection_id = $1', [id])
    )
    .then(() => {});
};
