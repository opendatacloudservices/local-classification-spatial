import {exec} from 'child_process';
import {Client} from 'pg';

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

export const createCollection = async (
  client: Client,
  tableName: string,
  collectionName: string,
  geomType: string
): Promise<number> => {
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
    `INSERT INTO "Geometries" (geom, collection_id) SELECT geom, ${collectionId} FROM ${tableName}`
  );

  return collectionId;
};
