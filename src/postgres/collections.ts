import {Client} from 'pg';
import {getGeometryType, toSingleGeomType} from '../postgis';
import {Collection} from '../types';

export const list = (client: Client): Promise<Collection[]> => {
  return client
    .query('SELECT * FROM "Collections"')
    .then(result => result.rows);
};

export const create = async (
  client: Client,
  tableName: string,
  collectionName: string
): Promise<number> => {
  const geomType = await toSingleGeomType(
    await getGeometryType(client, tableName),
    client,
    tableName
  );

  const bbox = await client
    .query(
      `SELECT ST_AsText(ST_Extent(ST_Transform(geom_3857, 4326))) AS bbox FROM ${tableName}`
    )
    .then(result => result.rows[0].bbox);

  const collectionId = await client
    .query(
      'INSERT INTO "Collections" (name, type, bbox) VALUES ($1, $2, ST_GeomFromText($3, 4326)) RETURNING id',
      [collectionName, geomType, bbox]
    )
    .then(result => result.rows[0].id);

  return collectionId;
};

export const drop = async (client: Client, id: number): Promise<void> => {
  const sources = await client
    .query('SELECT id FROM "Sources" WHERE collection_id = $1', [id])
    .then(result => result.rows);

  if (sources && sources.length > 0) {
    const tables = ['Geometries', 'GeometryProps', 'GeometryAttributes'];
    await Promise.all(
      tables.map(t =>
        client.query(
          `DELETE FROM "${t}" WHERE source_id IN (${sources
            .map(s => s.id)
            .join(',')})`
        )
      )
    );
    await client.query(
      `DELETE FROM "Sources" WHERE id IN (${sources.map(s => s.id).join(',')})`
    );
  }

  return client
    .query('DELETE FROM "Collections" WHERE id = $1', [id])
    .then(() => {});
};
