import {Client} from 'pg';
import {dropImport} from '../import';
import {DBMatch, DBMatchDetails, GeoJson} from '../types';

export const getMatch = (
  client: Client,
  id: number
): Promise<DBMatch | null> => {
  return client
    .query('SELECT * FROM "Matches" WHERE id = $1', [id])
    .then(result => (result.rows ? result.rows[0] : null));
};

export const list = (client: Client): Promise<DBMatch[]> => {
  return client.query('SELECT * FROM "Matches"').then(results => results.rows);
};

export const details = (
  odcsClient: Client,
  id: number
): Promise<DBMatchDetails[]> => {
  return odcsClient
    .query(
      `SELECT
        "DownloadedFiles".file,
        "Downloads".url,
        "Files".meta_name AS file_name,
        "Files".meta_description AS description,
        "Files".meta_function AS function,
        "Imports".meta_name AS name,
        "Imports".meta_abstract AS abstract,
        "Files".meta_format AS format
      FROM "DownloadedFiles" 
      JOIN "Downloads" ON "Downloads".id = "DownloadedFiles".download_id
      JOIN "Files" ON "Downloads".url = "Files".url
      JOIN "Imports" ON "Imports".id = "Files".dataset_id
      WHERE "DownloadedFiles".id = $1`,
      [id]
    )
    .then(results => results.rows);
};

export const geojsonClean = (client: Client, id: number): Promise<GeoJson> => {
  return client
    .query('SELECT table_name FROM "Matches" WHERE id = $1', [id])
    .then(results =>
      client.query(
        `SELECT fid, ST_AsGeoJson(ST_MakeValid(ST_CurveToLine(ST_Transform(geom_3857, 4326)))) AS geometry FROM ${results.rows[0].table_name}_cln`
      )
    )
    .then(results => {
      return generateGeoJson(results.rows);
    });
};

export const geojson = (client: Client, id: number): Promise<GeoJson> => {
  return client
    .query('SELECT table_name FROM "Matches" WHERE id = $1', [id])
    .then(results =>
      client.query(
        `SELECT *, ST_AsGeoJson(ST_MakeValid(ST_CurveToLine(geom_3857))) AS geometry FROM ${results.rows[0].table_name}`
      )
    )
    .then(results => {
      return generateGeoJson(results.rows);
    });
};

export const generateGeoJson = (
  features: {geometry: string; [index: string]: string | number}[]
): GeoJson => {
  const geojson: GeoJson = {
    type: 'FeatureCollection',
    features: [],
  };

  features.forEach(f => {
    const properties: {[index: string]: string | number} = {};
    Object.keys(f).forEach(k => {
      if (k !== 'geometry' && k !== 'geom' && k !== 'geom_3857') {
        properties[k] = f[k];
      }
    });
    geojson.features.push({
      type: 'Feature',
      properties,
      geometry: JSON.parse(f.geometry),
    });
  });

  return geojson;
};

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

export const removeMissingTables = async (client: Client): Promise<void> => {
  const matches = await client
    .query('SELECT table_name FROM "Matches"')
    .then(result => result.rows.map(r => r.table_name));

  const tables = await client
    .query(
      "SELECT * FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE' AND table_name LIKE 'db_%'"
    )
    .then(result => result.rows);

  for (let t = 0; t < tables.length; t += 1) {
    const name = tables[t].table_name;
    const baseName = name.split('_cln')[0];
    if (!systemTables.includes(name) && !matches.includes(baseName)) {
      await dropImport(client, tables[t].table_name);
    }
  }
};
