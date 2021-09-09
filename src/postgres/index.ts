import {Client} from 'pg';
import type {Columns, Download} from '../types';

export const generateTableName = (filename: string): string => {
  const prefix = 'db_' + Date.now();
  const nameStart = filename.lastIndexOf('/');
  if (nameStart) {
    filename = filename.substr(nameStart);
  }
  const cleanName = filename
    .toLowerCase()
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

const acceptedFormat = [
  'geojson',
  'gml',
  'gpkg',
  'gpx',
  'kml',
  'kmz',
  'shp',
  'wfs',
];

const acceptedMimetype = [
  'application/geo+json',
  'application/gml+xml',
  'application/gpx+xml',
  'application/vnd.google-earth.kml+xml',
  'application/vnd.google-earth.kmz',
];

// TODO: special handling for existing previous information...
export const getNext = (client: Client): Promise<Download | null> => {
  return client
    .query(
      `SELECT "DownloadedFiles".id, "DownloadedFiles".file, "Downloads".downloaded, "Downloads".format, "Downloads".mimetype FROM "DownloadedFiles"
      JOIN "Downloads" ON
        download_id = "Downloads".id
      WHERE
        ("DownloadedFiles".spatial_classification = FALSE OR "DownloadedFiles".spatial_classification IS NULL) AND
        (
          format IN (${acceptedFormat.map(f => `'${f}'`).join(',')}) OR
          mimetype IN (${acceptedMimetype.map(m => `'${m}'`).join(',')})
        )
      LIMIT 1`
    )
    .then(result => (result.rowCount === 1 ? result.rows[0] : null));
};

export const setClassified = (
  client: Client,
  id: number,
  hasGeom = true,
  geomType: null | string = null,
  xplan = false,
  centroid: null | string = null,
  bbox: null | string = null,
  thematic: null | string = null
): Promise<number> => {
  const params = [];

  let query = 'UPDATE "DownloadedFiles" SET spatial_classification = TRUE';

  if (xplan) {
    query += ', is_plan = TRUE';
  }

  if (bbox) {
    query += `, bbox = ${
      bbox.indexOf('POLYGON') > -1
        ? `ST_GeomFromText($${params.length + 1}, 4326)`
        : `ST_MakePolygon(ST_GeomFromText($${params.length + 1}, 4326))`
    }`;
    params.push(bbox);
  }

  if (centroid) {
    query += `, centroid = ST_GeomFromText($${params.length + 1}, 4326)`;
    params.push(centroid);
  }

  if (!hasGeom) {
    query += ', no_geom = TRUE';
  }

  if (geomType) {
    query += `, geom_type = $${params.length + 1}`;
    params.push(geomType);
  }

  if (thematic) {
    query += `, thematic = $${params.length + 1}`;
    params.push(thematic);
  }

  query += `WHERE id = $${params.length + 1} RETURNING download_id`;
  params.push(id);

  return client.query(query, params).then(result => result.rows[0].download_id);
};

export const getImportQueue = (client: Client): Promise<number> => {
  return client
    .query('SELECT COUNT(*) AS queue_count FROM "Matches"')
    .then(result => result.rows[0].queue_count);
};

export const downloadCompletion = (
  odcsClient: Client,
  download_id: number
): Promise<void> => {
  // check if all downloaded files belonging to one download have been processed
  return odcsClient
    .query(
      'SELECT download_id, spatial_classification FROM "DownloadedFiles" WHERE download_id = $1',
      [download_id]
    )
    .then(result => {
      const files = result.rows;
      let allClassified = true;
      files.forEach(f => {
        if (!f.spatial_classification) {
          allClassified = false;
        }
      });
      if (allClassified) {
        return odcsClient
          .query('UPDATE "Downloads" SET state = $1 WHERE id = $2', [
            'transformed',
            download_id,
          ])
          .then(() => {});
      } else {
        return Promise.resolve();
      }
    });
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

export const rowCount = (
  client: Client,
  tableName: string
): Promise<number> => {
  return client
    .query(`SELECT COUNT(*) AS row_count FROM ${tableName}`)
    .then(result => result.rows[0].row_count);
};
