import * as dotenv from 'dotenv';
import * as path from 'path';
import {Client} from 'pg';

// get environmental variables
dotenv.config({path: path.join(__dirname, '../../.env')});

// connect to postgres (via env vars params)
const client = new Client({
  user: process.env.PGUSER,
  host: process.env.PGHOST,
  database: process.env.PGDATABASE,
  password: process.env.PGPASSWORD,
  port: parseInt(process.env.PGPORT || '5432'),
});

client.connect().catch((err: Error) => {
  console.log({message: err});
});

// opendataservices database with imports
const odcsClient = new Client({
  user: process.env.ODCS_PGUSER,
  host: process.env.ODCS_PGHOST,
  database: process.env.ODCS_PGDATABASE,
  password: process.env.ODCS_PGPASSWORD,
  port: parseInt(process.env.ODCS_PGPORT || '5432'),
});
odcsClient.connect().catch((err: Error) => {
  console.log({message: err});
});

// import {exportCSV} from './index';
// exportCSV(client).then(() => console.log('updateBuffer'));

/*
 TODO:
 DOWNLOAD_LOCATION
PLANNING_LOCATION
THEMATIC_LOCATION
DATA_LOCATION
"DownloadedFiles" file_location
BACKUP_LOCATION > env
WHERE is_planning

let query =
    'UPDATE "DownloadedFiles" SET spatial_classification = TRUE, classified_at = now()::timestamp';

  if (xplan) {
    query += ', is_plan = TRUE';
  }

  if (bbox) {
    query += `, bbox = ${
      bbox.indexOf('POLYGON') > -1
        ? `ST_GeomFromText($${params.length + 1}, 4326)`
        : bbox.indexOf('POINT') > -1
        ? `ST_Transform(ST_Envelope(ST_Buffer(ST_Transform(ST_GeomFromText($${
            params.length + 1
          }, 4326), 3857), 0.1)), 4326)`
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
*/
 