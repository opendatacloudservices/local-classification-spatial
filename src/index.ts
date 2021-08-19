import * as dotenv from 'dotenv';
import * as path from 'path';
import {Client} from 'pg';

import {createCollection, ogr} from './import/index';
import {sizeLimit} from './file/index';
import {
  generateTableName,
  tableExists,
  dropTable,
  getColumns,
} from './postgres/index';
import {cleanGeometries, getGeometryType} from './postgis/index';

// get environmental variables
dotenv.config({path: path.join(__dirname, '../.env')});

import {api, catchAll} from 'local-microservice';

import {logError} from 'local-logger';

// connect to postgres (via env vars params)
const client = new Client({
  user: process.env.PGUSER,
  host: process.env.PGHOST,
  database: process.env.PGDATABASE,
  password: process.env.PGPASSWORD,
  port: parseInt(process.env.PGPORT || '5432'),
});
client.connect().catch((err: Error) => {
  logError({message: err});
});

/**
 * @swagger
 *
 * /match:
 *   get:
 *     operationId: getMatch
 *     description: match a local spatial data file against exiting spatial taxonomies
 *     produces:
 *       - application/json
 *     responses:
 *       500:
 *         description: error
 *       200:
 *         description: success
 */
api.get('/match', async (req, res) => {
  const file =
    '/home/sebastian/Sites/OpenDataCloudServices/local-classification-spatial/test_data/wfs_vg1000-ew/layer_0_vg1000:vg1000_krs.gpkg';

  if (sizeLimit(file)) {
    const tableName = generateTableName(file);
    const exists = await tableExists(client, tableName);

    if (exists) {
      await dropTable(client, tableName);
    }

    await ogr(file, tableName);

    const columns = await getColumns(client, tableName);
    console.log(columns);

    const geomType = await getGeometryType(client, tableName);
    console.log(geomType);
  }
  res.status(200).json({message: 'importing'});
});

/**
 * @swagger
 *
 * /import:
 *   get:
 *     operationId: getImport
 *     description: Import a new spatial topology as a new collection
 *     produces:
 *       - application/json
 *     responses:
 *       500:
 *         description: error
 *       200:
 *         description: success
 */
api.get('/import', async (req, res) => {
  if (!req.query.table || !req.query.name) {
    res.status(400).json({message: 'Missing parameters (table, name)'});
  } else {
    const collectionId = await createCollection(
      client,
      req.query.table!.toString(),
      req.query.name!.toString(),
      await getGeometryType(client, req.query.table!.toString())
    );

    await cleanGeometries(client, req.query.table!.toString());

    res.status(200).json({message: 'importing', collectionId});
  }
});

catchAll();
