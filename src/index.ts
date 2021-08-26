import * as dotenv from 'dotenv';
import * as path from 'path';
import {Client} from 'pg';

import {
  createCollection,
  dropCollection,
  getImportValues,
  checkValues,
  ogr,
} from './import/index';
import {sizeLimit} from './file/index';
import {
  generateTableName,
  tableExists,
  dropTable,
  getColumns,
} from './postgres/index';
import {
  cleanGeometries,
  getGeometryType,
  matchGeometries,
  matchMatrix,
} from './postgis/index';

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
 * /importFile:
 *   get:
 *     operationId: getImportFile
 *     description: match a local spatial data file against exiting spatial taxonomies
 *     produces:
 *       - application/json
 *     responses:
 *       500:
 *         description: error
 *       200:
 *         description: success
 */
api.get('/importFile', async (req, res) => {
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
  if (!req.query.table || !req.query.name || !req.query.namecolumn) {
    res
      .status(400)
      .json({message: 'Missing parameters (table, name, namecolumn)'});
  } else {
    await cleanGeometries(client, req.query.table!.toString());

    const collectionId = await createCollection(
      client,
      req.query.table!.toString(),
      req.query.name!.toString(),
      req.query.namecolumn!.toString(),
      req.query.spat ? req.query.spat!.toString() : null
    );

    res.status(200).json({message: 'importing', collectionId});
  }
});

/**
 * @swagger
 *
 * /drop:
 *   get:
 *     operationId: getDrop
 *     description: Drop a collection
 *     produces:
 *       - application/json
 *     responses:
 *       500:
 *         description: error
 *       200:
 *         description: success
 */
api.get('/drop/:id', async (req, res) => {
  if (!req.params.id) {
    res.status(400).json({message: 'Missing id parameter'});
  } else {
    await dropCollection(client, parseInt(req.params.id));

    res.status(200).json({message: 'dropped', id: req.params.id});
  }
});

/**
 * @swagger
 *
 * /match:
 *   get:
 *     operationId: getMatch
 *     description: Math import with all collections
 *     produces:
 *       - application/json
 *     responses:
 *       500:
 *         description: error
 *       200:
 *         description: success
 */
api.get('/match/:table', async (req, res) => {
  if (!req.params.table) {
    res.status(400).json({message: 'Missing table parameter'});
  } else {
    const tableName = req.params.table;
    const match = await matchGeometries(client, tableName + '_cln');

    let check = false;

    if (match) {
      const columns = await getColumns(client, tableName);
      const values = await getImportValues(client, tableName, columns);
      check = await checkValues(
        client,
        values,
        columns,
        match,
        new Date('2021-08-26 14:46:00'),
        await matchMatrix(client, tableName, match)
      );
    }

    res
      .status(200)
      .json({message: 'matched', id: req.params.table, match, check});
  }
});

catchAll();
