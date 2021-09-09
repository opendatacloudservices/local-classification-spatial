import * as dotenv from 'dotenv';
import * as path from 'path';
import {Client} from 'pg';
import * as notifier from 'node-notifier';
import fetch from 'node-fetch';

import {
  createCollection,
  dropCollection,
  getImportValues,
  checkValues,
  ogr,
  createSource,
  importValues,
  saveMatch,
  dropImport,
} from './import/index';
import {sizeLimit} from './file/index';
import {
  generateTableName,
  tableExists,
  getColumns,
  getNext,
  getImportQueue,
  saveBigFile,
  setClassified,
  rowCount,
} from './postgres/index';
import {
  cleanGeometries,
  collectionFromSource,
  matchGeometries,
  matchMatrix,
  hasGeom,
} from './postgis/index';
import {
  list as matchesList,
  details as matchesDetails,
  geojsonClean as matchesGeojsonClean,
} from './postgres/matches';

// get environmental variables
dotenv.config({path: path.join(__dirname, '../.env')});

import {api, catchAll, port} from 'local-microservice';

import {logError, addToken} from 'local-logger';
import {wait} from './utils';
import {handleXplan, isXplan} from './import/xplan';
import {handleThematic, list as thematicList} from './import/thematic';

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

// opendataservices database with imports
const odcs_client = new Client({
  user: process.env.ODCS_PGUSER,
  host: process.env.ODCS_PGHOST,
  database: process.env.ODCS_PGDATABASE,
  password: process.env.ODCS_PGPASSWORD,
  port: parseInt(process.env.ODCS_PGPORT || '5432'),
});
odcs_client.connect().catch((err: Error) => {
  logError({message: err});
});

let active = false;
const queueLimit = 50;
let notified = false;

/**
 * @swagger
 *
 * /next:
 *   get:
 *     operationId: getNext
 *     description: import next file
 *     produces:
 *       - application/json
 *     responses:
 *       500:
 *         description: error
 *       200:
 *         description: success
 */
api.get('/next', async (req, res) => {
  // if there are too many geometries await approval import is stopped
  const queueSize = await getImportQueue(client);
  if (queueSize < queueLimit) {
    active = true;
    // get next import from the opendataservice database
    const next = await getNext(odcs_client);
    if (next) {
      const file = process.env.DOWNLOAD_LOCATION + next.file;

      // files exceeding our import limit are being ignored
      if (sizeLimit(file)) {
        // create a temporary table and import the file through ogr
        const tableName = generateTableName(file);
        const exists = await tableExists(client, tableName);

        if (exists) {
          await dropImport(client, tableName);
        }

        try {
          await ogr(file, tableName);
          // postgres sometimes needs its time
          await wait(2500);

          // check if the table was created
          const exists = await tableExists(client, tableName);
          if (exists) {
            // check if this file contains geometries
            // quite often wfs layers do not contain geometries?!
            if (
              (await hasGeom(client, tableName)) &&
              (await rowCount(client, tableName)) > 0
            ) {
              // clean the geometries (multi > single + fixing)
              await cleanGeometries(client, tableName);
              await wait(2500);

              // match new geometries against existing geometries
              const match = await matchGeometries(client, tableName + '_cln');
              let check = false;

              // if there is a match, check if the geom-attributes already exist
              if (match.source_id) {
                const columns = await getColumns(client, tableName);
                const values = await getImportValues(
                  client,
                  tableName,
                  columns
                );
                check = await checkValues(
                  client,
                  values,
                  columns,
                  match.source_id,
                  new Date('2021-08-26 14:46:00'),
                  await matchMatrix(client, tableName, match.source_id)
                );

                // if the attributes do not exist, insert into database
                if (check) {
                  // TODO: identify name columns and add to names array
                  const collection_id = await collectionFromSource(
                    client,
                    match.source_id
                  );

                  const source_id = await createSource(
                    client,
                    odcs_client,
                    next.downloaded,
                    next.id,
                    collection_id
                  );

                  const columns = await getColumns(client, tableName);
                  const values = await getImportValues(
                    client,
                    tableName,
                    columns
                  );

                  await importValues(
                    client,
                    tableName,
                    values,
                    columns,
                    next.downloaded,
                    source_id
                  );
                } else {
                  // This already exists in our database
                  await saveMatch(
                    client,
                    next.id,
                    next.file,
                    match,
                    'duplicate',
                    tableName,
                    match.process?.differences
                  );
                }
                // remove temporary import table
                await dropImport(client, tableName);
              } else {
                const match_id = await saveMatch(
                  client,
                  next.id,
                  next.file,
                  match,
                  'no-match',
                  tableName
                );

                if (await isXplan(client, odcs_client, match_id)) {
                  await handleXplan(client, odcs_client, match_id);
                }
              }
              await setClassified(odcs_client, next.id);
              res
                .status(200)
                .json({message: 'importing, success', match, check});
            } else {
              await setClassified(odcs_client, next.id, false);
              await dropImport(client, tableName);
              res.status(200).json({message: 'no geom'});
            }
          } else {
            console.log('weird 1');
            await setClassified(odcs_client, next.id, false);
            await dropImport(client, tableName);
            res.status(200).json({message: 'weird file'});
          }
        } catch (err) {
          console.log('err', err);
          console.log(next.id, tableName);
          await setClassified(odcs_client, next.id);
          // for error evaluation the table in question is not dropped??
          // await dropImport(client, tableName);
          res.status(200).json({message: 'weird file'});
        }
      } else {
        // file is too big, information gets stored in table "Matches"
        await saveBigFile(client, next.id);
        await setClassified(odcs_client, next.id);
        res.status(200).json({message: 'importing, file too large'});
      }
      fetch(addToken(`http://localhost:${port}/next`, res));
    } else {
      // Everything from the opendataservice
      res.status(200).json({message: 'nothing to import'});
    }
  } else {
    if (!notified) {
      notified = true;
      notifier.notify({
        title: 'ODCS - Spatial Classfication',
        message: 'Classification queue is full',
        sound: true,
        wait: false,
      });
    }
    res.status(200).json({message: 'queue is full'});
  }
});

/**
 * @swagger
 *
 * /start:
 *   get:
 *     operationId: getStart
 *     description: start importing
 *     produces:
 *       - application/json
 *     responses:
 *       500:
 *         description: error
 *       200:
 *         description: success
 */
api.get('/start', async (req, res) => {
  if (active) {
    res.status(200).json({message: 'import in progress'});
  } else {
    // TODO: Call /next
    res.status(200).json({message: 'nothing to import'});
  }
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
    const collectionId = await createCollection(
      client,
      req.query.table!.toString(),
      req.query.name!.toString(),
      req.query.namecolumn!.toString(),
      req.query.spat ? req.query.spat!.toString() : null
    );

    // TODO: also import the values

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

// TODO: call /start
// TODO: derive spatial relationships

/**
 * @swagger
 *
 * /matches/list:
 *   get:
 *     operationId: getMatchesList
 *     description: List of awaiting matches
 *     produces:
 *       - application/json
 *     responses:
 *       500:
 *         description: error
 *       200:
 *         description: success
 */
api.get('/matches/list', async (req, res) => {
  matchesList(client).then(result => {
    res.status(200).json(result);
  });
});

/**
 * @swagger
 *
 * /matches/details/:id:
 *   get:
 *     operationId: getMatchesDetails
 *     description: Get more details on a specific match
 *     produces:
 *       - application/json
 *     responses:
 *       500:
 *         description: error
 *       200:
 *         description: success
 */
api.get('/matches/details/:id', async (req, res) => {
  if (!req.params.id) {
    res.status(400).json({message: 'Missing id parameter'});
  } else {
    matchesDetails(odcs_client, parseInt(req.params.id)).then(result => {
      res.status(200).json(result);
    });
  }
});

/**
 * @swagger
 *
 * /matches/geojson/:id:
 *   get:
 *     operationId: getMatchesGeojson
 *     description: Get geojson representation of a match
 *     produces:
 *       - application/json
 *     responses:
 *       500:
 *         description: error
 *       200:
 *         description: success
 */
api.get('/matches/geojson/:id', async (req, res) => {
  if (!req.params.id) {
    res.status(400).json({message: 'Missing id parameter'});
  } else {
    matchesGeojsonClean(client, parseInt(req.params.id)).then(result => {
      res.status(200).json(result);
    });
  }
});

/**
 * @swagger
 *
 * /matches/setxplan/:id:
 *   get:
 *     operationId: getMatchesSetXPlan
 *     description: Classify an import as xplan
 *     produces:
 *       - application/json
 *     responses:
 *       500:
 *         description: error
 *       200:
 *         description: success
 */
api.get('/matches/setxplan/:id', async (req, res) => {
  if (!req.params.id) {
    res.status(400).json({message: 'Missing id parameter'});
  } else {
    handleXplan(client, odcs_client, parseInt(req.params.id)).then(() => {
      res
        .status(200)
        .json({message: 'Classification success!', id: req.params.id});
    });
  }
});

/**
 * @swagger
 *
 * /matches/setthematic/:id:
 *   get:
 *     operationId: getMatchesSetThematic
 *     description: Classify an import as thematic
 *     produces:
 *       - application/json
 *     responses:
 *       500:
 *         description: error
 *       200:
 *         description: success
 */
api.get('/matches/setthematic/:id', async (req, res) => {
  if (!req.params.id) {
    res.status(400).json({message: 'Missing id parameter'});
  } else if (!req.query.thematic) {
    res.status(400).json({
      message:
        'Missing thematic query parameter (id:number or new_thematic:string)',
    });
  } else {
    handleThematic(
      odcs_client,
      client,
      parseInt(req.params.id),
      req.query.thematic.toString()
    ).then(() => {
      res
        .status(200)
        .json({message: 'Classification success!', id: req.params.id});
    });
  }
});

// TODO: This should probably be moved to another endpoin?!
/**
 * @swagger
 *
 * /thematic/topics:
 *   get:
 *     operationId: getThematicTopics
 *     description: Get a list of existing topics in the DownloadedFiles table
 *     produces:
 *       - application/json
 *     responses:
 *       500:
 *         description: error
 *       200:
 *         description: success
 */
// TODO: thematic as additional table and only IDs in downloadedFiles
api.get('/thematic/topics', async (req, res) => {
  if (!req.query.thematic) {
    thematicList(odcs_client).then(result => {
      res.status(200).json(result);
    });
  }
});

catchAll();
