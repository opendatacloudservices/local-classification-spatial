import * as dotenv from 'dotenv';
import * as path from 'path';
import {Client} from 'pg';
import * as notifier from 'node-notifier';
import fetch from 'node-fetch';

import {
  getImportValues,
  ogr,
  createSource,
  importValues,
  saveMatch,
  dropImport,
  importMatch,
  finishImport,
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
  hasGeom,
} from './postgis/index';
import {
  list as matchesList,
  details as matchesDetails,
  geojsonClean as matchesGeojsonClean,
  getMatch,
} from './postgres/matches';
import {
  list as collectionsList,
  drop as dropCollection,
} from './postgres/collections';

// get environmental variables
dotenv.config({path: path.join(__dirname, '../.env')});

import {api, catchAll, port} from 'local-microservice';

import {logError, addToken} from 'local-logger';
import {wait} from './utils';
import {handleXplan, isXplan} from './import/xplan';
import {handleThematic, list as thematicList} from './import/thematic';
import {getFromImportID} from './postgres/downloads';

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
              const match = await matchGeometries(client, tableName);

              // if there is a match, check if the geom-attributes already exist
              if (match.source_id) {
                // TODO: for now we save all values, as it is more or less impossible to tell if its a duplicate???
                // TODO: save the WFS layer name somewhere (its important)
                // const columns = await getColumns(client, tableName);
                // const values = await getImportValues(
                //   client,
                //   tableName,
                //   columns
                // );

                // check = await checkValues(
                //   client,
                //   values,
                //   columns,
                //   match.source_id,
                //   next.downloaded,
                //   await matchMatrix(client, tableName, match.source_id)
                // );

                // if the attributes do not exist, insert into database
                // if (check) {
                //    INSERT ITS NEW
                // } else {
                //   // This already exists in our database
                //   await saveMatch(
                //     client,
                //     next.id,
                //     next.file,
                //     match,
                //     'duplicate',
                //     tableName,
                //     match.process?.differences
                //   );
                // }

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
                  collection_id,
                  JSON.stringify(match.process) || null,
                  match.source_id
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
                  source_id,
                  match.source_id
                );

                await setClassified(
                  odcs_client,
                  next.id,
                  true,
                  null,
                  false,
                  null,
                  null,
                  null
                );
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
              res.status(200).json({message: 'importing, success', match});
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
      active = false;
      res.status(200).json({message: 'nothing to import'});
    }
  } else {
    active = false;
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
 *         examples:
 *           application/json: { "message": "import in progress" }
 *           application/json: { "message": "import started" }
 */
api.get('/start', async (req, res) => {
  if (active) {
    res.status(200).json({message: 'import in progress'});
  } else {
    active = true;
    fetch(addToken(`http://localhost:${port}/next`, res));
    res.status(200).json({message: 'import initiated'});
  }
});

/**
 * @swagger
 *
 * /recheck:
 *   get:
 *     operationId: getRecheck
 *     description: recheck already imported tables
 *     produces:
 *       - application/json
 *     responses:
 *       500:
 *         description: error
 *       200:
 *         description: success
 *         examples:
 *           application/json: { "message": "import in progress" }
 *           application/json: { "message": "import started" }
 */
api.get('/recheck', async (req, res) => {
  const matches = await matchesList(client);
  res.status(200).json({message: 'recheck started', count: matches.length});
  for (let m = 0; m < matches.length; m += 1) {
    const match = await matchGeometries(client, matches[m].table_name);
    if (match.source_id) {
      const collection_id = await collectionFromSource(client, match.source_id);
      const download = await getFromImportID(odcs_client, matches[m].import_id);

      const source_id = await createSource(
        client,
        odcs_client,
        download.downloaded,
        matches[m].import_id,
        collection_id,
        JSON.stringify(match.process) || null,
        match.source_id
      );

      const columns = await getColumns(client, matches[m].table_name);
      const values = await getImportValues(
        client,
        matches[m].table_name,
        columns
      );

      await importValues(
        client,
        matches[m].table_name,
        values,
        columns,
        source_id,
        match.source_id
      );

      await setClassified(
        odcs_client,
        matches[m].import_id,
        true,
        null,
        false,
        null,
        null,
        null
      );
      await dropImport(client, matches[m].table_name);
    } else {
      await saveMatch(
        client,
        matches[m].import_id,
        matches[m].file,
        match,
        'no-match',
        matches[m].table_name,
        undefined,
        matches[m].id
      );
    }
  }
  console.log('finished recheck');
});

/**
 * @swagger
 *
 * /check/{id}:
 *   get:
 *     operationId: getRecheck
 *     description: check a specific match
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema:
 *           type: integer
 *         description: id from the table Matches
 *     produces:
 *       - application/json
 *     responses:
 *       500:
 *         description: error
 *       200:
 *         description: success
 *         examples:
 *           application/json: { "message": "import in progress" }
 *           application/json: { "message": "import started" }
 */
api.get('/check/:id', async (req, res) => {
  if (!req.params.id) {
    res.status(400).json({
      message: 'Missing parameters (id)',
    });
  } else {
    const match = await getMatch(client, parseInt(req.params.id.toString()));
    const matching = await matchGeometries(client, match.table_name);
    res.status(400).json(matching);
  }
});

/**
 * @swagger
 *
 * /import/new:
 *   get:
 *     operationId: getImportNew
 *     description: Import a new spatial topology as a new collection (and its attributes)
 *     parameters:
 *       - in: query
 *         name: id
 *         required: true
 *         schema:
 *           type: integer
 *         description: id from the table Matches
 *       - in: query
 *         name: name
 *         required: true
 *         schema:
 *           type: string
 *         description: name for the new collection
 *       - in: query
 *         name: nameColumn
 *         required: true
 *         schema:
 *           type: string
 *         description: name of the column in which the names of geometries are stored in the to be imported table
 *       - in: query
 *         name: spatColumn
 *         required: false
 *         schema:
 *           type: string
 *         description: name of the column in which spatial identifiers of geometries are stored in the to be imported table
 *     produces:
 *       - application/json
 *     responses:
 *       500:
 *         description: error
 *       400:
 *         description: missing parameters
 *       200:
 *         description: success
 *         examples:
 *           application/json: { "message": "importing", collectionId: 123 }
 */
api.get('/import/new', async (req, res) => {
  if (!req.query.id || !req.query.name || !req.query.nameColumn) {
    res
      .status(400)
      .json({message: 'Missing parameters (table, name, namecolumn)'});
  } else {
    const collectionId = await importMatch(
      client,
      odcs_client,
      parseInt(req.query.id.toString()),
      req.query.name.toString(),
      req.query.nameColumn.toString(),
      req.query.spatColumn ? req.query.spatColumn.toString() : null
    );

    await finishImport(client, odcs_client, parseInt(req.query.id.toString()));

    res.status(200).json({message: 'importing', collectionId});
  }
});

/**
 * @swagger
 *
 * /import/add:
 *   get:
 *     operationId: getImportAdd
 *     description: Add a geometry to an existing collection (and its attributes)
 *     parameters:
 *       - in: query
 *         name: id
 *         required: true
 *         schema:
 *           type: integer
 *         description: id from the table Matches
 *       - in: query
 *         name: name
 *         required: true
 *         schema:
 *           type: string
 *         description: name for the new collection
 *       - in: query
 *         name: nameColumn
 *         required: true
 *         schema:
 *           type: string
 *         description: name of the column in which the names of geometries are stored in the to be imported table
 *       - in: query
 *         name: collectionId
 *         required: true
 *         schema:
 *           type: integer
 *         description: id of the collection this new geometries should be added to
 *       - in: query
 *         name: spatColumn
 *         required: false
 *         schema:
 *           type: string
 *         description: name of the column in which spatial identifiers of geometries are stored in the to be imported table
 *     produces:
 *       - application/json
 *     responses:
 *       500:
 *         description: error
 *       400:
 *         description: missing parameters
 *       200:
 *         description: success
 *         examples:
 *           application/json: { "message": "importing", collectionId: 123 }
 */
api.get('/import/add', async (req, res) => {
  if (
    !req.query.id ||
    !req.query.name ||
    !req.query.nameColumn ||
    !req.query.collectionId
  ) {
    res.status(400).json({
      message: 'Missing parameters (table, name, namecolumn, collectionId)',
    });
  } else {
    const collectionId = await importMatch(
      client,
      odcs_client,
      parseInt(req.query.id.toString()),
      req.query.name.toString(),
      req.query.nameColumn.toString(),
      req.query.spatColumn ? req.query.spatColumn.toString() : null,
      false,
      parseInt(req.query.collectionId.toString())
    );

    await finishImport(client, odcs_client, parseInt(req.query.id.toString()));

    res.status(200).json({message: 'importing add', collectionId});
  }
});

/**
 * @swagger
 *
 * /import/update:
 *   get:
 *     operationId: getImportUpdate
 *     description: Update a spatial topology within a collection (and add its attributes)
 *     parameters:
 *       - in: query
 *         name: id
 *         required: true
 *         schema:
 *           type: integer
 *         description: id from the table Matches
 *       - in: query
 *         name: name
 *         required: true
 *         schema:
 *           type: string
 *         description: name for the new collection
 *       - in: query
 *         name: nameColumn
 *         required: true
 *         schema:
 *           type: string
 *         description: name of the column in which the names of geometries are stored in the to be imported table
 *       - in: query
 *         name: collectionId
 *         required: true
 *         schema:
 *           type: integer
 *         description: id of the collection this new geometries should be added to
 *       - in: query
 *         name: previousSourceId
 *         required: true
 *         schema:
 *           type: integer
 *         description: id of the old geometry's source
 *       - in: query
 *         name: spatColumn
 *         required: false
 *         schema:
 *           type: string
 *         description: name of the column in which spatial identifiers of geometries are stored in the to be imported table
 *     produces:
 *       - application/json
 *     responses:
 *       500:
 *         description: error
 *       400:
 *         description: missing parameters
 *       200:
 *         description: success
 *         examples:
 *           application/json: { "message": "importing", collectionId: 123 }
 */
api.get('/import/update', async (req, res) => {
  if (
    !req.query.id ||
    !req.query.name ||
    !req.query.nameColumn ||
    !req.query.collectionId ||
    !req.query.previousSourceId
  ) {
    res.status(400).json({
      message:
        'Missing parameters (table, name, namecolumn, collectionId, previousSourceId)',
    });
  } else {
    const collectionId = await importMatch(
      client,
      odcs_client,
      parseInt(req.query.id.toString()),
      req.query.name.toString(),
      req.query.nameColumn.toString(),
      req.query.spatColumn ? req.query.spatColumn.toString() : null,
      false,
      parseInt(req.query.collectionId.toString()),
      'add',
      parseInt(req.query.previousSourceId.toString())
    );

    await finishImport(client, odcs_client, parseInt(req.query.id.toString()));

    res.status(200).json({message: 'importing update', collectionId});
  }
});

/**
 * @swagger
 *
 * /import/merge:
 *   get:
 *     operationId: getImportMerge
 *     description: Add a geometry to an existing collection, with a merge method (skip | replace), see Readme
 *     parameters:
 *       - in: query
 *         name: id
 *         required: true
 *         schema:
 *           type: integer
 *         description: id from the table Matches
 *       - in: query
 *         name: name
 *         required: true
 *         schema:
 *           type: string
 *         description: name for the new collection
 *       - in: query
 *         name: nameColumn
 *         required: true
 *         schema:
 *           type: string
 *         description: name of the column in which the names of geometries are stored in the to be imported table
 *       - in: query
 *         name: collectionId
 *         required: true
 *         schema:
 *           type: integer
 *         description: id of the collection this new geometries should be added to
 *       - in: query
 *         name: previousSourceId
 *         required: true
 *         schema:
 *           type: integer
 *         description: id of the old geometry's source
 *       - in: query
 *         name: method
 *         required: true
 *         schema:
 *           type: string
 *           enum:
 *             - skip
 *             - replace
 *         description: method to use for adding the new geometry
 *       - in: query
 *         name: spatColumn
 *         required: false
 *         schema:
 *           type: string
 *         description: name of the column in which spatial identifiers of geometries are stored in the to be imported table
 *     produces:
 *       - application/json
 *     responses:
 *       500:
 *         description: error
 *       400:
 *         description: missing parameters or unallowed parameter values
 *       200:
 *         description: success
 *         examples:
 *           application/json: { "message": "importing", collectionId: 123 }
 */
api.get('/import/merge', async (req, res) => {
  if (
    !req.query.id ||
    !req.query.name ||
    !req.query.nameColumn ||
    !req.query.collectionId ||
    !req.query.previousSourceId ||
    !req.query.method ||
    (req.query.method.toString() !== 'skip' &&
      req.query.method.toString() !== 'replace')
  ) {
    res.status(400).json({
      message:
        'Missing parameters (table, name, namecolumn, collectionId, previousSourceId, method) or invalid method parameter: skip | replace',
    });
  } else {
    // TODO: store merge action in source
    const collectionId = await importMatch(
      client,
      odcs_client,
      parseInt(req.query.id.toString()),
      req.query.name.toString(),
      req.query.nameColumn.toString(),
      req.query.spatColumn ? req.query.spatColumn.toString() : null,
      false,
      parseInt(req.query.collectionId.toString()),
      req.query.method.toString(),
      parseInt(req.query.previousSourceId.toString())
    );

    await finishImport(client, odcs_client, parseInt(req.query.id.toString()));

    res.status(200).json({message: 'importing update', collectionId});
  }
});

/**
 * @swagger
 *
 * /collections/drop/{id}:
 *   get:
 *     operationId: getCollectionsDrop
 *     description: Drop a collection
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema:
 *           type: integer
 *         description: id from the table Collections
 *     produces:
 *       - application/json
 *     responses:
 *       500:
 *         description: error
 *       400:
 *         description: missing id parameter
 *       200:
 *         description: success
 *         examples:
 *           application/json: { "message": "dropped", id: 123 }
 */
api.get('/collections/drop/:id', async (req, res) => {
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
 * /collections/list:
 *   get:
 *     operationId: getCollectionsList
 *     description: List of collections
 *     produces:
 *       - application/json
 *     responses:
 *       500:
 *         description: error
 *       200:
 *         description: success
 */
api.get('/collections/list', async (req, res) => {
  collectionsList(client).then(result => {
    res.status(200).json(result);
  });
});

/**
 * @swagger
 *
 * /matches/details/{id}:
 *   get:
 *     operationId: getMatchesDetails
 *     description: Get more details on a specific match
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema:
 *           type: integer
 *         description: id from the table Matches
 *     produces:
 *       - application/json
 *     responses:
 *       500:
 *         description: error
 *       400:
 *         description: missing id parameter
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
 * /matches/columns/{id}:
 *   get:
 *     operationId: getMatchesColumns
 *     description: Get list of columns of the match's table
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema:
 *           type: integer
 *         description: id from the table Matches
 *     produces:
 *       - application/json
 *     responses:
 *       500:
 *         description: error
 *       400:
 *         description: missing id parameter
 *       200:
 *         description: success
 */
api.get('/matches/columns/:id', async (req, res) => {
  if (!req.params.id) {
    res.status(400).json({message: 'Missing id parameter'});
  } else {
    // TODO: if id does not exist
    const match = await getMatch(client, parseInt(req.params.id));
    const columns = await getColumns(client, match.table_name);
    res.status(200).json(columns);
  }
});

/**
 * @swagger
 *
 * /matches/geojson/{id}:
 *   get:
 *     operationId: getMatchesGeojson
 *     description: Get geojson representation of a match
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema:
 *           type: integer
 *         description: id from the table Matches
 *     produces:
 *       - application/json
 *     responses:
 *       500:
 *         description: error
 *       400:
 *         description: missing id parameter
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
 * /matches/setxplan/{id}:
 *   get:
 *     operationId: getMatchesSetXPlan
 *     description: Classify an import as xplan
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema:
 *           type: integer
 *         description: id from the table Matches
 *     produces:
 *       - application/json
 *     responses:
 *       500:
 *         description: error
 *       400:
 *         description: missing id parameter
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
 * /matches/setthematic/{id}:
 *   get:
 *     operationId: getMatchesSetThematic
 *     description: Classify an import as thematic
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema:
 *           type: integer
 *         description: id from the table Matches
 *     produces:
 *       - application/json
 *     responses:
 *       500:
 *         description: error
 *       400:
 *         description: missing id parameter
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
