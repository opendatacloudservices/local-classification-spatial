import * as dotenv from 'dotenv';
import * as path from 'path';
// get environmental variables
dotenv.config({path: path.join(__dirname, '../.env')});

import {Client} from 'pg';
import * as notifier from 'node-notifier';
import fetch from 'node-fetch';

import {importMatch, finishImport, getMissingIds} from './import/index';
import {getColumns, getNext, getImportQueue} from './postgres/index';
import {matchGeometries, matchMatrix} from './postgis/index';
import {
  list as matchesList,
  details as matchesDetails,
  geojsonClean as matchesGeojsonClean,
  getMatch,
  removeMissingTables,
} from './postgres/matches';
import {
  list as collectionsList,
  drop as dropCollection,
} from './postgres/collections';

import {api, catchAll, port} from '@opendatacloudservices/local-microservice';

import {logError, addToken} from '@opendatacloudservices/local-logger';
import {handleXplan, isXplan} from './import/xplan';
import {handleThematic, list as thematicList} from './import/thematic';
import {isBbox} from './import/bbox';
import {isSimilar} from './import/similar';
import {checkImport, processImport} from './import/pipeline';
import {Response} from 'express';
import {fetchAgain} from './utils';

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
const odcsClient = new Client({
  user: process.env.ODCS_PGUSER,
  host: process.env.ODCS_PGHOST,
  database: process.env.ODCS_PGDATABASE,
  password: process.env.ODCS_PGPASSWORD,
  port: parseInt(process.env.ODCS_PGPORT || '5432'),
});
odcsClient.connect().catch((err: Error) => {
  logError({message: err});
});

let active = false;
const queueLimit = 100;
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
api.get('/next', async (_req, res) => {
  // if there are too many geometries await approval import is stopped
  const queueSize = await getImportQueue(client);
  if (queueSize < queueLimit) {
    active = true;
    // get next import from the opendataservice database
    const next = await getNext(odcsClient);
    if (next) {
      await processImport(next, client, odcsClient, <Response>res);
      if (!res.headersSent) {
        res.status(200).json({message: 'import completed. starting next'});
      }
      await fetchAgain(`http://localhost:${port}/next`, <Response>res);
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
 * /stop:
 *   get:
 *     operationId: getStop
 *     description: stop importing
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
api.get('/stop', async (req, res) => {
  if (active) {
    active = false;
    res.status(200).json({message: 'import stopping'});
  } else {
    res.status(200).json({message: 'import was not active'});
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
    await checkImport(matches[m], client, odcsClient);
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
    if (match) {
      const matching = await matchGeometries(client, match.table_name);
      res.status(200).json(matching);
    } else {
      res.status(404).json({
        message: 'Match id not found',
      });
    }
  }
});

/**
 * @swagger
 *
 * /similarity/{id}:
 *   get:
 *     operationId: getSimilarity
 *     description: check a specific match for similar downloads
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
api.get('/similarity/:id', async (req, res) => {
  if (!req.params.id) {
    res.status(400).json({
      message: 'Missing parameters (id)',
    });
  } else {
    const similar = await isSimilar(
      client,
      odcsClient,
      parseInt(req.params.id.toString())
    );
    if (similar) {
      // await handleSimilar(
      //   client,
      //   odcsClient,
      //   similar,
      //   parseInt(req.params.id.toString())
      // );
      res.status(200).json(similar);
    } else {
      res.status(404).json({
        message: 'Match id not found',
      });
    }
  }
});

/**
 * @swagger
 *
 * /xplan/{id}:
 *   get:
 *     operationId: getXplan
 *     description: check if a specific match is an xplan
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
 *           application/json: { "xplan": true }
 *           application/json: { "xplan": false }
 */
api.get('/xplan/:id', async (req, res) => {
  if (!req.params.id) {
    res.status(400).json({
      message: 'Missing parameters (id)',
    });
  } else {
    const xplan = await isXplan(
      client,
      odcsClient,
      parseInt(req.params.id.toString())
    );
    res.status(200).json({message: xplan});
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
    res.status(200).json({message: 'importing'});

    await importMatch(
      client,
      odcsClient,
      parseInt(req.query.id.toString()),
      req.query.name.toString(),
      req.query.nameColumn.toString(),
      req.query.spatColumn ? req.query.spatColumn.toString() : null
    );

    await finishImport(client, odcsClient, parseInt(req.query.id.toString()));
    console.log('finished importing');
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
  if (!req.query.id || !req.query.nameColumn || !req.query.collectionId) {
    res.status(400).json({
      message: 'Missing parameters (table, name, namecolumn, collectionId)',
    });
  } else {
    res.status(200).json({message: 'importing add'});

    await importMatch(
      client,
      odcsClient,
      parseInt(req.query.id.toString()),
      '',
      req.query.nameColumn.toString(),
      req.query.spatColumn ? req.query.spatColumn.toString() : null,
      false,
      parseInt(req.query.collectionId.toString()),
      'add'
    );

    await finishImport(client, odcsClient, parseInt(req.query.id.toString()));

    console.log('import finished');
  }
});

/**
 * @swagger
 *
 * /test/add:
 *   get:
 *     operationId: getTestAdd
 *     description: Test the adding of a geometry to an existing collection (and its attributes)
 *     parameters:
 *       - in: query
 *         name: id
 *         required: true
 *         schema:
 *           type: integer
 *         description: id from the table Matches
 *       - in: query
 *         name: collectionId
 *         required: true
 *         schema:
 *           type: integer
 *         description: id of the collection this new geometries should be added to
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
api.get('/test/add', async (req, res) => {
  if (!req.query.id || !req.query.collectionId) {
    res.status(400).json({
      message: 'Missing parameters (id, collectionId)',
    });
  } else {
    const match = await getMatch(client, parseInt(req.query.id.toString()));
    if (match) {
      const matrix = await matchMatrix(
        client,
        match.table_name,
        parseInt(req.query.collectionId.toString())
      );
      const missing = await getMissingIds(
        client,
        match.table_name,
        matrix.map(m => m[0][0])
      );
      res.status(200).json({ids: missing.map(m => m.id).join(',')});
    } else {
      res.status(200).json({message: 'match not found'});
    }
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
    !req.query.collectionId
  ) {
    res.status(400).json({
      message:
        'Missing parameters (table, name, namecolumn, collectionId, previousSourceId)',
    });
  } else {
    res.status(200).json({message: 'importing update'});

    await importMatch(
      client,
      odcsClient,
      parseInt(req.query.id.toString()),
      req.query.name.toString(),
      req.query.nameColumn.toString(),
      req.query.spatColumn ? req.query.spatColumn.toString() : null,
      false,
      parseInt(req.query.collectionId.toString()),
      'new'
    );

    await finishImport(client, odcsClient, parseInt(req.query.id.toString()));
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
    !req.query.method ||
    (req.query.method.toString() !== 'skip' &&
      req.query.method.toString() !== 'replace')
  ) {
    res.status(400).json({
      message:
        'Missing parameters (table, name, namecolumn, collectionId, previousSourceId, method) or invalid method parameter: skip | replace',
    });
  } else {
    res.status(200).json({message: 'importing update'});

    await importMatch(
      client,
      odcsClient,
      parseInt(req.query.id.toString()),
      req.query.name.toString(),
      req.query.nameColumn.toString(),
      req.query.spatColumn ? req.query.spatColumn.toString() : null,
      false,
      parseInt(req.query.collectionId.toString()),
      req.query.method.toString()
    );

    await finishImport(client, odcsClient, parseInt(req.query.id.toString()));
  }
});

// TODO: Before update compare the potential matches Endpoint for comparison geojson

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
 * /missingtables/drop:
 *   get:
 *     operationId: getMissingTablesDrop
 *     description: Drop tables without matches
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
 *           application/json: { "message": "dropped" }
 */
api.get('/missingtables/drop', async (req, res) => {
  await removeMissingTables(client);
  res.status(200).json({message: 'dropped'});
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
    matchesDetails(odcsClient, parseInt(req.params.id)).then(result => {
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
    const match = await getMatch(client, parseInt(req.params.id));
    if (match) {
      const columns = await getColumns(client, match.table_name);
      res.status(200).json(columns);
    } else {
      res.status(404).json({message: 'id not found'});
    }
  }
});

/**
 * @swagger
 *
 * /matches/geojson/{id}:
 *   get:
 *     operationId: getMatchesGeojson
 *     description: Get geojson representation of a match. Delivers a maximum of 100 geometries.
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
    matchesGeojsonClean(client, parseInt(req.params.id), 100).then(result => {
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
    handleXplan(client, odcsClient, parseInt(req.params.id)).then(() => {
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
      odcsClient,
      client,
      parseInt(req.params.id),
      req.query.thematic.toString()
    )
      .then(() => {
        res
          .status(200)
          .json({message: 'Classification success!', id: req.params.id});
      })
      .catch(err => {
        logError({message: err});
        res.status(400).json({message: 'Error', id: req.params.id, err});
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
api.get('/thematic/topics', async (req, res) => {
  if (!req.query.thematic) {
    thematicList(odcsClient).then(result => {
      res.status(200).json(result);
    });
  }
});

/**
 * @swagger
 *
 * /check-bbox/{id}:
 *   get:
 *     operationId: getCheck-Bbox
 *     description: check if match is a bbox
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
api.get('/check-bbox/:id', async (req, res) => {
  if (!req.params.id) {
    res.status(400).json({message: 'Missing id parameter'});
  } else {
    const test = await isBbox(client, parseInt(req.params.id.toString()));
    res.status(200).json({isBbox: test});
  }
});

catchAll();
