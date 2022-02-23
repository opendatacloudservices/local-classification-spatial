import {DBMatch, Download} from '../types';
import {existsSync} from 'fs';
import {logError} from '@opendatacloudservices/local-logger';
import {
  generateTableName,
  getColumns,
  rowCount,
  saveBigFile,
  setClassified,
  tableExists,
} from '../postgres';
import {Client} from 'pg';
import type {Response} from 'express';
import {sizeLimit} from '../file';
import {
  dropImport,
  ogr,
  createSource,
  getImportValues,
  importValues,
  saveMatch,
  saveColumns,
} from '.';
import {
  hasGeom,
  cleanGeometries,
  matchGeometries,
  getGeomSummary,
  getGeometryType,
} from '../postgis';
import {wait} from '../utils';
import {isBbox, handleBbox} from './bbox';
import {isSimilar, handleSimilar} from './similar';
import {isXplan, handleXplan} from './xplan';
import {getFromImportID} from '../postgres/downloads';
import {getMatch} from '../postgres/matches';

export const checkImport = async (
  match: DBMatch,
  client: Client,
  odcsClient: Client
): Promise<void> => {
  const geomMatch = await matchGeometries(client, match.table_name);
  if (geomMatch.collection_id) {
    const download = await getFromImportID(odcsClient, match.import_id);

    // TODO: identify name columns and add to names array

    const source_id = await createSource(
      client,
      odcsClient,
      download.downloaded,
      match.import_id,
      geomMatch.collection_id,
      JSON.stringify(geomMatch.process) || null
    );

    const columns = await getColumns(client, match.table_name);
    const values = await getImportValues(client, match.table_name, columns);

    await importValues(
      client,
      match.table_name,
      values,
      columns,
      source_id,
      geomMatch.collection_id
    );

    await saveColumns(client, columns, source_id);

    await setClassified(
      odcsClient,
      match.import_id,
      true,
      null,
      false,
      null,
      null,
      null
    );
    await dropImport(client, match.table_name);
    return;
  }

  if (await isXplan(client, odcsClient, match.id)) {
    await handleXplan(client, odcsClient, match.id);
    return;
  }

  if (await isBbox(client, match.id)) {
    await handleBbox(odcsClient, client, match.id);
    return;
  }

  const similar = await isSimilar(client, odcsClient, match.id);
  if (similar) {
    await handleSimilar(client, odcsClient, similar, match.id);
    return;
  }

  await saveMatch(
    client,
    match.import_id,
    match.file,
    geomMatch,
    'no-match',
    match.table_name,
    undefined,
    match.id
  );
};

export const processImport = async (
  next: Download,
  client: Client,
  odcsClient: Client,
  res: Response
): Promise<void> => {
  const file = process.env.DOWNLOAD_LOCATION + next.file;

  // check if the file exists, sometimes things go wrong, if not reset download
  if (!existsSync(file)) {
    logError({message: 'file does not exist', file, id: next.id});
    await setClassified(odcsClient, next.id);
    res.status(200).json({message: 'importing, file does not exist'});
    return;
  }

  // files exceeding our import limit are being ignored
  if (!sizeLimit(file)) {
    // file is too big, information gets stored in table "Matches"
    await saveBigFile(client, odcsClient, next.id);
    await setClassified(odcsClient, next.id);
    res.status(200).json({message: 'importing, file too large'});
    return;
  }

  // create a temporary table and import the file through ogr
  const tableName = generateTableName(file);

  // this is a security precaution, but this should not ever happen
  const exists = await tableExists(client, tableName);
  if (exists) {
    await dropImport(client, tableName);
  }

  try {
    // import througth gdal ogr
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
        res.status(200).json({message: 'importing, success(ish)'});

        // clean the geometries (multi > single + fixing)
        await cleanGeometries(client, tableName);
        await wait(2500);

        const geomSummary = await getGeomSummary(client, tableName);
        await setClassified(
          odcsClient,
          next.id,
          true,
          await getGeometryType(client, tableName),
          false,
          geomSummary.centroid,
          geomSummary.bbox,
          null
        );

        const match_id = await saveMatch(
          client,
          next.id,
          next.file,
          {process: undefined, collection_id: -1},
          'temp',
          tableName
        );

        const dbMatch = await getMatch(client, match_id);
        if (dbMatch) {
          await checkImport(dbMatch, client, odcsClient);
        }
      } else {
        await setClassified(odcsClient, next.id, false);
        await dropImport(client, tableName);
        res.status(200).json({message: 'no geom'});
      }
    } else {
      console.log('weird 1');
      await setClassified(odcsClient, next.id, false);
      await dropImport(client, tableName);
      res.status(200).json({message: 'weird file'});
    }
  } catch (err) {
    logError({err, id: next.id, tableName});
    await setClassified(odcsClient, next.id);
    if (
      err &&
      typeof err === 'object' &&
      'stderr' in err &&
      (JSON.stringify(err).indexOf('Unable to open datasource') > -1 ||
        JSON.stringify(err).indexOf(
          'transform coordinates, source layer has no'
        ) > -1)
    ) {
      await odcsClient.query(
        'UPDATE "DownloadedFiles" SET corrupted = TRUE WHERE id = $1',
        [next.id]
      );
      // Update already set files (spatial_classification = TRUE AND (no_geom IS NULL OR no_geom = FALSE) AND (is_plan = FALSE OR is_plan IS NULL) AND is_thematic IS NULL)
      await dropImport(client, tableName);
    } else {
      logError({
        message: 'Unknown import error',
        err,
        DownloadedFilesId: next.id,
      });
    }
    if (!res.headersSent) {
      res.status(200).json({message: 'weird file'});
    }
  }
};
