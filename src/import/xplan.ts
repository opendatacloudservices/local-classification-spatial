/*
 * A lot of open spatial data files are small xplan changes
 * Only a handful of geometries with only dates and identifiers as attributes
 */

import {Client} from 'pg';
import type {GeoJson, GeoJsonFeature} from '../types';
import {getGeometryType, getGeomSummary} from '../postgis';
import {geojson, getMatch} from '../postgres/matches';
import {downloadCompletion, setClassified} from '../postgres';
import {saveZip} from '../utils';
import {dropImport} from '.';

const rowLimit = 5;

const terms = ['xplan', 'bebauungsplan'];

export const matchTerm = (term: string): boolean => {
  let found = false;
  terms.forEach(t => {
    if (term.toLowerCase().indexOf(t) > -1) {
      found = true;
    }
  });
  return found;
};

export const isXplan = async (
  client: Client,
  odcs_client: Client,
  match_id: number
): Promise<boolean> => {
  const match = await client
    .query('SELECT * FROM "Matches" WHERE id = $1', [match_id])
    .then(result => result.rows[0]);

  const rowsCount: number = await client
    .query(`SELECT COUNT(*) as row_count FROM ${match.table_name}`)
    .then(result => result.rows[0].row_count);

  if (rowsCount > rowLimit || rowsCount === 0) {
    return false;
  }

  // search for indicators that this is a planning document
  // usually either xplan or xplanung is used somewhere in various lower upper variations

  // the wfs layer is named xplan (something)
  if (matchTerm(match.table_name)) {
    return true;
  }

  // sometimes the xplan definition is referenced in the table
  let foundXplan = false;
  const metadata = await client
    .query(`SELECT * FROM ${match.table_name}`)
    .then(result => result.rows);

  // check if the column name includes xplan
  Object.keys(metadata[0]).forEach(k => {
    if (matchTerm(k)) {
      foundXplan = true;
    }
  });

  // check if the content contains xplan
  metadata.forEach(m => {
    Object.keys(m).forEach(k => {
      if (typeof m[k] === 'string' && matchTerm(m[k])) {
        foundXplan = true;
      }
    });
  });

  if (foundXplan) {
    return true;
  }

  // digging deeper, metadata from DownloadedFiles > Downloads > Files > Imports
  const importMetadata = await odcs_client
    .query(
      `SELECT
        "DownloadedFiles".file,
        "Downloads".url,
        "Files".meta_name,
        "Files".meta_description,
        "Files".meta_function,
        "Imports".meta_name,
        "Imports".meta_abstract
      FROM "DownloadedFiles" 
      JOIN "Downloads" ON "Downloads".id = "DownloadedFiles".download_id
      JOIN "Files" ON "Downloads".url = "Files".url
      JOIN "Imports" ON "Imports".id = "Files".dataset_id
      WHERE "DownloadedFiles".id = $1`,
      [match.import_id]
    )
    .then(result => result.rows);

  // check if the content contains xplan
  importMetadata.forEach(m => {
    Object.keys(m).forEach(k => {
      if (typeof m[k] === 'string' && matchTerm(m[k])) {
        foundXplan = true;
      }
    });
  });

  if (foundXplan) {
    return true;
  }

  return false;
};

/*
 * planing documents are optimised and moved to storage:
 * zipped geojson with fixed geometries and all attributes
 * name is the id of the DownloadedFile table
 * the bounding box, geometry type, and xplan (true)
 * is send to the DownloadedFiles table
 */

export const handleXplan = async (
  client: Client,
  odcsClient: Client,
  match_id: number
): Promise<void> => {
  const match = await getMatch(client, match_id);
  const geom = await getGeomSummary(client, match.table_name);
  const geomType = await getGeometryType(client, match.table_name);

  const download_id = await setClassified(
    odcsClient,
    match.import_id,
    true,
    geomType,
    true,
    geom.centroid,
    geom.bbox
  );

  downloadCompletion(odcsClient, download_id);

  const geojsonObj = geojson(client, match.id);

  saveZip(
    JSON.stringify(geojsonObj),
    match.import_id + '.geojson',
    (process.env.PLANNING_LOCATION || '') + match.import_id + '.zip'
  );

  // Drop temporary tables
  await dropImport(client, match.table_name);
  await client.query('DELETE FROM "Matches" WHERE id = $1', [match_id]);

  // TODO: remove original file?
};
