import {Client} from 'pg';
import {dropImport} from '.';
import {getGeometryType, getGeomSummary} from '../postgis';
import {downloadCompletion, setClassified} from '../postgres';
import {geojson, getMatch} from '../postgres/matches';
import {saveZip} from '../utils';

export const list = (
  odcsClient: Client
): Promise<{name: string; count: number}[]> => {
  return odcsClient
    .query(
      'SELECT COUNT(*) AS count, thematic AS name FROM "DownloadedFiles" GROUP BY thematic'
    )
    .then(result => result.rows);
};

export const handleThematic = async (
  odcsClient: Client,
  client: Client,
  id: number,
  thematic: string
): Promise<void> => {
  const match = await getMatch(client, id);
  const geomType = await getGeometryType(client, match.table_name);
  const geom = await getGeomSummary(client, match.table_name);

  const download_id = await setClassified(
    odcsClient,
    match.import_id,
    true,
    geomType,
    false,
    geom.centroid,
    geom.bbox,
    thematic
  );

  downloadCompletion(odcsClient, download_id);

  const geojsonObj = geojson(client, match.id);

  saveZip(
    JSON.stringify(geojsonObj),
    match.import_id + '.geojson',
    (process.env.THEMATIC_LOCATION || '') + match.import_id + '.zip'
  );

  // Drop temporary tables
  await dropImport(client, match.table_name);
  await client.query('DELETE FROM "Matches" WHERE id = $1', [id]);

  // TODO: remove original file?
};
