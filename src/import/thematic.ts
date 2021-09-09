import {Client} from 'pg';
import {dropImport} from '.';
import {getGeometryType, getGeomSummary} from '../postgis';
import {downloadCompletion, setClassified} from '../postgres';
import {geojson, getMatch} from '../postgres/matches';
import {saveZip} from '../utils';

export const list = (
  odcsClient: Client
): Promise<
  {
    name: string;
    count: number;
    group: string;
    id: number;
  }[]
> => {
  return odcsClient
    .query(
      `SELECT
        COUNT(*) AS count,
        "Thematics".id AS id,
        "Thematics".thematic AS name,
        "Thematics".subthematic AS group
      FROM
        "Thematics"
      JOIN
        "DownloadedFiles"
        ON
          "DownloadedFiles".thematic = "Thematics".id
      GROUP BY "Thematics".id`
    )
    .then(result => result.rows);
};

export const createThematic = (
  odcsClient: Client,
  thematic: string,
  subthematic: string
): Promise<number> => {
  return odcsClient
    .query(
      'INSERT INTO "Thematics" (thematic, subthematic) VALUES ($1, $2) RETURNING id',
      [thematic, subthematic]
    )
    .then(result => result.rows[0].id);
};

export const handleThematic = async (
  odcsClient: Client,
  client: Client,
  id: number,
  thematic: string,
  subthematic?: string
): Promise<void> => {
  const match = await getMatch(client, id);
  const geomType = await getGeometryType(client, match.table_name);
  const geom = await getGeomSummary(client, match.table_name);

  let thematicId = parseInt(thematic);
  if (isNaN(thematicId) || thematicId.toString() !== thematic) {
    thematicId = await createThematic(odcsClient, thematic, subthematic || '');
  }

  const download_id = await setClassified(
    odcsClient,
    match.import_id,
    true,
    geomType,
    false,
    geom.centroid,
    geom.bbox,
    thematicId
  );

  await downloadCompletion(odcsClient, download_id);

  const geojsonObj = await geojson(client, match.id);

  await saveZip(
    JSON.stringify(geojsonObj),
    match.import_id + '.geojson',
    (process.env.THEMATIC_LOCATION || '') + match.import_id + '.zip'
  );

  // Drop temporary tables
  await dropImport(client, match.table_name);
  await client.query('DELETE FROM "Matches" WHERE id = $1', [id]);

  // TODO: remove original file?
};
