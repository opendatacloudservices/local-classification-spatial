import {Client} from 'pg';
import {dropImport} from '.';
import {geojson} from '../file';
import {getGeometryType, getGeomSummary} from '../postgis';
import {downloadCompletion, setClassified} from '../postgres';
import {getMatch} from '../postgres/matches';

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
  if (!match) {
    throw Error('match_id not found');
  }
  const geomType = await getGeometryType(client, match.table_name);
  const geom = await getGeomSummary(client, match.table_name);

  let thematicId = parseInt(thematic);
  if (isNaN(thematicId) || thematicId.toString() !== thematic) {
    thematicId = await createThematic(odcsClient, thematic, subthematic || '');
  }

  await setClassified(
    odcsClient,
    match.import_id,
    true,
    geomType,
    false,
    geom.centroid,
    geom.bbox,
    thematicId
  );

  await downloadCompletion(odcsClient, match.import_id);

  await geojson(
    process.env.DOWNLOAD_LOCATION + match.file,
    (process.env.THEMATIC_LOCATION || '') + match.import_id
  );

  // Drop temporary tables
  await dropImport(client, match.table_name);
  await client.query('DELETE FROM "Matches" WHERE id = $1', [id]);

  // TODO: remove original file?
};
