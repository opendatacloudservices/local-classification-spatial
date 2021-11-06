import {Client} from 'pg';
import {getGeometryType} from '../postgis';
import {getMatch} from '../postgres/matches';
import {handleThematic} from './thematic';
import {handleXplan} from './xplan';
import * as similarity from 'string-comparison';

export const isSimilar = async (
  client: Client,
  odcsClient: Client,
  matchId: number
): Promise<null | {
  type: string;
  refId: null | number;
  thematicId?: string;
}> => {
  const match = await getMatch(client, matchId);
  if (match) {
    const geomType = await getGeometryType(client, match.table_name);
    // Level I
    // This is part of an WFS which is already partially classified
    const family = await odcsClient
      .query(
        `WITH download AS (SELECT download_id FROM "DownloadedFiles" WHERE id = $1)
        SELECT thematic, is_plan, id
        FROM
          "DownloadedFiles"
        JOIN
          download
          ON "DownloadedFiles".download_id = download.download_id 
        WHERE
          (no_geom IS NULL OR no_geom = FALSE) AND
          geom_type = $2 AND
          spatial_classification = TRUE`,
        [match.import_id, geomType]
      )
      .then(result => (result.rows ? result.rows : []));

    if (family.length > 0) {
      // check if they are all classified the same
      let isPlan = false;
      let isPlanId: number | null = null;
      let isThematic = false;
      let isThematicId: number | null = null;
      const thematics: number[] = [];
      family.forEach(f => {
        if (f.is_plan) {
          isPlan = true;
          isPlanId = f.id;
        }
        if (f.thematic) {
          isThematic = true;
          thematics.push(f.thematic);
          isThematicId = f.id;
        }
      });
      if (isPlan && !isThematic) {
        return {type: 'xplan', refId: isPlanId};
      } else if (!isPlan && isThematic && thematics.length === 1) {
        return {
          type: 'thematic',
          refId: isThematicId,
          thematicId: thematics[0].toString(),
        };
      }
    }

    // Level II
    // A WFS with the same geomtry structure and similar description has already been classified
    const metaData = await odcsClient
      .query(
        `SELECT
          "Imports".meta_name AS name,
          "Imports".meta_abstract AS abstract,
          "Downloads".format AS format
        FROM
          "DownloadedFiles"
        JOIN
          "Downloads"
          ON "Downloads".id = "DownloadedFiles".download_id
        JOIN
          "Files"
          ON "Downloads".url = "Files".url
        JOIN
          "Imports"
          ON "Imports".id = "Files".dataset_id
        WHERE
          "DownloadedFiles".id = $1`,
        [match.import_id]
      )
      .then(result => (result.rows ? result.rows[0] : null));

    // for now we match similarity only for datasets of the same format, this means a dataset with the same description, but varying format is currently not caught (e.g. shp/wfs)
    const relatives = await odcsClient
      .query(
        `SELECT
        DISTINCT ON ("Files".id, thematic, is_plan)
        "Imports".meta_name AS name,
        "Imports".meta_abstract AS abstract,
        thematic,
        is_plan,
        "DownloadedFiles".id
      FROM
        "DownloadedFiles"
      JOIN
        "Downloads"
        ON "Downloads".id = "DownloadedFiles".download_id
      JOIN
        "Files"
        ON "Downloads".url = "Files".url
      JOIN
        "Imports"
        ON "Imports".id = "Files".dataset_id
      WHERE
        "DownloadedFiles".geom_type = $1 AND
        "Downloads".format = $2 AND
        (no_geom IS NULL OR no_geom = FALSE) AND
        "DownloadedFiles".spatial_classification = TRUE AND
        (thematic IS NOT NULL OR is_plan = TRUE)`,
        [geomType, metaData && metaData.format ? metaData.format : null]
      )
      .then(result => (result.rows ? result.rows : []));

    if (relatives.length > 0) {
      const thematicMatchIds: number[] = [];
      const thematicMatches: {
        [index: number]: {
          name: boolean;
          abstract: boolean;
          matchCount: number;
          id: number;
        }[];
      } = {};
      let isXPlan = false;
      const xPlanMatches: {
        name: boolean;
        abstract: boolean;
        matchCount: number;
        id: number;
      }[] = [];
      const name = metaData.name;
      const abstract = metaData.abstract;
      relatives.forEach(r => {
        const comparison = {
          name: false,
          abstract: false,
          id: r.id,
        };
        let hits = 0;
        if (
          name &&
          r.name &&
          similarity.levenshtein.similarity(name, r.name) > 0.9
        ) {
          comparison.name = true;
          hits += 1;
        }

        if (
          abstract &&
          r.abstract &&
          similarity.levenshtein.similarity(
            abstract.substr(0, 100),
            r.abstract.substr(0, 100)
          ) > 0.9
        ) {
          // TODO: Integrate exact similary as measure
          comparison.abstract = true;
          hits += 2;
        }
        if (hits >= 1) {
          if (r.is_plan) {
            isXPlan = true;
            xPlanMatches.push({...comparison, matchCount: hits});
          } else if (r.thematic) {
            thematicMatchIds.push(r.thematic);
            if (!(r.thematic in thematicMatches)) {
              thematicMatches[r.thematic] = [];
            }
            thematicMatches[r.thematic].push({...comparison, matchCount: hits});
          }
        }
      });
      if (isXPlan && thematicMatchIds.length > 0) {
        // which has the highest matchCount
        let thematicMax = 0;
        let xPlanMax = 0;
        xPlanMatches.forEach(m => {
          if (m.matchCount > xPlanMax) {
            xPlanMax = m.matchCount;
          }
        });
        thematicMatchIds.forEach(id => {
          thematicMatches[id].forEach(m => {
            if (m.matchCount > thematicMax) {
              thematicMax = m.matchCount;
            }
          });
        });
        if (xPlanMax > thematicMax) {
          isXPlan = true;
        } else if (xPlanMax < thematicMax) {
          isXPlan = false;
        } else {
          console.log('xplan or thematic');
          // xplan and thematic are equally good
          // TODO: somehow figure this out
          // TODO: run match with longer descriptions
        }
      }
      if (isXPlan) {
        return {type: 'xplan', refId: xPlanMatches[0].id};
      }
      if (thematicMatchIds.length > 0) {
        // Find the best matches
        let thematicMax = 0;
        let bestMatches: number[] = [];
        let bestRef: number | null = null;
        thematicMatchIds.forEach(id => {
          thematicMatches[id].forEach(m => {
            if (m.matchCount > thematicMax) {
              thematicMax = m.matchCount;
              bestMatches = [id];
              bestRef = m.id;
            } else if (m.matchCount === thematicMax) {
              if (!bestMatches.includes(id)) {
                bestMatches.push(id);
              }
            }
          });
        });
        if (bestMatches.length === 1) {
          return {
            type: 'thematic',
            refId: bestRef,
            thematicId: bestMatches[0].toString(),
          };
        } else {
          console.log('multiple thematic matches');
          // multiple good matches
          // TODO: somehow figure this out
        }
      }
    }
  }
  return null;
};

export const handleSimilar = async (
  client: Client,
  odcsClient: Client,
  similar: {type: string; refId: number | null; thematicId?: string},
  matchId: number
): Promise<void> => {
  if (!similar.refId) {
    similar.refId = -1;
  }
  const match = await getMatch(client, matchId);
  await odcsClient.query(
    'UPDATE "DownloadedFiles" SET similar_id = $1 WHERE id = $2',
    [similar.refId, match?.import_id]
  );
  if (similar.type === 'xplan') {
    await handleXplan(client, odcsClient, matchId);
  } else if (similar.type === 'thematic' && similar.thematicId) {
    await handleThematic(odcsClient, client, matchId, similar.thematicId);
  }
};
