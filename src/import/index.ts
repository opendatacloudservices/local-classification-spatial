import {exec} from 'child_process';
import {Client} from 'pg';
import {
  fidMatrix,
  getGeometryType,
  getGeomSummary,
  idMatrix,
  matchMatrix,
} from '../postgis/index';
import {getColumns, setClassified} from '../postgres';
import {create as createCollection} from '../postgres/collections';
import {getFromImportID} from '../postgres/downloads';
import {getMatch} from '../postgres/matches';
import type {Columns, Results, Match, Matrix} from '../types';
import {date2timestamp} from '../utils';
import * as fastcsv from 'fast-csv';
import {createWriteStream} from 'fs';

export const ogr = (
  filename: string,
  tableName: string
): Promise<{
  out: string;
  err: string;
}> => {
  return new Promise((resolve, reject) => {
    exec(
      `ogr2ogr \
      -f "PostgreSQL" \
      "PG:\
        host=${process.env.PGHOST} \
        user=${process.env.PGUSER} \
        dbname=${process.env.PGDATABASE} \
        password=${process.env.PGPASSWORD}" \
      "${filename}" \
      -lco GEOMETRY_NAME=geom_3857 \
      -lco FID=fid \
      -lco SPATIAL_INDEX=GIST \
      -nln ${tableName} \
      -t_srs EPSG:3857`,
      (err, stdout, stderr) => {
        if (err) {
          reject({err, stdout, stderr});
        }
        resolve({
          out: stdout,
          err: stderr,
        });
      }
    );
  });
};

export const dropImportById = (client: Client, id: number): Promise<void> => {
  return getMatch(client, id).then(match => {
    if (!match) {
      throw Error('match_id not found');
    }
    return dropImport(client, match.table_name);
  });
};

export const dropImport = async (
  client: Client,
  tableName: string
): Promise<void> => {
  return client
    .query(`DROP TABLE IF EXISTS "${tableName}"`)
    .then(() => client.query(`DROP TABLE IF EXISTS "${tableName}_cln"`))
    .then(() =>
      client.query('DELETE FROM "Matches" WHERE table_name = $1', [tableName])
    )
    .then(() => {});
};

// TODO: separate all collection functions into postgres/collection.ts

// const columns = filterColumns(await getColumns(client, tableName));

export const getImportValues = async (
  client: Client,
  tableName: string,
  columns: Columns
): Promise<Results> => {
  return client
    .query(
      `SELECT fid, ${columns
        .map(c => `"${c.name}"`)
        .join(', ')} FROM ${tableName}`
    )
    .then(result => result.rows);
};

export const checkValues = async (
  client: Client,
  values: Results,
  columns: Columns,
  source_id: number,
  updated: Date,
  matrix: Matrix
): Promise<boolean> => {
  let hasNewData = true;

  const valuesMatrixMap = fidMatrix(matrix);

  // check if timestamp already exists in the database
  const exists = await client
    .query(
      'SELECT * FROM "GeometryAttributes" WHERE source_id = $1 AND updated = TO_TIMESTAMP($2, \'YYYY-MM-DD HH:MI:SS\') LIMIT 1',
      [source_id, date2timestamp(updated)]
    )
    .then(result => result.rows.length >= 1);

  const timestamps: Date[] = [];

  if (exists) {
    // normally we would simply ignore data with the same timestamp, but with open data, who knows, maybe they forgot to update the timestamp
    timestamps.push(updated);
  } else {
    // get the closest timestamp before and after the new timestamp
    await client
      .query(
        'SELECT DISTINCT ON (updated) updated FROM "GeometryAttributes" WHERE source_id = $1 ORDER BY updated ASC',
        [source_id]
      )
      .then(result => {
        let before = -Number.MAX_VALUE;
        let beforeId: number | null = null;
        let after = Number.MAX_VALUE;
        let afterId: number | null = null;
        result.rows.forEach((r, ri) => {
          const diff = r.updated.getTime() - updated.getTime();
          if (diff < 0 && diff > before) {
            before = diff;
            beforeId = ri;
          } else if (diff > 0 && diff < after) {
            after = diff;
            afterId = ri;
          }
        });
        const timestamps: Date[] = [];
        if (beforeId) {
          timestamps.push(result.rows[beforeId].updated);
        }
        if (afterId) {
          timestamps.push(result.rows[afterId].updated);
        }
      });
  }

  // check if there is a change to the previous or next timestamp
  for (let t = 0; t < timestamps.length; t += 1) {
    const change = await checkAgainstTimestamp(
      client,
      valuesMatrixMap,
      columns,
      values,
      updated,
      source_id
    );
    if (change) {
      hasNewData = true;
    }
  }

  return hasNewData;
};

export const checkAgainstTimestamp = async (
  client: Client,
  matrixKeys: {[index: number]: number},
  columns: Columns,
  values: Results,
  timestamp: Date,
  source_id: number
): Promise<boolean> => {
  let hasChanged = false;

  for (let ci = 0; ci < columns.length; ci += 1) {
    const currentValues = await client
      .query(
        'SELECT * FROM "GeometryAttributes" WHERE key = $1 AND source_id = $3',
        [columns[ci].name, timestamp, source_id]
      )
      .then(r => r.rows);

    currentValues.forEach(c => {
      const newValue: string | number | string[] | number[] | null =
        values[matrixKeys[c.fid]][columns[ci].name];
      switch (columns[ci].type) {
        case 'ARRAY':
          if (Array.isArray(newValue)) {
            if (columns[ci].udt.indexOf('float') >= 0) {
              if (
                !checkArrayFloat(
                  newValue!.map(v => parseFloat(v.toString())),
                  c.int_a_val,
                  checkFloat
                )
              ) {
                hasChanged = true;
              }
            } else if (columns[ci].udt.indexOf('int') >= 0) {
              if (
                !checkArrayFloat(
                  newValue!.map(v => parseInt(v.toString())),
                  c.int_a_val,
                  checkInt
                )
              ) {
                hasChanged = true;
              }
            } else if (
              columns[ci].udt.indexOf('text') >= 0 ||
              columns[ci].udt.indexOf('char') >= 0
            ) {
              if (!checkArrayText(newValue, c.int_a_val)) {
                hasChanged = true;
              }
            }
          }
          break;
        case 'smallint':
        case 'integer':
          // string
          if (!checkFloat(parseInt(newValue!.toString()), c.int_val)) {
            hasChanged = true;
          }
          break;
        case 'double precision':
          // string
          if (!checkFloat(parseFloat(newValue!.toString()), c.float_val)) {
            hasChanged = true;
          }
          break;
        default:
          // string
          if (!checkText(newValue!.toString(), c.str_val)) {
            hasChanged = true;
          }
          break;
      }
    });
  }

  return hasChanged;
};

// the next two functions are almost identical, but writing switches and typings generate much more code
export const checkArrayFloat = (
  a1: number[],
  a2: number[],
  check: (n1: number, n2: number) => boolean
): boolean => {
  if (a1.length !== a2.length) {
    return false;
  }
  let theSame = true;
  // first hope for the best that the ordering is the same
  a1.forEach((a, ai) => {
    if (!check(a, a2[ai])) {
      theSame = false;
    }
  });

  if (!theSame) {
    // let's see if we find a match for all elements
    let allMatched = true;
    a1.forEach(a => {
      let thisMatched = false;
      a2.forEach(b => {
        if (check(a, b)) {
          thisMatched = true;
        }
      });
      if (!thisMatched) {
        allMatched = false;
      }
    });
    return allMatched;
  }

  return theSame;
};

export const checkArrayText = (a1: string[], a2: string[]): boolean => {
  if (a1.length !== a2.length) {
    return false;
  }
  let theSame = true;
  // first hope for the best that the ordering is the same
  a1.forEach((a, ai) => {
    if (!checkText(a, a2[ai])) {
      theSame = false;
    }
  });

  if (!theSame) {
    // let's see if we find a match for all elements
    let allMatched = true;
    a1.forEach(a => {
      let thisMatched = false;
      a2.forEach(b => {
        if (checkText(a, b)) {
          thisMatched = true;
        }
      });
      if (!thisMatched) {
        allMatched = false;
      }
    });
    return allMatched;
  }

  return theSame;
};

export const checkFloat = (v1: number, v2: number): boolean => {
  if (v1 === v2) {
    return true;
  }
  // check if only precision has changed
  const v1l = v1.toString().split('.')[1].length;
  const v2l = v2.toString().split('.')[1].length;
  if (v1l === v2l) {
    return false;
  }
  const sortList = [
    [v1, v1l],
    [v2, v2l],
  ];
  sortList.sort((a, b) => a[1] - b[2]);

  // properly rounded
  let rounded = parseFloat(
    (sortList[1][0] + Number.EPSILON).toFixed(sortList[0][1])
  );
  if (rounded === sortList[0][0]) {
    return true;
  }

  // not properly rounded on e.g. 0.5
  rounded = parseFloat(sortList[1][0].toFixed(sortList[0][1]));
  if (rounded === sortList[0][0]) {
    return true;
  }

  const cutOff = sortList[1][0]
    .toString()
    .substr(0, sortList[0][0].toString().length);
  // cut off instead of rounded
  if (parseFloat(cutOff) === sortList[0][0]) {
    return true;
  }

  return false;
};

export const checkText = (v1: string, v2: string): boolean => {
  // IDEA: for more inclusive testing levenshtein or something could be added?!
  return v1.trim().toLowerCase() === v2.trim().toLowerCase();
};

export const checkInt = (v1: number, v2: number): boolean => {
  return v1 === v2;
};

export const createSource = async (
  client: Client,
  odcsClient: Client,
  updated: Date,
  import_id: number | null,
  collection_id: number,
  hausdorff?: string | null
): Promise<number> => {
  let insertValues: (boolean | string | number | null | Date)[] = [];

  if (!import_id) {
    insertValues = [null, updated, true, null];
  } else {
    const importData = await odcsClient
      .query(
        `SELECT "Imports".meta_license FROM "DownloadedFiles"
        JOIN "Downloads" ON "DownloadedFiles".download_id = "Downloads".id
        JOIN "Files" ON "Downloads".url = "Files".url
        JOIN "Imports" ON "Files".dataset_id = "Imports".id
        WHERE "DownloadedFiles".id = $1`,
        [import_id]
      )
      .then(r => r.rows[0]);

    insertValues = [
      import_id,
      updated,
      false,
      importData && importData.meta_license
        ? importData.meta_license
        : 'unknown',
    ];
  }

  insertValues.push(collection_id, hausdorff || null);

  return client
    .query(
      'INSERT INTO "Sources" (import_id, updated, manual, copyright, collection_id, hausdorff) VALUES ($1, $2, $3, $4, $5, $6) RETURNING id',
      insertValues
    )
    .then(r => r.rows[0].id);
};

export const commitValues = async (
  client: Client,
  valueString: string,
  inserts: (number | string | null | number[] | string[] | Date)[],
  targetType: string
): Promise<void> => {
  await client.query(
    `INSERT INTO
    "GeometryAttributes"
    (
      fid,
      source_id,
      key,
      ${targetType}
    )
    VALUES
    ${valueString}`,
    inserts
  );
};

const ignoreValueNames = ['fid', 'geom', 'geom_3857', 'x_coord', 'y_coord'];

// TODO: store in static CSV files
// TODO: rethink if we want/need points
export const importValues = async (
  client: Client,
  tableName: string,
  values: Results,
  columns: Columns,
  source_id: number,
  collection_id: number
): Promise<void> => {
  const matrix = await matchMatrix(client, tableName, collection_id);
  const valuesMatrixMap = fidMatrix(matrix);

  const csvStream = fastcsv.format({headers: true});
  const csvName = (process.env.DATA_LOCATION || '') + source_id + '.csv';
  csvStream.pipe(createWriteStream(csvName));

  for (let vi = 0; vi < values.length; vi += 1) {
    const v = values[vi];
    const row: {[key: string]: string | number | null | boolean} = {
      oFid: v.fid,
      mFid:
        Object.keys(matrix).length === 0
          ? parseInt(v.fid!.toString())
          : valuesMatrixMap[v.fid!.toString()],
    };

    for (let c = 0; c < columns.length; c += 1) {
      if (!ignoreValueNames.includes(columns[c].name)) {
        row[columns[c].name] = v[columns[c].name];
      }
    }

    csvStream.write(row);
  }

  csvStream.end();
};

export const saveColumns = (
  client: Client,
  columns: {name: string; type: string}[],
  sourceId: number
): Promise<void> => {
  if (columns.length === 0) {
    return Promise.resolve();
  }
  return client
    .query(
      `INSERT INTO "SourceColumns" (name, type, source_id) VALUES ${columns
        .map((_c, ci) => `($${ci * 3 + 1}, $${ci * 3 + 2}, $${ci * 3 + 3})`)
        .join(',')}`,
      columns.map(c => [c.name, c.type, sourceId]).flat()
    )
    .then(() => {});
};

export const finishImport = async (
  client: Client,
  odcsClient: Client,
  id: number
): Promise<void> => {
  const match = await getMatch(client, id);
  if (!match) {
    throw Error('match_id not found');
  }

  const geom = await getGeomSummary(client, match.table_name);
  const geomType = await getGeometryType(client, match.table_name);

  await setClassified(
    odcsClient,
    match.import_id,
    true,
    geomType,
    false,
    geom.centroid,
    geom.bbox,
    null
  );
  await dropImportById(client, id);
};

export const saveMatch = (
  client: Client,
  import_id: number,
  file: string,
  match: Match,
  message: string,
  tableName: string,
  difference?: {
    source_id: number;
    target_id: number;
    dist: number;
  }[],
  match_id?: number
): Promise<number> => {
  if (match_id) {
    return client
      .query(
        'UPDATE "Matches" SET matches = $1, message = $2, matches_count = $3, difference = $4 WHERE id = $5',
        [
          match.process?.sources,
          message,
          match.process?.sourceCount,
          difference ? JSON.stringify(difference) : null,
          match_id,
        ]
      )
      .then(() => match_id);
  } else {
    return client
      .query(
        'INSERT INTO "Matches" (import_id, file, matches, message, matches_count, table_name, difference) VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id',
        [
          import_id,
          file,
          match.process?.sources,
          message,
          match.process?.sourceCount,
          tableName,
          difference ? JSON.stringify(difference) : null,
        ]
      )
      .then(result => result.rows[0].id);
  }
};

export const getMissingIds = (
  client: Client,
  tableName: string,
  ids: number[]
): Promise<{id: number; fid: number}[]> => {
  return client
    .query(
      `SELECT id, fid FROM ${tableName}_cln${
        ids.length > 0 ? ` WHERE id NOT IN (${ids.join(',')})` : ''
      }`
    )
    .then(result =>
      result.rows
        ? result.rows.map(r => {
            return {id: r.id, fid: r.fid};
          })
        : []
    );
};

export const getMaxFid = (
  client: Client,
  tableName: string
): Promise<number> => {
  return client
    .query(`SELECT MAX(fid) AS max_fid FROM ${tableName}`)
    .then(result => result.rows[0].max_fid);
};

export const addGeometries = async (
  client: Client,
  tableName: string,
  sourceId: number,
  collectionId: number
): Promise<null | {[index: number]: number}> => {
  // insert geometries that do not yet exist
  const matrix = await matchMatrix(client, tableName, collectionId);
  const missing = await getMissingIds(
    client,
    tableName,
    matrix.map(m => m[0][0])
  );
  const matrixFids = fidMatrix(matrix);
  let maxFid = await getMaxFid(client, tableName);

  if (missing.length > 0) {
    const addMap: {[index: number]: number} = {};
    const inserts: string[] = [];
    missing.forEach(m => {
      if (!(m.fid in matrixFids)) {
        maxFid += 1;
        matrixFids[m.fid] = maxFid;
        addMap[m.fid] = maxFid;
      } else {
        addMap[m.fid] = matrixFids[m.fid];
      }
      inserts.push(
        `((
          SELECT geom_3857 AS geom
          FROM ${tableName}_cln
          WHERE
            id = ${m.id}), ${
          m.fid in matrixFids ? matrixFids[m.fid] : maxFid
        }, ${sourceId})`
      );
    });
    await client.query(
      `INSERT INTO "Geometries" (geom_3857, fid, source_id) VALUES ${inserts.join(
        ','
      )}`
    );
    await client.query(
      'UPDATE "Geometries" SET buffer = ST_Buffer(ST_MakeValid(geom_3857), 50) WHERE source_id = $1',
      [sourceId]
    );
    return addMap;
  }
  return null;
};

export const updateGeometries = async (
  client: Client,
  tableName: string,
  sourceId: number,
  collectionId: number
): Promise<void> => {
  // insert geometries that do not yet exist
  const matrix = await matchMatrix(client, tableName, collectionId, true);
  const matrixFids = fidMatrix(matrix);
  const matrixIds = idMatrix(matrix);

  if (matrix.length > 0) {
    const inserts: string[] = [];
    matrix.forEach(m => {
      inserts.push(
        `(SELECT geom_3857 AS geom FROM ${tableName} WHERE id = ${m[0][0]}), ${
          matrixFids[m[0][1]]
        }, ${matrixIds[m[0][1]]}, ${sourceId}`
      );
    });
    await client.query(
      `INSERT INTO "Geometries" (geom_3857, fid, previous, source_id) VALUES (${inserts.join(
        ','
      )})`
    );
    await client.query(
      'UPDATE "Geometries" SET buffer = ST_Buffer(ST_MakeValid(geom_3857), 50) WHERE source_id = $1',
      [sourceId]
    );
  }
};

export const insertProps = (
  client: Client,
  propValueString: string[],
  propValues: (string | number | null)[]
): Promise<void> => {
  return client
    .query(
      `INSERT INTO "GeometryProps" 
        (fid, source_id, name, spat_id)
      VALUES
        ${propValueString.join(',')}`,
      propValues
    )
    .then(() => {});
};

export const importMatch = async (
  client: Client,
  odcsClient: Client,
  id: number,
  name: string,
  nameColumn?: string | null,
  spatColumn?: string | null,
  geomOnly = false,
  collectionId: number | null = null,
  method = 'new' // add, update
): Promise<number> => {
  const match = await getMatch(client, id);
  if (!match) {
    throw Error('match_id not found');
  }

  const download = await getFromImportID(odcsClient, match.import_id);

  // create a new collection
  if (!collectionId) {
    collectionId = await createCollection(client, match.table_name, name);
  }

  // create an import source for the new data
  const source_id = await createSource(
    client,
    odcsClient,
    download.downloaded,
    match.import_id,
    collectionId,
    JSON.stringify({
      matches: match.matches,
      matchesCount: match.matches_count,
      differences: match.difference,
    })
  );

  let addedIds: {[index: number]: number} = {};

  if (method === 'new') {
    // insert new geometries
    await client.query(
      `INSERT INTO "Geometries" (geom_3857, fid, source_id) SELECT geom_3857, fid, ${source_id} FROM ${match.table_name}_cln`
    );
    await client.query(
      'UPDATE "Geometries" SET buffer = ST_Buffer(ST_MakeValid(geom_3857), 50) WHERE source_id = $1',
      [source_id]
    );
    // insert geometry properties
    await client.query(
      `INSERT INTO "GeometryProps" 
        (fid, source_id, name, spat_id)
      SELECT
        fid,
        ${source_id},
        ${nameColumn},
        ${spatColumn ? spatColumn : 'NULL'}
      FROM
        ${match.table_name}
      WHERE
        fid IN (
          SELECT fid FROM "Geometries" WHERE source_id = ${source_id} GROUP BY fid
        )`
    );
  } else if (method === 'add' && collectionId) {
    const tAddedIds = await addGeometries(
      client,
      match.table_name,
      source_id,
      collectionId
    );
    if (tAddedIds) {
      addedIds = tAddedIds;
    }
  } else if (method === 'update') {
    // update existing geometries, which do not yet exist, or where dist is above threshold
    // 1. add missing geoms
    const tAddedIds = await addGeometries(
      client,
      match.table_name,
      source_id,
      collectionId
    );
    if (tAddedIds) {
      addedIds = tAddedIds;
    }

    // 2. insert updates for existing geometries
    await updateGeometries(client, match.table_name, source_id, collectionId);
  } else {
    throw new Error('invalid method or missing previous source_id');
  }

  if (Object.keys(addedIds).length > 0) {
    const addedFids: number[] = [];
    Object.keys(addedIds).forEach(key => {
      addedFids.push(parseInt(key.toString()));
    });
    const metaData = await client.query(`
      SELECT
        fid,
        ${source_id} AS source_id,
        ${nameColumn} AS name,
        ${spatColumn ? spatColumn : 'NULL'} AS spat_column
      FROM
        ${match.table_name}
      WHERE fid IN (${addedFids.join(',')})
    `);

    let propValues: string[] = [];
    let propValueString: string[] = [];
    let propCounter = 0;
    metaData.rows.forEach(r => {
      propValueString.push(
        `(${addedIds[r.fid]}, ${r.source_id}, $${2 * propCounter + 1}, $${
          2 * propCounter + 2
        })`
      );
      propCounter += 1;
      propValues.push(r.name, r.spat_column);
      if (propValues.length > 10000) {
        insertProps(client, propValueString, propValues);
        propValues = [];
        propValueString = [];
        propCounter = 0;
      }
    });

    if (propValues.length > 0) {
      insertProps(client, propValueString, propValues);
    }
  }

  if (!geomOnly) {
    const columns = await getColumns(client, match.table_name);
    const values = await getImportValues(client, match.table_name, columns);

    await importValues(
      client,
      match.table_name,
      values,
      columns,
      source_id,
      collectionId
    );

    await saveColumns(client, columns, source_id);
  }

  return collectionId;
};
