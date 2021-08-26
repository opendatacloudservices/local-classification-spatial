import {exec} from 'child_process';
import {Client} from 'pg';
import {toSingleGeomType, getGeometryType, matchMatrix} from '../postgis/index';
import type {Columns, Results} from '../types';
import {date2timestamp} from '../utils';

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
      -lco GEOMETRY_NAME=geom \
      -lco FID=fid \
      -lco SPATIAL_INDEX=GIST \
      -nln ${tableName} \
      -t_srs EPSG:4326`,
      (err, stdout, stderr) => {
        if (err) {
          reject(err);
        }
        resolve({
          out: stdout,
          err: stderr,
        });
      }
    );
  });
};

export const dropImport = async (
  client: Client,
  tableName: string
): Promise<void> => {
  return client.query(`DROP TABLE ${tableName}`).then(() => {});
};

// TODO: separate all collection functions into postgres/collection.ts

export const createCollection = async (
  client: Client,
  tableName: string,
  collectionName: string,
  nameColumn: string,
  spatColumn?: string | null
): Promise<number> => {
  const geomType = toSingleGeomType(await getGeometryType(client, tableName));
  const bbox = await client
    .query(`SELECT ST_AsText(ST_Extent(geom)) AS bbox FROM ${tableName}`)
    .then(result => result.rows[0].bbox);
  const collectionId = await client
    .query(
      'INSERT INTO "Collections" (name, type, bbox) VALUES ($1, $2, ST_GeomFromText($3)) RETURNING id',
      [collectionName, geomType, bbox]
    )
    .then(result => result.rows[0].id);

  await client.query(
    `INSERT INTO "Geometries" (geom, fid, collection_id) SELECT geom, fid, ${collectionId} FROM ${tableName}_cln`
  );

  await client.query(
    `INSERT INTO "GeometryProps" 
      (fid, collection_id, name, centroid, area, len, spat_id)
    SELECT
      fid,
      ${collectionId},
      ${nameColumn},
      ST_Centroid(geom),
      ${geomType === 'POLYGON' ? 'ST_Area(geom)' : 'NULL'},
      ${geomType === 'LINESTRING' ? 'ST_LENGTH(geom)' : 'NULL'},
      ${spatColumn ? spatColumn : 'NULL'}
    FROM
      ${tableName}`
  );

  return collectionId;
};

export const dropCollection = async (
  client: Client,
  id: number
): Promise<void> => {
  return client
    .query('DELETE FROM "Collections" WHERE id = $1', [id])
    .then(() =>
      client.query('DELETE FROM "Geometries" WHERE collection_id = $1', [id])
    )
    .then(() => {});
};

// const columns = filterColumns(await getColumns(client, tableName));

export const getImportValues = async (
  client: Client,
  tableName: string,
  columns: Columns
): Promise<Results> => {
  return client
    .query(
      `SELECT fid, ${columns.map(c => c.name).join(', ')} FROM ${tableName}`
    )
    .then(result => result.rows);
};

export const matrixToValuesMatrix = (
  matrix: [number, number][],
  values: Results
): {[index: number]: number} => {
  const valuesMatrixMap: {[index: number]: number} = {};
  matrix.forEach(m => {
    let targetKey = -1;
    values.forEach((v, vi) => {
      if (v.fid === m[0]) {
        targetKey = vi;
      }
    });
    valuesMatrixMap[m[1]] = targetKey;
  });

  return valuesMatrixMap;
};

export const checkValues = async (
  client: Client,
  values: Results,
  columns: Columns,
  collection_id: number,
  updated: Date,
  matrix: [number, number][]
): Promise<boolean> => {
  let hasNewData = true;

  const valuesMatrixMap = matrixToValuesMatrix(matrix, values);

  // check if timestamp already exists in the database
  const exists = await client
    .query(
      'SELECT * FROM "GeometryAttributes" WHERE collection_id = $1 AND updated = TO_TIMESTAMP($2, \'YYYY-MM-DD HH:MI:SS\') LIMIT 1',
      [collection_id, date2timestamp(updated)]
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
        'SELECT DISTINCT ON (updated) updated FROM "GeometryAttributes" WHERE collection_id = $1 ORDER BY updated ASC',
        [collection_id]
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
      collection_id
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
  collection_id: number
): Promise<boolean> => {
  let hasChanged = false;

  for (let ci = 0; ci < columns.length; ci += 1) {
    const currentValues = await client
      .query(
        'SELECT * FROM "GeometryAttributes" WHERE key = $1 update = $2 AND collection_id = $3',
        [columns[ci].name, timestamp, collection_id]
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
  import_id: number | null
): Promise<number> => {
  if (!import_id) {
    return client
      .query(
        'INSERT INTO "Sources" (updated, manual) VALUES ($1, $2) RETURN id',
        [updated, true]
      )
      .then(r => r.rows[0].id);
  }

  const importData = await odcsClient
    .query('SELECT * FROM "Imports" WHERE id = $1', [import_id])
    .then(r => r.rows[0]);

  return client
    .query(
      'INSERT INTO "Sources" (import_id, updated, manual, copyright) VALUES ($1, $2, $3, $4) RETURN id',
      [import_id, updated, false, importData.meta_license]
    )
    .then(r => r.rows[0].id);
};

export const importValues = async (
  client: Client,
  tableName: string,
  values: Results,
  columns: Columns,
  updated: Date,
  collection_id: number
): Promise<void> => {
  const matrix = await matchMatrix(client, tableName, collection_id);
  const valuesMatrixMap = matrixToValuesMatrix(matrix, values);
  for (let c = 0; c < columns.length; c += 1) {
    const inserts: (number | string | null | number[] | string[] | Date)[] = [];
    let valueString = '';

    values.forEach((v, vi) => {
      if (valueString !== '') {
        valueString += ',';
      }
      valueString += `(
        $${vi * 5 + 1},
        $${vi * 5 + 2},
        $${vi * 5 + 3},
        $${vi * 5 + 4},
        $${vi * 5 + 5})`;

      inserts.push(
        updated,
        valuesMatrixMap[parseInt(v.fid!.toString())],
        collection_id,
        columns[c].name,
        v[columns[c].name]
      );
    });

    let targetType = 'str_val';
    switch (columns[c].type) {
      case 'ARRAY':
        if (columns[c].udt.indexOf('float') >= 0) {
          targetType = 'float_a_val';
        } else if (columns[c].udt.indexOf('int') >= 0) {
          targetType = 'int_a_val';
        } else if (
          columns[c].udt.indexOf('text') >= 0 ||
          columns[c].udt.indexOf('char') >= 0
        ) {
          targetType = 'str_a_val';
        }
        break;
      case 'smallint':
      case 'integer':
        targetType = 'int_val';
        break;
      case 'double precision':
        targetType = 'float_val';
        break;
      default:
        targetType = 'str_val';
        break;
    }
    await client.query(
      `INSERT INTO
      "GeometriesAttributes"
      (
        updated,
        fid,
        collection_id,
        key,
        ${targetType}
      )
      VALUES
      ${valueString}`,
      inserts
    );
  }
};
