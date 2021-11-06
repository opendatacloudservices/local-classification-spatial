import type {Client, QueryResult} from 'pg';
import type {GeometryType, Match, Matrix} from '../types';

export const getGeomSummary = (
  client: Client,
  tableName: string
): Promise<{bbox: string; centroid: string}> => {
  return client
    .query(
      `SELECT ST_AsText(ST_Envelope(ST_Collect(ST_Transform(geom_3857, 4326)))) AS bbox, ST_AsText(ST_Centroid(ST_Collect(ST_Transform(geom_3857, 4326)))) AS centroid FROM ${tableName}`
    )
    .then(result => result.rows[0]);
};

export const getGeometryType = (
  client: Client,
  tableName: string
): Promise<GeometryType> => {
  return client
    .query(
      `SELECT type 
        FROM geometry_columns 
        WHERE f_table_schema = 'public' 
        AND f_table_name = $1 
        AND f_geometry_column = 'geom_3857';
    `,
      [tableName]
    )
    .then(async results => {
      if (results.rowCount < 1) {
        throw Error(
          'table does not exist or has no geometry column (geom): ' + tableName
        );
      }

      let type = results.rows[0].type;
      if (
        type === 'MULTISURFACE' ||
        type === 'MULTICURVE' ||
        type === 'CURVEPOLYGON'
      ) {
        const typeTest = await client
          .query(
            `SELECT ST_AsText((ST_Dump(ST_Multi(ST_CurveToLine(geom_3857)))).geom) AS text FROM ${tableName} LIMIT 1`
          )
          .then(result => result.rows[0].text);
        if (typeTest.indexOf('LINESTRING') > -1) {
          type = 'CURVEDLINE';
        }
      } else if (type === 'GEOMETRY' || type === 'GEOGRAPHY') {
        const typeTest = await client
          .query(
            `SELECT ST_AsText(geom_3857) AS text FROM ${tableName} LIMIT 1`
          )
          .then(result => result.rows[0].text);

        if (typeTest.indexOf('LINESTRING') > -1) {
          type = 'LINESTRING';
        } else if (typeTest.indexOf('POINT') > -1) {
          type = 'POINT';
        } else if (typeTest.indexOf('POLYGON') > -1) {
          type = 'POLYGON';
        }
      }

      return type;
    });
};

export const hasGeom = (
  client: Client,
  tableName: string
): Promise<boolean> => {
  return client
    .query(
      `SELECT * 
        FROM geometry_columns 
        WHERE f_table_schema = 'public' 
        AND f_table_name = $1 
        AND f_geometry_column = 'geom_3857';
    `,
      [tableName]
    )
    .then(result => (result.rowCount >= 1 ? true : false));
};

export const toSingleGeomType = async (
  geomType: string,
  client: Client,
  tableName: string
): Promise<string> => {
  let newGeomType = 'POLYGON';
  if (geomType === 'POINT' || geomType === 'MULTIPOINT') {
    newGeomType = 'POINT';
  } else if (
    geomType === 'LINESTRING' ||
    geomType === 'MULTILINESTRING' ||
    geomType === 'CURVEDLINE'
  ) {
    newGeomType = 'LINESTRING';
  } else if (
    geomType === 'GEOMETRYCOLLECTION' ||
    geomType === 'GEOGRAPHYCOLLECTION'
  ) {
    const typeTest = await client
      .query(`SELECT ST_AsText(geom_3857) AS text FROM ${tableName} LIMIT 1`)
      .then(result => result.rows[0].text);

    if (typeTest.indexOf('LINESTRING') > -1) {
      newGeomType = 'LINESTRING';
    } else if (typeTest.indexOf('POINT') > -1) {
      newGeomType = 'POINT';
    } else if (typeTest.indexOf('POLYGON') > -1) {
      newGeomType = 'POLYGON';
    }
  }

  return newGeomType;
};

export const cleanGeometries = async (
  client: Client,
  tableName: string
): Promise<string> => {
  const newTableName = tableName + '_cln';

  const geomType = await getGeometryType(client, tableName);
  const newGeomType = await toSingleGeomType(geomType, client, tableName);

  await client.query(`DROP TABLE IF EXISTS ${newTableName}`);

  await client.query(`CREATE TABLE ${newTableName} (
    id SERIAL PRIMARY KEY,
    fid INTEGER,
    geom_3857 GEOMETRY(${newGeomType}, 3857),
    buffer GEOMETRY(${newGeomType}, 3857)
  )`);

  await client.query(
    `CREATE INDEX ${newTableName}_fid ON ${newTableName} (fid)`
  );
  await client.query(
    `CREATE INDEX ${newTableName}_geom_3857 ON ${newTableName} USING gist(geom_3857)`
  );
  await client.query(
    `CREATE INDEX ${newTableName}_buffer ON ${newTableName} USING gist(buffer)`
  );

  /*
   * This transformatin included an ST_MakeValid, but
   * this function can result in mixed geometries.
   * For now we had to remove this.
   */

  let dumpStr = '(ST_Dump(ST_Multi(ST_Force2D(geom_3857)))).geom';
  if (
    geomType === 'CURVEPOLYGON' ||
    geomType === 'MULTISURFACE' ||
    geomType === 'MULTICURVE' ||
    geomType === 'CURVEDLINE'
  ) {
    dumpStr = '(ST_Dump(ST_Multi(ST_CurveToLine(ST_Force2D(geom_3857))))).geom';
  } else if (
    geomType === 'GEOMETRYCOLLECTION' ||
    geomType === 'GEOGRAPHYCOLLECTION'
  ) {
    dumpStr =
      '(ST_Dump(ST_Multi(ST_CollectionHomogenize(ST_Force2D(geom_3857))))).geom';
  }

  await client.query(`
    INSERT INTO ${newTableName} (fid, geom_3857) 
    SELECT fid, ${dumpStr} FROM ${tableName}
  `);

  await client.query(
    `UPDATE ${newTableName} SET buffer = ST_Buffer(ST_MakeValid(geom_3857), 50)`
  );

  return newTableName;
};

// This function returns all points with a matching point within 1m distance
export const matchPoints = (
  client: Client,
  tableName: string,
  open: boolean,
  collection_id?: number
): Promise<QueryResult> => {
  tableName = tableName + '_cln';
  return client.query(
    `WITH distances AS (SELECT
      "Sources".collection_id AS collection_id,
      "Geometries".id as target_id,
      "Geometries".fid as target_fid,
      "Geometries".source_id AS source,
      ${tableName}.id as source_id,
      ${tableName}.fid as source_fid,
      ST_DistanceSpheroid("Geometries".geom_3857, ${tableName}.geom_3857) AS dist
    FROM "Geometries"
    ${
      open || !collection_id
        ? `
    JOIN "Sources" ON "Geometries".source_id = "Sources".id
    JOIN "Collections" ON "Sources".collection_id = "Collections".id AND ("Collections".type = 'POINT' OR "Collections".type = 'MULTIPOINT')
    INNER JOIN ${tableName} ON
      ST_Contains("Geometries".buffer, ${tableName}.geom_3857)
    `
        : `
      JOIN "Sources" ON "Geometries".source_id = "Sources".id AND "Sources".collection_id = ${collection_id}
      JOIN ${tableName}
      ON 
      ST_Contains("Geometries".buffer, ${tableName}.geom_3857) AND ST_Contains(${tableName}.buffer, ST_Transform("Geometries".geom_3857)
    `
    }
    )
    SELECT DISTINCT ON (d1.source_id) d1.* FROM distances d1
    JOIN (
        SELECT source_id, MIN(dist) AS min_dist
        FROM distances
        GROUP BY source_id
    ) d2
    ON d1.source_id = d2.source_id AND d1.dist = d2.min_dist AND d1.source_id IS NOT NULL`
  );
};

// This function returns all lines, that have a matching line contained within a 5 meter buffer area
export const matchLines = (
  client: Client,
  tableName: string,
  open: boolean,
  collection_id?: number,
  similar = false
): Promise<QueryResult> => {
  tableName = tableName + '_cln';
  return client.query(
    `WITH distances AS (SELECT
      "Sources".collection_id AS collection_id,
      "Geometries".fid AS target_id,
      "Geometries".fid AS target_fid,
      "Geometries".source_id AS source,
      ${tableName}.id AS source_id,
      ${tableName}.fid AS source_fid,
      ST_Length(ST_Difference(ST_MakeValid("Geometries".geom_3857), ST_MakeValid(${tableName}.geom_3857))) / ST_Length("Geometries".geom_3857) * 100 AS target_diff,
      ST_Length(ST_Difference(ST_MakeValid(${tableName}.geom_3857), ST_MakeValid("Geometries".geom_3857))) / ST_Length(${tableName}.geom_3857) * 100 AS source_diff,
      ST_HausdorffDistance("Geometries".geom_3857, ${tableName}.geom_3857) AS dist
    FROM "Geometries"
    ${
      open || !collection_id
        ? `
    JOIN "Sources" ON "Geometries".source_id = "Sources".id
    JOIN "Collections" ON "Sources".collection_id = "Collections".id
    INNER JOIN ${tableName} ON
      ("Collections".type = 'LINESTRING' OR "Collections".type = 'MULTILINESTRING') AND
      ST_Intersects(ST_MakeValid("Geometries".geom_3857), ST_MakeValid(${tableName}.geom_3857)) AND ${
            similar
              ? `ST_Contains(ST_Buffer("Geometries".geom_3857, 100), ST_MakeValid(${tableName}.geom_3857)) AND ST_Contains(ST_Buffer(${tableName}.geom_3857, 100), "Geometries".geom_3857)`
              : `ST_Contains("Geometries".buffer, ST_MakeValid(${tableName}.geom_3857)) AND ST_Contains(${tableName}.buffer, "Geometries".geom_3857)`
          }
     
    `
        : `
    JOIN "Sources" ON "Geometries".source_id = "Sources".id
    JOIN ${tableName}
      ON
          "Sources".collection_id = ${collection_id} AND
          ST_Intersects(ST_MakeValid("Geometries".geom_3857), ST_MakeValid(${tableName}.geom_3857)) AND ${
            similar
              ? `ST_Contains(ST_Buffer("Geometries".geom_3857, 100), ST_MakeValid(${tableName}.geom_3857)) AND ST_Contains(ST_Buffer(${tableName}.geom_3857, 100), "Geometries".geom_3857)`
              : `ST_Contains("Geometries".buffer, ST_MakeValid(${tableName}.geom_3857)) AND ST_Contains(${tableName}.buffer, "Geometries".geom_3857)`
          }
    `
    }
    )
    SELECT DISTINCT ON (d1.source_id) d1.* FROM distances d1
    JOIN (
        SELECT source_id, MIN(dist) AS min_dist
        FROM distances
        GROUP BY source_id
    ) d2
    ON d1.source_id = d2.source_id AND d1.dist = d2.min_dist AND d1.source_id IS NOT NULL`
  );
};

// This function return all polygons that have a matching polygon contained within a 5 meter buffer
export const matchPolygons = (
  client: Client,
  tableName: string,
  open: boolean,
  collection_id?: number,
  similar = false
): Promise<QueryResult> => {
  tableName = tableName + '_cln';
  return client.query(
    `WITH distances AS (SELECT 
      "Sources".collection_id AS collection_id,
      "Geometries".id AS target_id,
      "Geometries".fid AS target_fid,
      "Geometries".source_id AS source,
      ${tableName}.id AS source_id,
      ${tableName}.fid AS source_fid,
      ST_Area(ST_Difference(ST_MakeValid("Geometries".geom_3857), ST_MakeValid(${tableName}.geom_3857))) / ST_Area("Geometries".geom_3857) * 100 AS target_diff,
      ST_Area(ST_Difference(ST_MakeValid(${tableName}.geom_3857), ST_MakeValid("Geometries".geom_3857))) / ST_Area(${tableName}.geom_3857) * 100 AS source_diff,
      ST_HausdorffDistance("Geometries".geom_3857, ${tableName}.geom_3857) AS dist
    FROM "Geometries"
    ${
      open || !collection_id
        ? `
    JOIN "Sources" ON "Geometries".source_id = "Sources".id
    JOIN "Collections" ON "Sources".collection_id = "Collections".id
    INNER JOIN ${tableName} ON
      ("Collections".type = 'POLYGON' OR "Collections".type = 'MULTIPOLYGON') AND
      ST_Intersects(ST_MakeValid("Geometries".geom_3857), ST_MakeValid(${tableName}.geom_3857)) AND ${
            similar
              ? `ST_Contains(ST_Buffer("Geometries".geom_3857, 100), ST_MakeValid(${tableName}.geom_3857)) AND ST_Contains(ST_Buffer(${tableName}.geom_3857, 100), "Geometries".geom_3857)`
              : `ST_Contains("Geometries".buffer, ST_MakeValid(${tableName}.geom_3857)) AND ST_Contains(${tableName}.buffer, "Geometries".geom_3857)`
          }
    `
        : `
    JOIN "Sources" ON "Geometries".source_id = "Sources".id
    JOIN ${tableName}
      ON 
        "Sources".collection_id = ${collection_id} AND
        ST_Intersects(ST_MakeValid("Geometries".geom_3857), ST_MakeValid(${tableName}.geom_3857)) AND ${
            similar
              ? `ST_Contains(ST_Buffer("Geometries".geom_3857, 100), ST_MakeValid(${tableName}.geom_3857)) AND ST_Contains(ST_Buffer(${tableName}.geom_3857, 100), "Geometries".geom_3857)`
              : `ST_Contains("Geometries".buffer, ST_MakeValid(${tableName}.geom_3857)) AND ST_Contains(${tableName}.buffer, "Geometries".geom_3857)`
          }
    `
    }
    )
    SELECT DISTINCT ON (d1.source_id) d1.* FROM distances d1
    JOIN (
        SELECT source_id, MIN(dist) AS min_dist
        FROM distances
        GROUP BY source_id
    ) d2
    ON d1.source_id = d2.source_id AND d1.dist = d2.min_dist AND d1.source_id IS NOT NULL`
  );
};

export const matchGeometries = async (
  client: Client,
  tableName: string
): Promise<Match> => {
  const geometryType = await getGeometryType(client, tableName);
  // get number of rows
  const rowCount = await client
    .query(`SELECT COUNT(*) AS rowcount FROM ${tableName}_cln`)
    .then(result => {
      return parseInt(result.rows[0].rowcount);
    });

  let matches: QueryResult | null = null;

  // find best potential matches with an "open" approach
  if (geometryType === 'POINT' || geometryType === 'MULTIPOINT') {
    matches = await matchPoints(client, tableName, true);
  } else if (
    geometryType === 'POLYGON' ||
    geometryType === 'MULTIPOLYGON' ||
    geometryType === 'CURVEPOLYGON' ||
    geometryType === 'MULTICURVE' ||
    geometryType === 'MULTISURFACE'
  ) {
    matches = await matchPolygons(client, tableName, true);
  } else if (
    geometryType === 'LINESTRING' ||
    geometryType === 'MULTILINESTRING' ||
    geometryType === 'CURVEDLINE'
  ) {
    matches = await matchLines(client, tableName, true);
  }

  // aggregate collections in result
  const collections: {[index: number]: number} = {};
  let collectionCount = 0;
  if (matches && matches.rows) {
    matches.rows.forEach(row => {
      if (!(row.collection_id in collections)) {
        collections[row.collection_id] = 0;
        collectionCount++;
      }
      collections[row.collection_id]++;
    });
  }

  // identify dominant collection across matches
  let dominantCollection = -1;
  let dominantCount = 0;
  Object.keys(collections).forEach(id => {
    const iid = parseInt(id);
    if (collections[iid] > dominantCount) {
      dominantCount = collections[iid];
      dominantCollection = iid;
    }
  });

  const process = {
    importCount: rowCount,
    sources: Object.keys(collections).map(key => parseInt(key)),
    sourceCount: Object.keys(collections).map(
      key => collections[parseInt(key.toString())]
    ),
    message: '',
    differences:
      matches && matches.rows
        ? matches.rows.map(r => {
            return {
              collection_id: r.collection_id,
              source_id: r.source_id,
              target_id: r.target_id,
              source_fid: r.source_id,
              target_fid: r.target_id,
              source_diff: r.source_diff || null,
              target_diff: r.target_diff || null,
              dist: r.dist,
            };
          })
        : [],
  };

  if (dominantCollection === -1) {
    // no proper matches were returned
    process.message = 'no-match';
    return {
      collection_id: null,
      process,
    };
  } else if (
    collectionCount === 1 &&
    matches &&
    matches.rowCount === rowCount
  ) {
    // only one matching geom was returned and matches for all elements are included
    return {collection_id: dominantCollection};
  }

  // check aginst the dominant collection
  if (geometryType === 'POINT' || geometryType === 'MULTIPOINT') {
    matches = await matchPoints(client, tableName, false, dominantCollection);
  } else if (
    geometryType === 'POLYGON' ||
    geometryType === 'MULTIPOLYGON' ||
    geometryType === 'CURVEPOLYGON' ||
    geometryType === 'MULTICURVE' ||
    geometryType === 'MULTISURFACE'
  ) {
    matches = await matchPolygons(client, tableName, false, dominantCollection);
  } else if (
    geometryType === 'LINESTRING' ||
    geometryType === 'MULTILINESTRING' ||
    geometryType === 'CURVEDLINE'
  ) {
    matches = await matchLines(client, tableName, false, dominantCollection);
  }

  if (matches && matches.rows) {
    process.differences = matches!.rows.map(r => {
      return {
        collection_id: r.collection_id,
        source_id: r.source_id,
        target_id: r.target_id,
        source_fid: r.source_id,
        target_fid: r.target_id,
        source_diff: r.source_diff || null,
        target_diff: r.target_diff || null,
        dist: r.dist,
      };
    });

    if (matches!.rowCount === rowCount) {
      process.message = 'match';
      return {collection_id: dominantCollection, process};
    } else {
      const collectionCount = await client.query(
        // The join makes sure we only get one "version" of each geometry
        `WITH geoms AS (
          SELECT g1.id, g2.id FROM
            "Geometries" AS g1
          JOIN
            "Geometries" AS g2
            ON g2.id = g1.previous
          JOIN
            "Sources"
            ON "Sources".id = g1.source_id
          WHERE
            g2.id IS NULL AND
            "Sources".collection_id = $1
        )
        SELECT
          COUNT(*) AS rowcount
        FROM
          geoms
        `,
        [dominantCollection]
      );
      if (matches!.rowCount === collectionCount!.rows[0].rowcount) {
        // the new set has more geometries than the original, but all existing parts match
        process.message = 'subset';
        process.sources = [dominantCollection];
        process.sourceCount = [collectionCount!.rows[0].rowcount];
        return {
          collection_id: null,
          process,
        };
      } else {
        process.message = 'no-match-2';
        return {
          collection_id: null,
          process,
        };
      }
    }
  } else {
    process.message = 'no-match-3';
    process.differences = [];
    return {
      collection_id: null,
      process,
    };
  }
};

export const matchMatrix = async (
  client: Client,
  tableName: string,
  collection_id: number,
  similar = false
): Promise<Matrix> => {
  // RETURN: [[TMP_TABLE.id, TMP_TABLE.fid], [Geometry.id, Geometry.fid]][]

  const geometryType = await getGeometryType(client, tableName);

  let matches: QueryResult | null = null;

  if (geometryType === 'POINT' || geometryType === 'MULTIPOINT') {
    matches = await matchPoints(client, tableName, false, collection_id);
  } else if (
    geometryType === 'POLYGON' ||
    geometryType === 'MULTIPOLYGON' ||
    geometryType === 'CURVEPOLYGON' ||
    geometryType === 'MULTICURVE' ||
    geometryType === 'MULTISURFACE'
  ) {
    matches = await matchPolygons(
      client,
      tableName,
      false,
      collection_id,
      similar
    );
  } else if (
    geometryType === 'LINESTRING' ||
    geometryType === 'MULTILINESTRING' ||
    geometryType === 'CURVEDLINE'
  ) {
    matches = await matchLines(
      client,
      tableName,
      false,
      collection_id,
      similar
    );
  }

  if (matches && matches.rows) {
    return matches.rows.map(r => [
      [r.source_id, r.source_fid],
      [r.target_id, r.target_fid],
      [r.dist],
    ]);
  } else {
    return [];
  }
};

export const idMatrix = (matrix: Matrix): {[index: string]: number} => {
  const map: {[index: number]: number} = {};
  matrix.forEach(m => {
    map[m[0][0]] = m[1][0];
  });
  return map;
};

export const fidMatrix = (matrix: Matrix): {[index: string]: number} => {
  const map: {[index: string]: number} = {};
  matrix.forEach(m => {
    map[m[0][1]] = m[1][1];
  });
  return map;
};

export const collectionFromSource = (
  client: Client,
  source_id: number
): Promise<number> => {
  return client
    .query('SELECT collection_id FROM "Sources" WHERE id = $1', [source_id])
    .then(result => result.rows[0].collection_id);
};
