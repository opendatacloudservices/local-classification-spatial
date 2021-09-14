import type {Client, QueryResult} from 'pg';
import type {GeometryType, Match} from '../types';

export const getGeomSummary = (
  client: Client,
  tableName: string
): Promise<{bbox: string; centroid: string}> => {
  return client
    .query(
      `SELECT ST_AsText(ST_Envelope(ST_Union(geom))) AS bbox, ST_AsText(ST_Centroid(ST_Union(geom))) AS centroid FROM ${tableName}`
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
        AND f_geometry_column = 'geom';
    `,
      [tableName]
    )
    .then(async results => {
      if (results.rowCount < 1) {
        throw Error('table does not exist or has no geometry column (geom).');
      }

      let type = results.rows[0].type;
      if (
        type === 'MULTISURFACE' ||
        type === 'MULTICURVE' ||
        type === 'CURVEPOLYGON'
      ) {
        const typeTest = await client
          .query(
            `SELECT ST_AsText((ST_Dump(ST_Multi(ST_CurveToLine(geom)))).geom) AS text FROM ${tableName} LIMIT 1`
          )
          .then(result => result.rows[0].text);
        if (typeTest.indexOf('LINESTRING') > -1) {
          type = 'CURVEDLINE';
        }
      } else if (type === 'GEOMETRY' || type === 'GEOGRAPHY') {
        const typeTest = await client
          .query(`SELECT ST_AsText(geom) AS text FROM ${tableName} LIMIT 1`)
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
        AND f_geometry_column = 'geom';
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
      .query(`SELECT ST_AsText(geom) AS text FROM ${tableName} LIMIT 1`)
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
    geom GEOMETRY(${newGeomType}, 4326)
  )`);

  await client.query(
    `CREATE INDEX ${newTableName}_fid ON ${newTableName} (fid)`
  );
  await client.query(
    `CREATE INDEX ${newTableName}_geom ON ${newTableName} USING gist(geom)`
  );

  /*
   * This transformatin included an ST_MakeValid, but
   * this function can result in mixed geometries.
   * For now we had to remove this.
   */

  let dumpStr = '(ST_Dump(ST_Multi(geom))).geom';
  if (
    geomType === 'CURVEPOLYGON' ||
    geomType === 'MULTISURFACE' ||
    geomType === 'MULTICURVE' ||
    geomType === 'CURVEDLINE'
  ) {
    dumpStr = '(ST_Dump(ST_Multi(ST_CurveToLine(geom)))).geom';
  } else if (
    geomType === 'GEOMETRYCOLLECTION' ||
    geomType === 'GEOGRAPHYCOLLECTION'
  ) {
    dumpStr = '(ST_Dump(ST_Multi(ST_CollectionHomogenize(geom)))).geom';
  }

  await client.query(`
    INSERT INTO ${newTableName} (fid, geom) 
    SELECT fid, ${dumpStr} FROM ${tableName}
  `);

  return newTableName;
};

export const matchPoints = (
  client: Client,
  tableName: string,
  open: boolean,
  source_id?: number
): Promise<QueryResult> => {
  return client.query(
    `WITH distances AS (SELECT
      "Geometries".id as target_id,
      "Geometries".source_id AS source,
      ${tableName}.id as source_id,
      ST_Distance("Geometries".geom, ${tableName}.geom) AS dist
    FROM "Geometries"
    ${
      open
        ? `
    JOIN "Sources" ON "Geometries".source_id = "Sources".id
    JOIN "Collections" ON "Sources".collection_id = "Collections".id
    INNER JOIN ${tableName} ON
      ("Collections".type = 'POINT' OR "Collections".type = 'MULTIPOINT') AND 
      ST_DWithin("Geometries".geom, ${tableName}.geom, 0.02)
    `
        : `
    CROSS JOIN ${tableName}
        WHERE 
          "Geometries".source_id = $1 AND
          ST_Distance("Geometries".geom, ${tableName}.geom) < 0.02
    `
    }
    )
    SELECT d1.* FROM distances d1
    JOIN (
        SELECT source_id, MIN(dist) AS min_dist
        FROM distances
        GROUP BY source_id
    ) d2
    ON d1.source_id = d2.source_id AND d1.dist = d2.min_dist`,
    open ? [] : [source_id]
  );
};

export const matchLines = (
  client: Client,
  tableName: string,
  open: boolean,
  source_id?: number
): Promise<QueryResult> => {
  return client.query(
    `WITH distances AS (SELECT
      "Geometries".fid as target_id,
      "Geometries".source_id AS source,
      ${tableName}.id as source_id,
      ST_HausdorffDistance("Geometries".geom, ${tableName}.geom) AS dist
    FROM "Geometries"
    ${
      open
        ? `
    JOIN "Sources" ON "Geometries".source_id = "Sources".id
    JOIN "Collections" ON "Sources".collection_id = "Collections".id
    INNER JOIN ${tableName} ON
      ("Collections".type = 'LINESTRING' OR "Collections".type = 'MULTILINESTRING') AND 
      ST_Contains(ST_Buffer("Geometries".geom, 0.01), ${tableName}.geom)
    `
        : `
    CROSS JOIN ${tableName}
        WHERE 
          "Geometries".source_id = $1 AND
          ST_Contains(ST_Buffer("Geometries".geom, 0.01), ${tableName}.geom)
    `
    }
    )
    SELECT d1.* FROM distances d1
    JOIN (
        SELECT source_id, MIN(dist) AS min_dist
        FROM distances
        GROUP BY source_id
    ) d2
    ON d1.source_id = d2.source_id AND d1.dist = d2.min_dist`,
    open ? [] : [source_id]
  );
  /*
    WHERE
      (ST_Length("Geometries".geom) > ST_Length(${tableName}.geom) * 0.9 AND
      ST_Length("Geometries".geom) < ST_Length(${tableName}.geom) * 1.1)
      AND
      (ST_Area(ST_Envelope("Geometries".geom)) > ST_Area(ST_Envelope(${tableName}.geom)) * 0.9 AND
      ST_Area(ST_Envelope("Geometries".geom)) < ST_Area(ST_Envelope(${tableName}.geom)) * 1.1)
    AND
      (ST_Length("Geometries".geom) > ST_Length(${tableName}.geom) * 0.9 AND
      ST_Length("Geometries".geom) < ST_Length(${tableName}.geom) * 1.1)
      AND
      (ST_Area(ST_Envelope("Geometries".geom)) > ST_Area(ST_Envelope(${tableName}.geom)) * 0.9 AND
      ST_Area(ST_Envelope("Geometries".geom)) < ST_Area(ST_Envelope(${tableName}.geom)) * 1.1)
  */
};

export const matchPolygons = (
  client: Client,
  tableName: string,
  open: boolean,
  source_id?: number
): Promise<QueryResult> => {
  return client.query(
    `WITH distances AS (SELECT 
      "Geometries".id as target_id,
      "Geometries".source_id AS source,
      ${tableName}.id as source_id,
      ST_HausdorffDistance("Geometries".geom, ${tableName}.geom) AS dist
    FROM "Geometries"
    ${
      open
        ? `
    JOIN "Sources" ON "Geometries".source_id = "Sources".id
    JOIN "Collections" ON "Sources".collection_id = "Collections".id
    INNER JOIN ${tableName} ON
      ("Collections".type = 'POLYGON' OR "Collections".type = 'MULTIPOLYGON') AND 
      ST_Contains(ST_Buffer("Geometries".geom, 0.01), ${tableName}.geom)
    `
        : `
    CROSS JOIN ${tableName}
      WHERE 
        "Geometries".source_id = $1 AND
        ST_Contains(ST_Buffer("Geometries".geom, 0.01), ${tableName}.geom)
    `
    }
    )
    SELECT d1.* FROM distances d1
    JOIN (
        SELECT source_id, MIN(dist) AS min_dist
        FROM distances
        GROUP BY source_id
    ) d2
    ON d1.source_id = d2.source_id AND d1.dist = d2.min_dist`,
    open ? [] : [source_id]
  );
  /*
   * WHERE
   *  ST_Area("Geometries".geom) > ST_Area(${tableName}.geom) * 0.9 AND
   *    ST_Area("Geometries".geom) < ST_Area(${tableName}.geom) * 1.1
   */
};

export const matchGeometries = async (
  client: Client,
  tableName: string
): Promise<Match> => {
  const geometryType = await getGeometryType(client, tableName);
  // get number of rows
  const rowCount = await client
    .query(`SELECT COUNT(*) AS rowcount FROM ${tableName}`)
    .then(result => {
      return parseInt(result.rows[0].rowcount);
    });

  let matches: QueryResult;

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
  const sources: {[index: number]: number} = {};
  let sourceCount = 0;
  matches!.rows.forEach(row => {
    if (!(row.source in sources)) {
      sources[row.source] = 0;
      sourceCount++;
    }
    sources[row.source]++;
  });

  // identify dominant collection across matches
  let dominantSource = -1;
  let dominantCount = 0;
  Object.keys(sources).forEach(id => {
    const iid = parseInt(id);
    if (sources[iid] > dominantCount) {
      dominantCount = sources[iid];
      dominantSource = iid;
    }
  });

  const process = {
    sources: Object.keys(sources).map(key => parseInt(key)),
    sourceCount: Object.keys(sources).map(
      key => sources[parseInt(key.toString())]
    ),
    message: '',
    differences: matches!.rows.map(r => {
      return {
        source_id: r.source_id,
        target_id: r.target_id,
        dist: r.dist,
      };
    }),
  };

  if (dominantSource === -1) {
    process.message = 'no-match';
    return {
      source_id: null,
      process,
    };
  } else if (sourceCount === 1 && matches!.rowCount === rowCount) {
    return {source_id: dominantSource};
  }

  // check aginst the dominant collection
  if (geometryType === 'POINT' || geometryType === 'MULTIPOINT') {
    matches = await matchPoints(client, tableName, false, dominantSource);
  } else if (
    geometryType === 'POLYGON' ||
    geometryType === 'MULTIPOLYGON' ||
    geometryType === 'CURVEPOLYGON' ||
    geometryType === 'MULTICURVE' ||
    geometryType === 'MULTISURFACE'
  ) {
    matches = await matchPolygons(client, tableName, false, dominantSource);
  } else if (
    geometryType === 'LINESTRING' ||
    geometryType === 'MULTILINESTRING' ||
    geometryType === 'CURVEDLINE'
  ) {
    matches = await matchLines(client, tableName, false, dominantSource);
  }

  process.differences = matches!.rows.map(r => {
    return {
      source_id: r.source_id,
      target_id: r.target_id,
      dist: r.dist,
    };
  });

  if (matches!.rowCount === rowCount) {
    return {source_id: dominantSource};
  } else {
    const collectionCount = await client.query(
      'SELECT COUNT(*) AS rowcount FROM "Geometries" WHERE source_id = $1',
      [dominantSource]
    );
    if (matches!.rowCount === collectionCount!.rows[0].rowcount) {
      process.message = 'subset';
      process.sources = [dominantSource];
      process.sourceCount = [collectionCount!.rows[0].rowcount];
      return {
        source_id: null,
        process,
      };
    } else {
      process.message = 'no-match-2';
      return {
        source_id: null,
        process,
      };
    }
  }
};

export const matchMatrix = async (
  client: Client,
  tableName: string,
  source_id: number
): Promise<[number, number][]> => {
  const geometryType = await getGeometryType(client, tableName);

  let matches: QueryResult;

  if (geometryType === 'POINT' || geometryType === 'MULTIPOINT') {
    matches = await matchPoints(client, tableName, false, source_id);
  } else if (
    geometryType === 'POLYGON' ||
    geometryType === 'MULTIPOLYGON' ||
    geometryType === 'CURVEPOLYGON' ||
    geometryType === 'MULTICURVE' ||
    geometryType === 'MULTISURFACE'
  ) {
    matches = await matchPolygons(client, tableName, false, source_id);
  } else if (
    geometryType === 'LINESTRING' ||
    geometryType === 'MULTILINESTRING' ||
    geometryType === 'CURVEDLINE'
  ) {
    matches = await matchLines(client, tableName, false, source_id);
  }

  return matches!.rows.map(r => [r.source_id, r.target_id]);
};

export const negativeMatchMatrix = async (
  client: Client,
  tableName: string,
  source_id: number
): Promise<{
  missing: {geometry_fids: number[]; match_ids: number[]};
  matches: {geometry_fids: number[]; match_ids: number[]};
}> => {
  const matrix = await matchMatrix(client, tableName, source_id);
  const geometries_fid = matrix.map(m => m[0]);
  const matchTable_id = matrix.map(m => m[1]);
  const missingGeometryIds = await client
    .query(
      `SELECT fid FROM "Geometries" WHERE source_id = $1 AND fid NOT IN (${geometries_fid.join(
        ','
      )})`,
      [source_id]
    )
    .then(result => (result.rowCount > 0 ? result.rows : []));
  const missingMatchIds = await client
    .query(
      `SELECT id FROM ${tableName} WHERE id NOT IN (${matchTable_id.join(',')})`
    )
    .then(result => (result.rowCount > 0 ? result.rows : []));

  return {
    missing: {
      geometry_fids: missingGeometryIds,
      match_ids: missingMatchIds,
    },
    matches: {
      geometry_fids: geometries_fid,
      match_ids: matchTable_id,
    },
  };
};

export const collectionFromSource = (
  client: Client,
  source_id: number
): Promise<number> => {
  return client
    .query('SELECT collection_id FROM "Sources" WHERE id = $1', [source_id])
    .then(result => result.rows[0].collection_id);
};
