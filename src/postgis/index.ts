import type {Client, QueryResult} from 'pg';
import type {GeometryType} from '../types';

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
    .then(results => {
      if (results.rowCount < 1) {
        throw Error('table does not exist or has no geometry column (geom).');
      }
      return results.rows[0].type;
    });
};

export const toSingleGeomType = (geomType: string): string => {
  let newGeomType = 'POLYGON';
  if (geomType === 'POINT' || geomType === 'MULTIPOINT') {
    newGeomType = 'POINT';
  } else if (geomType === 'LINESTRING' || geomType === 'MULTILINESTRING') {
    newGeomType = 'LINESTRING';
  }

  return newGeomType;
};

export const cleanGeometries = async (
  client: Client,
  tableName: string
): Promise<string> => {
  const newTableName = tableName + '_cln';

  const geomType = await getGeometryType(client, tableName);
  const newGeomType = toSingleGeomType(geomType);

  // TODO: remove
  await client.query(`DROP TABLE IF EXISTS ${newTableName}`);

  await client.query(`CREATE TABLE ${newTableName} (
    id SERIAL PRIMARY KEY,
    fid INTEGER,
    geom GEOMETRY(${newGeomType}, 4326),
    single_geom GEOMETRY(${newGeomType}, 4326)
  )`);

  await client.query(
    `CREATE INDEX ${newTableName}_fid ON ${newTableName} (fid)`
  );
  await client.query(
    `CREATE INDEX ${newTableName}_geom ON ${newTableName} USING gist(geom)`
  );

  await client.query(`
    INSERT INTO ${newTableName} (fid, single_geom) 
    SELECT fid, (ST_Dump(geom)).geom FROM ${tableName}
  `);

  await client.query(
    `UPDATE ${newTableName} SET geom = ST_MakeValid(single_geom)`
  );

  await client.query(`ALTER TABLE ${newTableName} DROP COLUMN single_geom`);

  return newTableName;
};

export const matchPoints = (
  client: Client,
  tableName: string,
  open: boolean,
  collection_id?: number
): Promise<QueryResult> => {
  return client.query(
    `WITH distances AS (SELECT
      "Geometries".id as target_id,
      "Geometries".collection_id,
      ${tableName}.id as source_id,
      ST_Distance("Geometries".geom, ${tableName}.geom) AS dist
    FROM "Geometries"
    ${
      open
        ? `
    JOIN "Collections" ON "Geometries".collection_id = "Collections".id
    INNER JOIN ${tableName} ON
      ("Collections".type = 'POINT' OR "Collections".type = 'MULTIPOINT') AND 
      ST_DWithin("Geometries".geom, ${tableName}.geom, 0.02)
    `
        : `
    CROSS JOIN ${tableName}
        WHERE 
          "Geometries".collection_id = $1 AND
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
    open ? [] : [collection_id]
  );
};

export const matchLines = (
  client: Client,
  tableName: string,
  open: boolean,
  collection_id?: number
): Promise<QueryResult> => {
  return client.query(
    `WITH distances AS (SELECT
      "Geometries".fid as target_id,
      "Geometries".collection_id,
      ${tableName}.id as source_id,
      ST_HausdorffDistance("Geometries".geom, ${tableName}.geom) AS hausdorff
    FROM "Geometries"
    ${
      open
        ? `
    JOIN "Collections" ON "Geometries".collection_id = "Collections".id
    INNER JOIN ${tableName} ON
      ("Collections".type = 'LINESTRING' OR "Collections".type = 'MULTILINESTRING') AND 
      ST_Contains(ST_Buffer("Geometries".geom, 0.01), ${tableName}.geom)
    `
        : `
    CROSS JOIN ${tableName}
        WHERE 
          "Geometries".collection_id = $1 AND
          ST_Contains(ST_Buffer("Geometries".geom, 0.01), ${tableName}.geom)
    `
    }
    )
    SELECT d1.* FROM distances d1
    JOIN (
        SELECT source_id, MIN(hausdorff) AS min_hausdorff
        FROM distances
        GROUP BY source_id
    ) d2
    ON d1.source_id = d2.source_id AND d1.hausdorff = d2.min_hausdorff`,
    open ? [] : [collection_id]
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
  collection_id?: number
): Promise<QueryResult> => {
  return client.query(
    `WITH distances AS (SELECT 
      "Geometries".id as target_id,
      "Geometries".collection_id,
      ${tableName}.id as source_id,
      ST_HausdorffDistance("Geometries".geom, ${tableName}.geom) AS hausdorff
    FROM "Geometries"
    ${
      open
        ? `
    JOIN "Collections" ON "Geometries".collection_id = "Collections".id
    INNER JOIN ${tableName} ON
      ("Collections".type = 'POLYGON' OR "Collections".type = 'MULTIPOLYGON') AND 
      ST_Contains(ST_Buffer("Geometries".geom, 0.01), ${tableName}.geom)
    `
        : `
    CROSS JOIN ${tableName}
      WHERE 
        "Geometries".collection_id = $1 AND
        ST_Contains(ST_Buffer("Geometries".geom, 0.01), ${tableName}.geom)
    `
    }
    )
    SELECT d1.* FROM distances d1
    JOIN (
        SELECT source_id, MIN(hausdorff) AS min_hausdorff
        FROM distances
        GROUP BY source_id
    ) d2
    ON d1.source_id = d2.source_id AND d1.hausdorff = d2.min_hausdorff`,
    open ? [] : [collection_id]
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
): Promise<null | number> => {
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
  } else if (geometryType === 'POLYGON' || geometryType === 'MULTIPOLYGON') {
    matches = await matchPolygons(client, tableName, true);
  } else if (
    geometryType === 'LINESTRING' ||
    geometryType === 'MULTILINESTRING'
  ) {
    matches = await matchLines(client, tableName, true);
  }

  // aggregate collections in result
  const collections: {[index: number]: number} = {};
  let collectionCount = 0;
  matches!.rows.forEach(row => {
    if (!(row.collection_id in collections)) {
      collections[row.collection_id] = 0;
      collectionCount++;
    }
    collections[row.collection_id]++;
  });

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

  console.log(
    dominantCount,
    dominantCollection,
    collectionCount,
    matches!.rowCount,
    rowCount
  );

  if (dominantCollection === -1) {
    // TODO: There was no proper match
    console.log('no proper match 1');
    return null;
  } else if (collectionCount === 1 && matches!.rowCount === rowCount) {
    // if there is only one collection and all elements are matched
    return dominantCollection;
  }

  // check aginst the dominant collection
  if (geometryType === 'POINT' || geometryType === 'MULTIPOINT') {
    matches = await matchPoints(client, tableName, false, dominantCollection);
  } else if (geometryType === 'POLYGON' || geometryType === 'MULTIPOLYGON') {
    matches = await matchPolygons(client, tableName, false, dominantCollection);
  } else if (
    geometryType === 'LINESTRING' ||
    geometryType === 'MULTILINESTRING'
  ) {
    matches = await matchLines(client, tableName, false, dominantCollection);
  }

  console.log(matches!.rowCount, rowCount);

  if (matches!.rowCount === rowCount) {
    return dominantCollection;
  } else {
    const collectionCount = await client.query(
      'SELECT COUNT(*) AS rowcount FROM "Geometries" WHERE collection_id = $1',
      [dominantCollection]
    );
    if (matches!.rowCount === collectionCount!.rows[0].rowcount) {
      // TODO: The existing data is a subset of the new data
      console.log('subset');
    } else {
      // TODO: Looks like there is no match in the database
      console.log('no proper match 2');
    }
  }

  return null;
};
