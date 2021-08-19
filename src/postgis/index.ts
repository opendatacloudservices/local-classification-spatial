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

// TODO: dump multigeometries as single geometries for easier processing: https://postgis.net/docs/ST_Dump.html
export const cleanGeometries = async (
  client: Client,
  tableName: string
): Promise<string> => {
  const newTableName = tableName + '_cln';

  const geomType = await getGeometryType(client, tableName);
  let newGeomType = 'POLYGON';
  if (geomType === 'POINT' || geomType === 'MULTIPOINT') {
    newGeomType = 'POINT';
  } else if (geomType === 'LINESTRING' || geomType === 'MULTILINESTRING') {
    newGeomType = 'LINESTRING';
  }

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
    `SELECT DISTINCT ON (${tableName}.fid, collection_geometries.id)
      collection_geometries.id as target_id,
      collection_geometries.collection_id,
      ${tableName}.fid as source_id
    FROM collection_geometries
    ${
      open
        ? `
    JOIN collections ON collection_geometries.collection_id = collections.id
    INNER JOIN ${tableName} ON
      (collections.type = 'POINT' OR collections.type = 'MULTIPOINT') AND 
      ST_DWithin(collection_geometries.geom, ${tableName}.geom, 20)
    `
        : `
    CROSS JOIN ${tableName}
        WHERE 
          collection_geometries.collection_id = $1 AND
          ST_Distance(collection_geometries.geom, ${tableName}.geom) < 20
    `
    }
    ORDER BY ST_Distance(collection_geometries.geom, ${tableName}.geom), ${tableName}.fid`,
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
    `SELECT DISTINCT ON(${tableName}.fid, collection_geometries.id)
      collection_geometries.fid as target_id,
      collection_geometries.collection_id,
      ${tableName}.fid as source_id
    FROM collection_geometries
    ${
      open
        ? `
    JOIN collections ON collection_geometries.collection_id = collections.id
    INNER JOIN ${tableName} ON
      (collections.type = 'LINESTRING' OR collections.type = 'MULTILINESTRING') AND 
      ST_Contains(ST_Buffer(collection_geometries.geom, 20), ${tableName}.geom)
    WHERE
      (ST_Length(collection_geometries.geom) > ST_Length(${tableName}.geom) * 0.9 AND 
      ST_Length(collection_geometries.geom) < ST_Length(${tableName}.geom) * 1.1)
      AND
      (ST_Area(ST_Envelope(collection_geometries.geom)) > ST_Area(ST_Envelope(${tableName}.geom)) * 0.9 AND 
      ST_Area(ST_Envelope(collection_geometries.geom)) < ST_Area(ST_Envelope(${tableName}.geom)) * 1.1)
    `
        : `
    CROSS JOIN ${tableName}
        WHERE 
          collection_geometries.collection_id = $1 AND
          ST_Contains(ST_Buffer(collection_geometries.geom, 20), ${tableName}.geom) AND
          (ST_Length(collection_geometries.geom) > ST_Length(${tableName}.geom) * 0.9 AND 
          ST_Length(collection_geometries.geom) < ST_Length(${tableName}.geom) * 1.1)
          AND
          (ST_Area(ST_Envelope(collection_geometries.geom)) > ST_Area(ST_Envelope(${tableName}.geom)) * 0.9 AND 
          ST_Area(ST_Envelope(collection_geometries.geom)) < ST_Area(ST_Envelope(${tableName}.geom)) * 1.1)
    `
    }
    ORDER BY ST_HausdorffDistance(collection_geometries.geom, ${tableName}.geom), ${tableName}.fid`,
    open ? [] : [collection_id]
  );
};

export const matchPolygons = (
  client: Client,
  tableName: string,
  open: boolean,
  collection_id?: number
): Promise<QueryResult> => {
  return client.query(
    `SELECT DISTINCT ON(${tableName}.fid, collection_geometries.id)
      collection_geometries.id as target_id,
      collection_geometries.collection_id,
      ${tableName}.fid as source_id
    FROM collection_geometries
    ${
      open
        ? `
    JOIN collections ON collection_geometries.collection_id = collections.id
    INNER JOIN ${tableName} ON
      (collections.type = 'POLYGON' OR collections.type = 'MULTIPOLYGON') AND 
      ST_Contains(ST_Buffer(collection_geometries.geom, 20), ${tableName}.geom)
    WHERE
      ST_Area(collection_geometries.geom) > ST_Area(${tableName}.geom) * 0.9 AND 
      ST_Area(collection_geometries.geom) < ST_Area(${tableName}.geom) * 1.1
    `
        : `
    CROSS JOIN ${tableName}
      WHERE 
        collection_geometries.collection_id = $1 AND
        ST_Contains(ST_Buffer(collection_geometries.geom, 20), ${tableName}.geom) AND
        ST_Area(collection_geometries.geom) > ST_Area(${tableName}.geom) * 0.9 AND 
        ST_Area(collection_geometries.geom) < ST_Area(${tableName}.geom) * 1.1
    `
    }
    ORDER BY ST_HausdorffDistance(collection_geometries.geom, ${tableName}.geom), ${tableName}.fid`,
    open ? [] : [collection_id]
  );
};

export const matchGeometries = async (
  client: Client,
  tableName: string,
  geometryType: GeometryType
): Promise<null | number> => {
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

  if (dominantCollection === -1) {
    // TODO: There was no proper match
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

  if (matches!.rowCount === rowCount) {
    return dominantCollection;
  } else {
    const collectionCount = await client.query(
      'SELECT COUNT(*) AS rowcount FROM collection_geometries WHERE collection_id = $1',
      [dominantCollection]
    );
    if (matches!.rowCount === collectionCount!.rowCount) {
      // TODO: The existing data is a subset of the new data
    } else {
      // TODO: Looks like there is no match in the database
    }
  }

  return null;
};
