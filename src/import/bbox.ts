import {Client} from 'pg';
import {handleThematic} from './thematic';

export const isBbox = async (
  client: Client,
  matchId: number
): Promise<boolean> => {
  const match = await client
    .query('SELECT * FROM "Matches" WHERE id = $1', [matchId])
    .then(result => result.rows[0]);
  const geom = await client
    .query(
      `SELECT ST_AsGeoJson(ST_CurveToLine(geom_3857)) AS geojson FROM ${match.table_name} LIMIT 11`
    )
    .then(result => (result.rows ? result.rows : []));
  // sometimes these files hold multiple bbox ??
  if (geom.length <= 10) {
    let allBbox = true;
    geom.forEach(g => {
      let thisBbox = false;
      const geojson = JSON.parse(g.geojson);
      let coords: [number, number][];
      // the nesting on polygons sometimes differs
      if (
        geojson.coordinates[0] &&
        geojson.coordinates[0][0] &&
        geojson.coordinates[0][0][0] &&
        typeof geojson.coordinates[0][0][0] === 'object'
      ) {
        coords = geojson.coordinates[0][0];
      } else {
        coords = geojson.coordinates[0];
      }
      if (
        geojson.type === 'Polygon' &&
        coords.length === 5 &&
        coords[0][0] === coords[4][0] &&
        coords[0][1] === coords[4][1]
      ) {
        const x: number[] = [];
        const y: number[] = [];
        coords.forEach((c: number[]) => {
          if (!x.includes(c[0])) {
            x.push(c[0]);
          }
          if (!y.includes(c[1])) {
            y.push(c[1]);
          }
        });
        if (x.length === 2 && y.length === 2) {
          thisBbox = true;
        }
      }
      if (!thisBbox) {
        allBbox = false;
      }
    });
    return allBbox;
  }
  return false;
};

export const handleBbox = (
  odcsClient: Client,
  client: Client,
  matchId: number
): Promise<void> => {
  return handleThematic(odcsClient, client, matchId, '13');
};
