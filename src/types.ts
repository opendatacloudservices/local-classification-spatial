export type GeometryType =
  | 'POINT'
  | 'MULTIPOINT'
  | 'LINESTRING'
  | 'MULTILINESTRING'
  | 'POLYGON'
  | 'MULTIPOLYGON'
  | string;

export type Columns = {
  name: string;
  type: string;
  udt: string;
}[];
