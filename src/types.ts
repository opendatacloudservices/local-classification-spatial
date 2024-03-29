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

export type Results = {[index: string]: number | string | null}[];

export type Download = {
  id: number;
  url: string;
  state: string;
  downloaded: Date;
  previous: number;
  file: string;
  format: string;
  mimetype: string;
  spatial_classification: boolean;
};

export type Match = {
  collection_id: number | null;
  process?: {
    sources: number[];
    sourceCount: number[];
    importCount: number;
    message: string;
    differences?: {
      collection_id: number;
      source_id: number;
      target_id: number;
      source_fid: number;
      target_fid: number;
      source_diff?: number;
      target_diff?: number;
      dist: number;
    }[];
  };
};

export type Matrix = [[number, number], [number, number], [number]][];

export type GeoJson = {
  type: string;
  features: GeoJsonFeature[];
};

export type GeoJsonFeature = {
  type: string;
  properties: {
    [index: string]: string | number | null;
  };
  geometry: {
    type: string;
    coordinates: number[] | number[][] | number[][][];
  };
};

export type DBMatch = {
  id: number;
  import_id: number;
  file: string;
  matches: number[];
  message: string;
  matches_count: number[];
  table_name: string;
  difference: {
    source_id: number;
    target_id: number;
    dist: number;
  }[];
};

export type DBMatchDetails = {
  file: string;
  url: string;
  name: string;
  file_name: string;
  description: string;
  function: string;
  abstract: string;
  format: string;
};

export type Collection = {
  id: number;
  type: string;
  bbox: string;
  name: string;
  current_geometry_source_id: number;
};
