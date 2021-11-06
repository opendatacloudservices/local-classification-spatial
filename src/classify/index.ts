import type {Columns} from '../types';

const ignoreColumns = [
  'shape_length',
  'shape_area',
  'geom',
  'geom_3857',
  'fid',
  'gml_id',
  'objectid',
];

export const filterColumns = (columns: Columns): Columns => {
  return columns.filter(c => {
    if (ignoreColumns.includes(c.name)) {
      return false;
    } else if (c.type.indexOf('timestamp') >= 0) {
      return false;
    }
    return true;
  });
};
