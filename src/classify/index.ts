import type {Columns} from '../types';

export const identifyNameColumn = (columns: Columns): string | null => {
  let nameColumn = null;
  // TODO: identify common name patters extent to regex
  const names = ['name', 'gen'];
  columns.forEach(c => {
    if (names.includes(c.name.toLowerCase())) {
      nameColumn = c.name;
    }
  });
  return nameColumn;
};
