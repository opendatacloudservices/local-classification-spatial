import {statSync} from 'fs';

// 500 MB
const fileSizeLimit = 500 * (1024 * 1024);

export const sizeLimit = (file: string): boolean => {
  const stats = statSync(file);
  if (stats.size <= fileSizeLimit) {
    return true;
  }
  return false;
};
