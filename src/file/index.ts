import {statSync, unlinkSync, existsSync} from 'fs';
import {exec} from 'child_process';

// 500 MB
const fileSizeLimit = 500 * (1024 * 1024);

export const sizeLimit = (file: string): boolean => {
  const stats = statSync(file);
  if (stats.size <= fileSizeLimit) {
    return true;
  }
  return false;
};

export const geojson = async (
  sourceFile: string,
  destinationFile: string
): Promise<void> => {
  return new Promise((resolve, reject) => {
    exec(
      `ogr2ogr \
      -t_srs "EPSG:4326" \
      -f "GeoJSON" \
      -skipfailures \
      "${destinationFile}.geojson" \
      "${sourceFile}"`,
      (err, stdout, stderr) => {
        if (err) {
          reject({err, stdout, stderr});
        }
        exec(
          `zip -j ${destinationFile}.zip ${destinationFile}.geojson`,
          (err, stdout, stderr) => {
            if (err) {
              reject({err, stdout, stderr});
            }
            if (existsSync(destinationFile + '.geojson')) {
              unlinkSync(destinationFile + '.geojson');
            }
            resolve();
          }
        );
      }
    );
  });
};
