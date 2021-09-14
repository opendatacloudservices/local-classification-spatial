import {Client} from 'pg';
import {Download} from '../types';

export const get = (odscClient: Client, id: number): Promise<Download> => {
  return odscClient
    .query('SELECT * FROM "Downloads" WHERE id = $1', [id])
    .then(result => result.rows[0]);
};

export const getFromImportID = (
  odscClient: Client,
  id: number
): Promise<Download> => {
  return odscClient
    .query(
      'SELECT "Downloads".* FROM "DownloadedFiles" JOIN "Downloads" ON "DownloadedFiles".download_id = "Downloads".id WHERE "DownloadedFiles".id = $1',
      [id]
    )
    .then(result => result.rows[0]);
};
