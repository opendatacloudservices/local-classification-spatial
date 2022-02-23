import {Response} from 'express';
import * as fs from 'fs';
import * as jszip from 'jszip';
import {addToken, logError} from '@opendatacloudservices/local-logger';
import fetch from 'node-fetch';

export const date2timestamp = (date: Date): string => {
  return `${appendZero(date.getFullYear())}-${appendZero(
    date.getMonth()
  )}-${appendZero(date.getDay())} ${appendZero(date.getHours())}:${appendZero(
    date.getMinutes()
  )}:${appendZero(date.getSeconds())}`;
};

export const appendZero = (num: number): string => {
  return num < 10 ? '0' + num.toString() : num.toString();
};

export const countDecimals = (num: number): number => {
  if (Math.floor(num.valueOf()) === num.valueOf()) return 0;
  return num.toString().split('.')[1].length || 0;
};

export const wait = (time: number): Promise<void> => {
  return new Promise(resolve => {
    setTimeout(() => {
      resolve();
    }, time);
  });
};

export const saveZip = (
  content: string,
  fileName: string,
  zipPath: string
): Promise<void> => {
  const zip = new jszip();
  zip.file(fileName, content);

  return new Promise((resolve, reject) => {
    zip
      .generateNodeStream({type: 'nodebuffer', streamFiles: true})
      .pipe(fs.createWriteStream(zipPath))
      .on('error', err => {
        logError(err);
        reject(err);
      })
      .on('finish', () => {
        resolve();
      });
  });
};

// TODO: Move to local-microservice
export const fetchAgain = async (
  url: string,
  res: Response,
  pass = 0
): Promise<void> => {
  if (pass < 12) {
    await wait(pass === 0 ? 0 : 1000 * 60 * 5);
    try {
      await fetch(addToken(url, res));
    } catch (err) {
      logError({
        url,
        pass,
        message: 'could not complete request',
      });
      fetchAgain(url, res, pass + 1);
    }
  } else {
    logError({
      url,
      message: 'could not complete request after 12 tries (60 minutes)',
    });
    return Promise.resolve();
  }
};
