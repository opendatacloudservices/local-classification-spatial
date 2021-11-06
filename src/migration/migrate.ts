import * as dotenv from 'dotenv';
import * as path from 'path';
import {Client} from 'pg';

// get environmental variables
dotenv.config({path: path.join(__dirname, '../../.env')});

// connect to postgres (via env vars params)
const client = new Client({
  user: process.env.PGUSER,
  host: process.env.PGHOST,
  database: process.env.PGDATABASE,
  password: process.env.PGPASSWORD,
  port: parseInt(process.env.PGPORT || '5432'),
});

client.connect().catch((err: Error) => {
  console.log({message: err});
});

// opendataservices database with imports
const odcsClient = new Client({
  user: process.env.ODCS_PGUSER,
  host: process.env.ODCS_PGHOST,
  database: process.env.ODCS_PGDATABASE,
  password: process.env.ODCS_PGPASSWORD,
  port: parseInt(process.env.ODCS_PGPORT || '5432'),
});
odcsClient.connect().catch((err: Error) => {
  console.log({message: err});
});

import {exportCSV} from './index';
exportCSV(client).then(() => console.log('updateBuffer'));
