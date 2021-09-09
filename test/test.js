const dotenv = require('dotenv');
const path = require('path');
const {Client} = require('pg');
const fs = require('fs');

const {isXplan, handleXplan} = require('../build/import/xplan');

// get environmental variables
dotenv.config({path: path.join(__dirname, '../.env')});

const getClient = async () => {
  // connect to postgres (via env vars params)
  const client = new Client({
    user: process.env.PGUSER,
    host: process.env.PGHOST,
    database: process.env.PGDATABASE,
    password: process.env.PGPASSWORD,
    port: parseInt(process.env.PGPORT || '5432'),
  });
  await client.connect().catch(err => {
    console.log(err);
  });
  return client;
};

const getODSClient = async () => {
  // opendataservices database with imports
  const client = new Client({
    user: process.env.ODCS_PGUSER,
    host: process.env.ODCS_PGHOST,
    database: process.env.ODCS_PGDATABASE,
    password: process.env.ODCS_PGPASSWORD,
    port: parseInt(process.env.ODCS_PGPORT || '5432'),
  });
  await client.connect().catch(err => {
    console.log(err);
  });
  return client;
};

test('isXplan', async () => {
  const client = await getClient();
  const odsClient = await getODSClient();

  for (let m = 6; m <= 54; m += 1) {
    const result = await isXplan(client, odsClient, m);
    if (!result) {
      console.log(m);
    }
    expect(result).toBe(true);
  }
});

test('handleXplan', async () => {
  const client = await getClient();
  const odsClient = await getODSClient();

  for (let m = 6; m <= 54; m += 1) {
    if (await isXplan(client, odsClient, m)) {
      await handleXplan(client, odsClient, m);
    }
  }

  const exists = fs.existsSync(process.env.PLANNING_LOCATION + '/433663.zip');
  expect(exists).toBe(true);
});
