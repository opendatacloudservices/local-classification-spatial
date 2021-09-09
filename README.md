# local-classification-spatial
Classify a downloaded dataset in terms of its spatial structure

## Download test data

```bash
ogr2ogr -f gpkg test_data/NAME.gpkg WFS:WFS_URL FEATURE --config OGR_WFS_PAGE_SIZE 1000
```

Datasets:

- https://sgx.geodatenzentrum.de/wfs_gn250_inspire
- https://sgx.geodatenzentrum.de/wfs_gnde
- https://sgx.geodatenzentrum.de/wfs_kfz250
- https://sgx.geodatenzentrum.de/wfs_vg1000-ew
- https://sgx.geodatenzentrum.de/wfs_vg250-ew
- https://sgx.geodatenzentrum.de/wfs_vz250_0101
- https://sgx.geodatenzentrum.de/wfs_vz250_3112

## Database setup

We need to add remote mapping to our opendataservices database.

```sql
-- CREATE EXTENSION postgres_fdw
-- CREATE SERVER opendataservices FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host 'localhost', dbname 'opendataservices', port '5432');
-- CREATE USER MAPPING FOR opendataservices / postgres SERVER opendataservices OPTIONS (user 'opendataservices', password 'opendataservices');
/*CREATE FOREIGN TABLE "DownloadedFiles" (
        id integer NOT NULL,
        download_id integer,
		file text,
		spatial_classification boolean
)
        SERVER opendataservices
        OPTIONS (schema_name 'public', table_name 'DownloadedFiles');*/

/*CREATE FOREIGN TABLE "Downloads" (
        id integer NOT NULL,
        url text
)
        SERVER opendataservices
        OPTIONS (schema_name 'public', table_name 'Downloads');*/

/*CREATE FOREIGN TABLE "Files" (
        id integer NOT NULL,
        url text,
	dataset_id integer
)
        SERVER opendataservices
        OPTIONS (schema_name 'public', table_name 'Files');*/

-- ALTER FOREIGN TABLE "Files" ADD COLUMN meta_description text;
-- ALTER FOREIGN TABLE "Files" ADD COLUMN meta_name text;

/*CREATE FOREIGN TABLE "Imports" (
        id integer NOT NULL,
        meta_abstract text,
		bbox Geometry(Polygon, 4326)
)
        SERVER opendataservices
        OPTIONS (schema_name 'public', table_name 'Imports');*/
```