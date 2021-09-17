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

## Premise

Geometries are always split up into their smallest units. Every such geometry unit is only stored once in the database. If a geometry unit changes, old and new are both stored. Attributes are then connected to those such geometries.

## Data example

**Collections**
id: 1
name (unique): districts > the relationships of different types is documented in CollectionRelations
type: POLYGON
current_geometry_source_id: 1 (for performance store which source holds the current geometry)

This specific collection received two geometry imports (two version at different times):

**Source**
id: 1
import_id: 48902
collection_id: 1
geometry_source_id: NULL (If a source has geometry (and attributes)) this is NULL (see below)
previous_id: Source.id of previous geometry (can also be derived by imported_at)
imported_at: 2021-07-03 21:00:00

The geometries imported from that specific source at that specific point in time is stored in the **Geometries** table:

**Geometries**
fid: 1
geom: ...
source_id: 1

NOTE: Geometries often come as MULTI-Geometries, this makes it extremely hard to compare geometries, e.g.: Sometimes the islands in the east and north sea are included in certain geometries, sometimes they are not included. Comparing the geometry of the affected federal states would result in a not good enough match. To overcome this. Those MULTI-Geometries are stored as SINGLE-Geometries, still connected through their shared FID. This allows us to quickly identify good geometry matches and furthermore see that a match is really good and, in case of the missing islands, a subset of the more complete version of the geometry. Downside higher storage usage, certain queries will take longer, while other perform a lot faster.

For faster processing, we precalculate a couple of geometry properties, those are for each FID (not the SINGLE-Geometries).

**GeometryProps**
soruce_id: 1 (source_id, fid are a combined unique key)
fid: 1
area, centroid, len, name/names

Now it gets a bit complicated. For each geometry (fid) we have several Attribute sets, they can come from the same import source, we still need to store them separately, because a lot of times we will only import the attributes, because we already have the geometries stored in our system:

**Source**
(see above)
geometry_source_id: 1 (if the source's geometry is not imported, we reference the closest matching source's geometry)
import_id: 48903
imported_at: 2021-08-01 00:10:01

The values of such an attribute import are stored as **GeometryAttributes**:

**GeometryAttributes**
attributesource_id (combined unique key is attributesource_id, fid, key)
fid: 1
key: Bevölkerung
value: 3244

Values can be stored as str, int, float or an array of one of the three.

TODO: The key is currently simply derived from the original column name of the dataset. In the future those names need to be classified and then stored in a different table and connected through an id. 

## Classification pipeline

The system is build to handle three types of spatial data:

### 1. Planning

*/matches/setxplan/:id*

A lot of geometries in the open data realm are updates to planning documents. Those updates usually only hold a handful of geometries. Unless you are interested in that specific planning process, those geometries are more or less useless. Those geometries are not imported, instead their geometry type is being identified, a bounding box is calculated and then a fixed geometry is stored in a zipped geojson for easy access.

### 2. Thematic

*/matches/setthematic/:id*

Open data is highly heterogenic, many datasets only exist in once specifc region. Those data sets are classified as "thematic". Each thematic data set is classified under a category (potential groups of categories not yet create but database structure exists). In addition those geometries are also cleaned and stored as zipped geojsons for easy access.

#### Why zipped geojson?

zipped obviously to save storage. Geojson because of web-compatibility. If it were for efficient use on the server we would store them as geopackages, but fur quickly puttin them on a web map, geojson is the way to go.

### 3. Spatial taxonomies

Spatial taxonomies are the actual hear of this package. These data sets are thematic and hierarchical taxonomies across space and time. They are organisied in collections (see data example above). To import those taxonomies a few endpoints exist:

*/import/new*

Add a new collection and add first geometry.

*/import/update* ?collection_id=1&previous=2

Over time geomtries are being updated, e.g. voting districts are updated every year.
This does not change the collection, it adds a new geometry source and attached geometries.

*/import/add* ?collection_id=1

A collection could for example be (city) districts. City districts are provided by each city. Therefore the collection of (city) districts has multiple sets of geometry (for each city).
Add is adding a new geometry to a dataset.

*/import/merge*

Merge is more or less the same than */import/add* with the only difference that it takes an addition argument on how to handle duplicate geometries:
- *replace* - replace existing geometries with new geometries > acts like an update, but only imports geometries that already exist
- *skip* - only import geometries that do not yet exist, ignore duplicates
Both functions store a report on overlaps within the haussmanm column.