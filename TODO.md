- standardize attribute keys in separate table
- 
- remove table after insert
- recheck tables
- check if file exists > note error

err Error: Command failed: ogr2ogr       -f "PostgreSQL"       "PG:        host=localhost         user=opendataservices         dbname=spatial         password=opendataservices"       "/media/data/odcs/downloads/541579--org.38.16f3e9bd-daa3-44ca-a7e7-94bf52bffcd8/layer_1_xplan:XP_Rasterdarstellung.gpkg"       -lco GEOMETRY_NAME=geom       -lco FID=fid       -lco SPATIAL_INDEX=GIST       -nln db_1630513633363layer1xplanxpra       -t_srs EPSG:4326
ERROR 1: ERROR:  duplicate key value violates unique constraint "pg_type_typname_nsp_index"
DETAIL:  Key (typname, typnamespace)=(db_1630513633363layer1xplanxpra_fid_seq, 2200) already exists.

ERROR 1: CREATE TABLE "public"."db_1630513633363layer1xplanxpra" ( "fid" SERIAL, PRIMARY KEY ("fid"), "gml_id" VARCHAR NOT NULL, "georefurl" VARCHAR, "referenzname" VARCHAR, "referenzurl" VARCHAR, "datum" VARCHAR )
ERROR:  duplicate key value violates unique constraint "pg_type_typname_nsp_index"
DETAIL:  Key (typname, typnamespace)=(db_1630513633363layer1xplanxpra_fid_seq, 2200) already exists.

ERROR 1: Unable to write feature 1 from layer xplan:XP_Rasterdarstellung.
ERROR 1: Terminating translation prematurely after failed
translation of layer xplan:XP_Rasterdarstellung (use -skipfailures to skip errors)




err error: table "db_1630513633363layer1xplanxpra" does not exist





