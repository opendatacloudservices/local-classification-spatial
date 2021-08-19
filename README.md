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

