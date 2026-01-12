# ATP 2 OSM Import

In this project, All The Places data are imported into OpenStreetMap.

## Starting the containers

```
podman-compose up -d

podman-compose run osm2pgsql osm2pgsql --output flex -S /osm2pgsql/generic.lua -d o2p -U o2p -H 127.0.0.1 -P 5432 /data/osm/your-file.osm.pbf
```

## Install dependencies

```
uv sync
```

## Run the script

```
uv run main.py
```
