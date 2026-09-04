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

## Start the server

Copy the `.env.sample` into `.env` and setup your own environment variables, then use `dev.sh` — the recommended way to run the app:

```
./dev.sh up          # start and stream the logs
./dev.sh up -d       # start detached, prints the URL
./dev.sh down        # stop it
./dev.sh logs -f     # follow the logs
```

It handles the `.env`, picks a stable port (one per git worktree) and installs
the versioned git hooks (pre-push runs the tests). It takes an optional worktree
name: `./dev.sh up my-feature` serves `.worktrees/my-feature`.

Under the hood it is just Flask, if you prefer running it yourself:

```
uv run --env-file .env flask --app ./src/app.py run --debug
```
