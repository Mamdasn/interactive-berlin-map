# Interactive Berlin Map

Go through events/protests in Berlin on an interactive map with a timeline.

## Quick Start

```bash
docker compose up -d --build
```

Open: `http://localhost`

## Docker Stack Setup

- `postgres` -> PostgreSQL DB
- `protestcrawler` -> fetches and stores events to `postgres`
- `nominatim` -> provides geocoding for addresses
- `tileserver-gl` -> provides map tiles
- `events-map-webapp` serves a interactive map
- `nginx` -> caches requests & optimizes the webapp endpoint

## Project Tree

```text
.
├── docker-compose.yml
├── events-map-webapp
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── server.py
│   ├── templates
│   │   ├── 429.html
│   │   └── index.html
│   └── wsgi.py
├── nginx
│   └── nginx.conf
├── protestcrawler
│   └── output
│       └── protestcrawler.logs
├── pbf
│   └── berlin-latest.osm.pbf # OpenStreetMap DB used by `nominatim`
├── tiles/            # mbtiles database volume
├── geocache/         # runtime cache of nominatim geocoding database volume
├── postgres_data/    # runtime DB volume
├── nominatim-data/   # runtime nominatim geocoding database volume
└── nginx-cache/      # runtime nginx cache volume for map tiles
```
