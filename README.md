# Interactive Berlin Map

Go through events/protests in Berlin on an interactive map with a timeline.

## Quick Start

```bash
ansible-playbook -i inventory.ini site.yml -v
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
├── inventory.ini                                          # Ansible inventory
├── LICENSE
├── README.md
├── roles                                                  # Ansible roles
│   └── deploys                                            # Ansible deploys
│       ├── files                                          # Ansible files
│       │   ├── stack_a                                    # Main stack
│       │   │   ├── docker-compose.yml                     
│       │   │   ├── events-map-webapp                      # Interactive map app
│       │   │   │   ├── Dockerfile
│       │   │   │   ├── libs                               # Geolocation modules
│       │   │   │   │   ├── db_clients.py
│       │   │   │   │   ├── geocode_cache_warm_up.py
│       │   │   │   │   ├── geocoding.py
│       │   │   │   │   ├── geolib.py
│       │   │   │   │   ├── geo_utils.py
│       │   │   │   │   ├── __init__.py
│       │   │   │   │   └── locations.py
│       │   │   │   ├── requirements.txt                   # Interactive map app requirements
│       │   │   │   ├── server.py
│       │   │   │   ├── templates
│       │   │   │   │   └── index.html
│       │   │   │   └── wsgi.py
│       │   │   ├── nginx                                  # Nginx config templates
│       │   │   │   ├── 429.html
│       │   │   │   ├── error.html
│       │   │   │   └── nginx.conf.template                # Main nginx config
│       │   │   ├── pbf                                    # OpenStreetMap Berlin OSM dataset
│       │   │   │   └── berlin-latest.osm.pbf
│       │   │   └── tiles                                  # Tile server data
│       │   │       ├── basic.json                         # Tile style config
│       │   │       ├── config.json                        # Tile config
│       │   │       └── berlin.mbtiles                     # MBTiles database
│       │   └── stack_b                                    # Front stack
│       │       ├── docker-compose.yml
│       │       └── nginx                                  # Nginx config templates
│       │           ├── blocked_ips.conf                   # cached IP denylist
│       │           ├── common_proxy_rules.inc.template    # Reverse-proxy rules
│       │           ├── https_server_common.inc.template   # Https rules
│       │           ├── maps_tiles_locations.inc.template  # Map and tile routing rules
│       │           └── nginx.conf.template                # Main nginx config
│       ├── handlers                                       # Ansible handlers
│       │   └── main.yml                                   
│       ├── tasks                                          # Ansible tasks
│       │   └── main.yml                                   
│       └── templates                                      # Ansible jinja templates
│           └── stack_b
│               └── nginx
│                   └── cloudflare_real_ip.inc.template.j2 # Cloudflare IPs template
└── site.yml                                               # Ansible playbook
```
