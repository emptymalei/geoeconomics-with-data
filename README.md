# geoeconomics

Proof of concept for a complete project for ETL for geoeconomics.

This package will download osm data, extract street data, and clean up the data. Some other useful data parsing functions are also included.


## Development

`Makefile` will deal with everything:

1. `make build` will build the docker locally
2. `publish` will publish the docker image to docker registry

Changes should be made before push using this file.

The code is localed in the folder `app`.

```
app
├── config
│   └── geo.yml
├── distance_calculator.py
└── geo
    ├── config.py
    ├── osm.py
    ├── schema
    │   └── city_streets.json
    ├── sourcing.py
    ├── transformer.py
    └── util.py
```

