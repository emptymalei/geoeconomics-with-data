# geoeconomics

Proof of concept for a complete project for ETL for geoeconomics.

This package will download osm data, extract street data, and clean up the data. Some other useful data parsing functions are also included.



## How to Use

This tool is dockerized and [published on docker registry](https://cloud.docker.com/u/emptymalei/repository/docker/emptymalei/geoeconomics-with-data).

```
docker pull emptymalei/geoeconomics-with-data
```

### Endpoints

`distance_calculator`: given the geocoordinates, output the distance to all streets in the specified city.

The following options are supported:

- `--point`/`-p`: geocoordinate, such as `-p '(10.2323,52.9384)'` (`(longitude,latitude)` without any white space)
- `--city`/`-c`: city name; the city name should be specified as a model in config file whose path can be specifid using `--config`/`-cfg` parameter if desired
- `--output`/`-o`: output data path
- `--verbose`/`-v` (optional): change logging levels
- `--config`/`-cfg` (optional): path to config file; default config file is located at `app/config/geo.yml`
- `--schema`/`-s` (optional): path to schema file being used for data transformations; default schema is located at `app/geo/schema/city_streets.json`

Example:

```
[...whatever_docker_path...] distance_calculator -p '(10.2323,52.9384)' -c berlin -o ~/Downloads/berlin.json
```

## Development


### Code

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

### Workflow

1. Download data from some service, and save locally.
2. Transform data and save locally
3. Do the calculations and save data to the path specified by `--output` option


This package uses the open street map data as our data source. The download and transformation are defined in the config file `geo.yml` under `resources` variable. For example, we have this for berlin.

```
- city: berlin
  source: "https://download.geofabrik.de/europe/germany/berlin-latest.osm.pbf"
  pbf_file: "/tmp/germany/berlin-latest.osm.pbf"
  pbf_file_highway: "/tmp/germany/berlin-latest-highway.osm.pbf"
  geojson_file: "/tmp/germany/berlin-latest.geojson"
  transformed_json_file: "/tmp/germany/berlin-latest-transformed.json"
```


### Adding New OSM City Model

Not all cities are included in the config file `app/config/geo.yml`.

To include a new city, either change add new city model to this default config file or make a copy and specify the path to new config using the `--config` option.

### Adding More Data Fields

The transformed data field saved locally in a file. Here is the example for berlin.


```
- city: berlin
  source: "https://download.geofabrik.de/europe/germany/berlin-latest.osm.pbf"
  pbf_file: "/tmp/germany/berlin-latest.osm.pbf"
  pbf_file_highway: "/tmp/germany/berlin-latest-highway.osm.pbf"
  geojson_file: "/tmp/germany/berlin-latest.geojson"
  transformed_json_file: "/tmp/germany/berlin-latest-transformed.json"
```

The config `transformed_json_file` specifies the path to the transformed data file. This fields in this data file is specified using a schema file. The default schema file is located at `app/geo/schema/city_streets.json`. A customized schema can be specified using the `--schema` option. Meanwhile, the transformers should also be included in `app/geo/transformer.py`.


### Adding More Functions

More functions can be added easily by adding new python files and setting up the endpoints in `setup.py`.

### Deployment


`Makefile` will deal with everything:

1. `make build` will build the docker locally
2. `publish` will publish the docker image to docker registry

Changes should be made before push using this file.

