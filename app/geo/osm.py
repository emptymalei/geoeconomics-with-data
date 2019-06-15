import simplejson as json
import os
import datetime
import argparse
import logging
from time import sleep as _sleep

from app.geo.config import get_geo_config as _get_geo_config
from app.geo.sourcing import data_downloader as _data_downloader
from app.geo.sourcing import pbf_filter as _pbf_filter
from app.geo.sourcing import pbf2geojson as _pbf2geojson
from app.geo.transformer import clean_up_geojson as _clean_up_geojson
from app.geo.transformer import OSMStreetTransformations
from app.geo.transformer import transform_records_and_save_to_file as _transform_records_and_save_to_file

from app.geo.util import check_and_convert_to_date as _check_and_convert_to_date

logging.basicConfig()
_logger = logging.getLogger('app.geo.osm')

__cwd__ = os.getcwd()
__location__ = os.path.realpath(
    os.path.join(__cwd__, os.path.dirname(__file__))
    )

# PARAMS

GEO_CONFIG = _get_geo_config()
GEO_RESOURCES = GEO_CONFIG.get('resources')
HIGHWAY_FILTERS = GEO_CONFIG.get('filters')

# Define the Pipes

def data_downloader(osm_resource):
    return _data_downloader(
        osm_resource.get("source"),
        osm_resource.get("pbf_file")
    )


# Connecting the pipes

def osm_data_pipeline(
    schema,
    transformations,
    osm_resource
    ):
    """Download, Transform, and Upload one poi resource
    """

    today_is = datetime.date.today().isoformat()

    res_osm_log = {}

    ### Download pbf file
    _logger.info('Downloading pbf from: ', osm_resource.get("source"))
    poi_download_log = data_downloader(osm_resource)

    res_osm_log['download'] = poi_download_log

    ### Extract highway pbf from all
    _logger.info('Extacting highways from all pbf: ', osm_resource.get("pbf_file"))
    poi_pbf_highway_log = _pbf_filter(
            osm_resource.get("pbf_file"),
            osm_resource.get("pbf_file_highway"),
            HIGHWAY_FILTERS,
            bounding_box=osm_resource.get('pbf_filter_bounding_box')
        )
    res_osm_log['pbf2geojson'] = poi_pbf_highway_log

    ### Convert pbf to geojson
    _logger.info('Converting pbf to geojson: ', osm_resource.get("pbf_file_highway"))
    poi_pbf2geojson_log = _pbf2geojson(
        osm_resource.get("pbf_file_highway"),
        osm_resource.get("geojson_file")
    )
    res_osm_log['pbf2geojson'] = poi_pbf2geojson_log

    ### Clean up geojson
    _logger.info('Cleaning up geojson file for ', osm_resource.get("geojson_file"))
    poi_clean_geojson_log = _clean_up_geojson(
        osm_resource.get("geojson_file"),
        load_only_key = "features"
    )
    res_osm_log['clean_geojson'] = {
        "geojson_file": osm_resource.get("geojson_file")
    }

    ### transform data
    available_osm_transformers = [x for x in dir(transformations) if not x.startswith('_')]

    _logger.info('Transforming: '.format( osm_resource.get("geojson_file") ) )
    _transform_records_and_save_to_file(
            schema,
            available_osm_transformers,
            transformations,
            observation_date=today_is,
            input_file=osm_resource.get('geojson_file'),
            output_file=osm_resource.get('transformed_json_file')
        )
    res_osm_log['transformations'] = {
        "geojson_file": osm_resource.get("geojson_file"),
        "transformed_json_file": osm_resource.get('transformed_json_file')
    }


    return res_osm_log


## Workflow

def main():
    """Connecting the pipes
    """

    today_is = datetime.date.today()

    ### Options
    parser = argparse.ArgumentParser(description='Options for Getting OSM Street data')

    parser.add_argument(
        '-c', '--city',
        dest='city',
        nargs='+',
        help= 'Specify city resource to be used'
        )

    args = parser.parse_args()
    geo_city = args.city
    if geo_city:
        _logger.info(f'--city: {geo_city}')
    else:
        raise ValueError('Wrong input --city: {}'.format(geo_city) )

    # Get resource for the cities
    osm_resources_selected = []
    for geo_resource in GEO_RESOURCES:
        if geo_resource.get('city') in geo_city:
            osm_resources_selected.append(geo_resource)

    #### Load Transformers
    with open(os.path.join(__location__, 'schema', 'city_streets.json'), 'rb') as schema_file:
            schema = json.load(schema_file)

    osm_transformer = OSMStreetTransformations()

    ### Iterate through selected poi resources
    #
    for osm_resource in osm_resources_selected:
        osm_data_pipeline(
            schema=schema,
            transformations=osm_transformer,
            osm_resource=osm_resource
            )


if __name__ == "__main__":

    main()

    print('done')