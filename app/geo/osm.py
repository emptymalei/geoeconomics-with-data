import simplejson as json
import os
import datetime
import argparse
import logging
from time import sleep as _sleep

from .config import get_geo_config as _get_geo_config
from .sourcing import data_downloader as _data_downloader
from .sourcing import pbf_filter as _pbf_filter
from .sourcing import pbf2geojson as _pbf2geojson
from .transformer import clean_up_geojson as _clean_up_geojson
from .transformer import OSMStreetTransformations
from .transformer import transform_records_and_save_to_file as _transform_records_and_save_to_file

from .util import check_and_convert_to_date as _check_and_convert_to_date

logging.basicConfig()
_logger = logging.getLogger('app.geo.osm')

__cwd__ = os.getcwd()
__location__ = os.path.realpath(
    os.path.join(__cwd__, os.path.dirname(__file__))
    )

# PARAMS

GEO_CONFIG = _get_geo_config()
STREET_RESOURCES = GEO_CONFIG.get('resources')
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

    today_is = datetime.date.today()

    res_osm_log = {}

    ### Download pbf file
    _logger('Downloading pbf from: ', osm_resource.get("source"))
    poi_download_log = data_downloader

    res_osm_log['download'] = poi_download_log

    ### Extract highway pbf from all
    _logger('Extacting highways from all pbf: ', osm_resource.get("pbf_file"))
    poi_pbf_highway_log = _pbf_filter(
            osm_resource.get("pbf_file"),
            osm_resource.get("pbf_file_highway"),
            HIGHWAY_FILTERS,
            bounding_box=osm_resource.get('pbf_filter_bounding_box')
        )
    res_osm_log['pbf2geojson'] = poi_pbf_highway_log

    ### Convert pbf to geojson
    _logger('Converting pbf to geojson: ', osm_resource.get("pbf_file_highway"))
    poi_pbf2geojson_log = _pbf2geojson(
        osm_resource.get("pbf_file_highway"),
        osm_resource.get("geojson_file")
    )
    res_osm_log['pbf2geojson'] = poi_pbf2geojson_log

    ### Clean up geojson
    _logger('Cleaning up geojson file for ', osm_resource.get("geojson_file"))
    poi_clean_geojson_log = _clean_up_geojson(
        osm_resource.get("geojson_file"),
        load_only_key = "features"
    )
    res_osm_log['clean_geojson'] = {
        "geojson_file": osm_resource.get("geojson_file")
    }

    ### transform data
    available_osm_transformers = [x for x in dir(transformations) if not x.startswith('_')]

    _logger('Transforming: '.format( osm_resource.get("geojson_file") ) )
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
    all_countries = [i for i in STREET_RESOURCES if i.get('level') == 'country' ]
    all_regions = [i for i in STREET_RESOURCES]
    all_states = [i for i in STREET_RESOURCES if i.get('level') == 'state' ]

    ### Options
    parser = argparse.ArgumentParser(description='Options for Getting OSM Street data')
    parser.add_argument(
        '--recreate-gbq-table',
        dest='recreate_gbq_table',
        default='true',
        choices=['true','false'],
        help='whether to recreate the destination table, the default is false.'
        )
    parser.add_argument(
        '--partition-date',
        dest='partition_date',
        default='today',
        help= (
            'specify the partition_date as the partition field. '
            'The partition table will be overwritten if this partition date already exists in GBQ!'
            )
        )
    parser.add_argument(
        '--region',
        dest='region',
        default='all',
        nargs='+',
        help= '; '.join([
            'specify the region to be downloaded and transformed. ',
            'all-countries: get all countries; {}'.format([i.get('location') for i in all_countries]),
            'all-states: get all states: {}'.format([i.get('location') for i in all_states]),
            '[region1 region2 region3 ...]: a list of regions to be download; {}'.format(
                [i.get('location') for i in STREET_RESOURCES]
                )
            ])
        )

    args = parser.parse_args()

    recreate_gbq_table = args.recreate_gbq_table
    if recreate_gbq_table.lower() == 'false':
        recreate_gbq_table = False
    elif recreate_gbq_table.lower() == 'true':
        print("Will overwrite GBQ tables!!!")
        recreate_gbq_table = True
    else:
        raise ValueError('option for overwrite is not recognized!')

    geo_region = args.region
    print('--region: ', geo_region)
    if 'all' in geo_region:
        osm_resources_selected = all_regions
        print('osm_resources_selected', osm_resources_selected)
    elif 'all-countries' in geo_region:
        osm_resources_selected = all_countries
        print('osm_resources_selected', osm_resources_selected)
    elif 'all-states' in geo_region:
        osm_resources_selected = all_states
        print('osm_resources_selected', osm_resources_selected)
    elif geo_region:
        osm_resources_selected = [i for i in STREET_RESOURCES if i.get('location') in geo_region ]
        print('osm_resources_selected', osm_resources_selected)
    else:
        raise ValueError('Wrong input region: {}'.format(geo_region) )

    partition_date = args.partition_date
    if partition_date == 'today':
        partition_date = '$' + today_is.strftime("%Y%m%d")
    else:
        partition_date = _check_and_convert_to_date(partition_date)
        if not partition_date:
            partition_date='$' + today_is.strftime("%Y%m%d")
        else:
            partition_date='$' + partition_date.strftime("%Y%m%d")

    #### initialize bigquery client
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