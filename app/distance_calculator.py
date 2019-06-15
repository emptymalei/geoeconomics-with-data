import argparse
import ast
import datetime
import logging
import os
import time
from ast import literal_eval
from functools import partial
from time import sleep as _sleep

import geopandas as gpd
import pandas as pd
import pyproj
import simplejson as json

from app.geo.util import file_exists as _file_exists
from app.geo.util import distance_between_geom as _distance_between_geom
from app.geo.util import split_dataframe as _split_dataframe
from app.geo.util import save_records as _save_records
from app.geo.transformer import OSMStreetTransformations

from app.geo.config import get_geo_config as _get_geo_config
from app.geo.osm import osm_data_pipeline as _osm_data_pipeline

from app.geo.util import isoencode as _isoencode
from shapely.geometry import LineString, Point, Polygon
from shapely.ops import transform

logging.basicConfig()
_logger = logging.getLogger('app.distance-calculator')

__cwd__ = os.getcwd()
__location__ = os.path.realpath(
    os.path.join(__cwd__, os.path.dirname(__file__))
    )

# Define the project to be used to calculate distances
PROJECT = partial(
    pyproj.transform,
    pyproj.Proj(init='EPSG:4326'),
    pyproj.Proj(init='EPSG:32633'))

GEO_CONFIG = _get_geo_config()
GEO_RESOURCES = GEO_CONFIG.get('resources')


def load_street_data(geojson_file_path=None):
    """Load street data into geodataframe
    """

    if geojson_file_path:
        geojson_file_exists, geojson_file_size = _file_exists(
            geojson_file_path
            )

        if geojson_file_exists:
            geojson_data = []
            with open(geojson_file_path) as fp:
                for line in fp:
                    geojson_data.append(json.loads(line))

            df_geopandas_output = gpd.GeoDataFrame( geojson_data )
            del geojson_data
            df_geopandas_output_length = len(df_geopandas_output)
            _logger.info(
                f"""Loaded {geojson_file_path} ({geojson_file_size} Mb);
                    produced GeoDataFrame with {df_geopandas_output_length} rows!"""
                )

            return df_geopandas_output
        else:

            with open(os.path.join(__location__, 'schema', 'city_streets.json'), 'rb') as schema_file:
                schema = json.load(schema_file)

            osm_transformer = OSMStreetTransformations()
            _osm_data_pipeline(
                schema=schema,
                transformations=osm_transformer,
                osm_resource=osm_resource
            )
            raise FileNotFoundError(
                f'{geojson_file_path} not found! Need to run the data pipeline?'
                )



def prepare_street_data(df_inp):
    """Prepare street data with geometries to be used for the distance calculations

    """

    # convert geojson dict string to actual dict
    df_highway_intermediate = df_inp.copy()
    df_highway_intermediate['geometry'] = df_highway_intermediate.apply(
        lambda x: ast.literal_eval(x.geometry), axis=1
        )

    # extract highway types
    df_highway_intermediate['highway'] = df_highway_intermediate.apply(
        lambda x: x.types.get('highway'), axis=1
        )
    # extract and filter geometry type
    df_highway_intermediate['geom_type'] = df_highway_intermediate.apply(
        lambda x: x.geometry.get('type'), axis=1
        )
    df_highway_intermediate = df_highway_intermediate[
        df_highway_intermediate['geom_type'] == 'LineString'
        ]
    # reset index since we have remove some rows
    df_highway_intermediate.reset_index(drop=True, inplace=True)

    _logger.info(
        'Loaded {} clean geopandas data'.format(len(df_highway_intermediate))
        )

    # construct actual shapely geometry object
    df_highway_intermediate['geometry'] = df_highway_intermediate.apply(
        lambda x: LineString( x.geometry.get('coordinates') ), axis=1
        )
    df_highway_intermediate.set_geometry('geometry', inplace=True)

    # select essential columns
    df_highway_intermediate = df_highway_intermediate[
        ['geometry', 'id', 'name', 'highway', 'observation_date']
    ]

    return df_highway_intermediate


def street_distance_to_point(geo_point, streets_df, max_distance=None):
    """Calculate distance from a point to streets and fine the

    :param geo_point: (longitude,latitude), this should be a string
    """
    if isinstance(geo_point, (str)):
        geo_point = literal_eval(geo_point)
    geo_point_longitude, geo_point_latitude = geo_point
    geo_point = Point(geo_point_longitude, geo_point_latitude)

    start_time = time.time()
    streets_df['distance'] = streets_df.apply(
        lambda x: _distance_between_geom(x.geometry, geo_point, PROJECT), axis=1
        )
    end_time = time.time()
    _logger.debug(f'{end_time - start_time} seconds used ended for {geo_point}')

    if max_distance:
        streets_df = streets_df[ streets_df['distance'] <= max_distance ]

    if streets_df.empty:
        _logger.warning(f"Got no nearby streets!")
        return [], datetime.datetime.today().strftime('%Y-%m-%d')
    else:
        streets_df = streets_df.groupby(
            ['name','highway']
            ).apply(
                lambda x: x.sort_values(by='distance').iloc[0]
                )

        streets_df = streets_df.reset_index(drop=True).sort_values(by='distance')

        observation_date = streets_df.observation_date.iloc[0]

        return streets_df[['id', 'name', 'highway', 'distance']].to_dict(
            orient = 'record'
            ), observation_date


def save_data(records, output):

    # Check if the output json file exists
    street_distance_json_file = output
    street_distance_json_file_exists, _ = _file_exists(
        street_distance_json_file
        )

    if street_distance_json_file_exists:
        _logger.info(
            street_distance_json_file + ' already exists! Will delete the file'
            )
        try:
            os.remove(street_distance_json_file)
        except Exception as ee:
            raise Exception(
                'Could not remove old data file for apt nearby streets'
                )

    _save_records(records, output)


# Connecting the pipes
def geo_distance_calculator(street_resource, geo_points):
    """Calculate distances to the given point
    """

    # Load transformed street data
    df_streets = load_street_data(
        geojson_file_path=street_resource.get('transformed_json_file')
        )
    df_streets = prepare_street_data(df_streets)

    res = []
    for geo_point in geo_points:
        geo_records, date = street_distance_to_point(geo_point, df_streets)
        res.append(
            {
                "records": geo_records
            }
        )

    return {
        "data": res
    }


def main():
    """Use the street data to calculate nearby street for any given point
    """

    parser = argparse.ArgumentParser(description='Distance Calculator')

    parser.add_argument(
        "-v", "--verbose",
        help="Increase output verbosity",
        action="store_const",
        const=logging.DEBUG, default=logging.INFO
        )

    parser.add_argument(
        '-c', '--city',
        dest='city',
        help='Specify the city to be calculated. You will have to define the params for the city in a config file'
        )

    parser.add_argument(
        '-p', '--point',
        dest='point',
        nargs='+',
        help='Specify the points to be calculated; e.g., (10.2323,52.9384) (10.012,52.1923)'
    )

    parser.add_argument(
        '-o', '--output',
        dest='output',
        help='Path to output data'
    )

    args = parser.parse_args()
    _logger.setLevel(args.verbose)

    city = args.city
    geo_points = args.point
    output_path = args.output

    if city:
        _logger.info(f'street_resources_selected: {city}')
        for geo_resource in GEO_RESOURCES:
            if geo_resource.get('city') == city:
                city_resource = geo_resource

    res = geo_distance_calculator(city_resource, geo_points)

    save_data(res.get('data'), output_path)

    return res


if __name__ == "__main__":

    main()

    print('END of Game')
