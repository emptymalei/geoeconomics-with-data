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
import swifter

from .geo.util import file_exists as _file_exists
from .geo.util import distance_between_geom as _distance_between_geom
from .geo.util import split_dataframe as _split_dataframe

from .geo.config import get_geo_config as _get_geo_config
from .geo.util import osm_data_pipeline as _osm_data_pipeline

from .geo.util import isoencode as _isoencode
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

BATCH_SIZE = 3

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


def street_distance_to_point(apt_point, streets_df, max_distance):
    """Calculate distance from a point to streets and fine the
    """

    start_time = time.time()
    streets_df['distance'] = streets_df.swifter.progress_bar(
        False
        ).allow_dask_on_strings().apply(
        lambda x: _distance_between_geom(x.geometry, apt_point, PROJECT), axis=1
        )
    end_time = time.time()
    _logger.debug(f'{end_time - start_time} seconds used ended for {apt_point}')

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


# Connecting the pipes
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

    args = parser.parse_args()
    _logger.setLevel(args.verbose)

    city = args.city

    if city:
        _logger.info(f'street_resources_selected: {city}')


if __name__ == "__main__":

    main()

    print('END of Game')
