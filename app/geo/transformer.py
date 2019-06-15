import datetime
import logging
import os
import re
import traceback
from shutil import copyfile

import numpy as np
import simplejson as json

from .schema import schema as _schema
from .util import \
    check_and_convert_to_datetime as _check_and_convert_to_datetime
from .util import get_dict_val_recursively as _get_dict_val_recursively
from .util import insert_to_dict_at_level as _insert_to_dict_at_level
from .util import isoencode as _isoencode

logging.basicConfig()
_logger = logging.getLogger('app.geo.transformer')


def get_osm_id(osm_id_raw, with_type):
    """Standardize the notations for types
    """
    if not isinstance(osm_id_raw, str):
        try:
            osm_id_raw = str(osm_id_raw)
        except Exception as ee:
            logging.error(
                f'Input osm id is not str and can not be converted {osm_id_raw}'
                )
            return None

    if with_type: # If we are gonna append the type to the osm id
        if '/' in osm_id_raw:
            osm_id = osm_id_raw
        else:
            osm_id_type = ''
            if osm_id_raw.startswith('n'):
                osm_id_type = 'node'
            elif osm_id_raw.startswith('w'):
                osm_id_type = 'way'
            elif osm_id_raw.startswith('r'):
                osm_id_type = 'relation'

            osm_id = re.findall(r'\d+', osm_id_raw)
            if osm_id:
                osm_id = osm_id[-1]
                osm_id = osm_id_type + '/' + osm_id
            else:
                logging.warning(
                    'Could not extract and convert id of {}'.format(
                        osm_id_raw
                        ))
                osm_id = osm_id_raw
    else: # If we do not append osm type to osm id
        if '/' in osm_id_raw:
            osm_id = osm_id_raw.split('/')[-1]
        else:
            osm_id = re.findall(r'\d+', osm_id_raw)
            if osm_id:
                osm_id = osm_id[-1]
            else:
                logging.warning(
                    'Could not extract id from'.format(
                        osm_id_raw
                        ))
                osm_id = osm_id_raw

    return osm_id


class OSMStreetTransformations(object):
    def __init__(
        self,
        localisation_client=None,
        city_mappings=None,
        country_mappings=None
        ):

        if localisation_client:
            self._localisation_client = localisation_client
        if city_mappings:
            self._city_mappings = city_mappings
        if country_mappings:
            self._country_mappings = country_mappings

    @staticmethod
    def id(osm, with_type=None):
        """Extract id from osm data and standardize it.
        OSM object id is stored differently in geojson files depends on the
        convertors.
        Here we abide to the convention of the form 'node/1234'
        """
        if with_type is None:
            with_type = True

        osm_id_raw = osm.get('id')

        osm_id = get_osm_id(osm_id_raw, with_type)

        return osm_id

    @staticmethod
    def name(osm):
        osm_prop = osm.get('properties',{})
        return osm_prop.get('name')

    @staticmethod
    def official_name(osm):
        osm_prop = osm.get('properties',{})
        return osm_prop.get('official_name')

    @staticmethod
    def city(osm):
        osm_prop = osm.get('properties',{})
        return osm_prop.get('addr:city')

    @staticmethod
    def country(osm):
        osm_prop = osm.get('properties',{})
        return osm_prop.get('addr:country')

    @staticmethod
    def types__highway(osm):
        osm_prop = osm.get('properties',{})
        return osm_prop.get('highway')

    @staticmethod
    def geometry(osm):
        osm_geometry = str(
            osm.get('geometry')
        )
        if osm_geometry:
            return osm_geometry
        else:
            return None


def transform_record(schema_at_level,
                     path,
                     dict_inp,
                     result,
                     available_transformers,
                     transformations):
    """Transforms one record according to the input schema
    """

    for field in schema_at_level['fields']:
        key = field['name']
        key_path_array = path + [key]
        key_path_str = '__'.join(key_path_array)
        if key_path_str in available_transformers:
            transformed = getattr(transformations, key_path_str)(dict_inp)
            _insert_to_dict_at_level(result, key_path_array, transformed)
        else:
            if field['type'] == 'RECORD':
                transform_record(field, key_path_array, dict_inp, result, available_transformers, transformations)
            else:
                _logger.error('Could not transform data')

def is_useful_osm_record(dic_inp):
    """Tell if the OSM data record is really useful
    """

    is_useful = True
    if (not dic_inp.get('properties', {}).get('name') ) and (not dic_inp.get('properties', {}).get('official_name') ):
        is_useful = False

    return is_useful


def clean_up_geojson(json_inp, load_only_key = None):
    """Convert json file to line delimited format

    #TODO This is not the best way to deal with large json file.
    """
    today_is = datetime.date.today()
    json_out_temp = '/'.join(
        json_inp.split('/')[:-1]
        ) + "/poi-line-delimited-{}.json".format(
        today_is
        )
    try:
        with open(json_out_temp, "w+") as fp:
            with open(json_inp, "r", encoding='utf-8') as json_inp_fp:
                json_inp_dict = json.load(json_inp_fp)
                if load_only_key and json_inp_dict.get(load_only_key):
                    for record in json_inp_dict.get(load_only_key):
                        if is_useful_osm_record(record):
                            fp.write(json.dumps(record)+'\n')
                else:
                    for record in json_inp_dict:
                        if is_useful_osm_record(record):
                            fp.write(json.dumps(record)+'\n')
    except Exception as ee:
        raise Exception("Can not convert json file to line delimited!")

    try:
        copyfile(json_inp, json_inp + ".{}".format("bak") )
    except:
        raise Exception('could not replace the geojson {} with line-delimited file {}'.format(
            json_inp,
            json_out_temp
            ))

    try:
        copyfile(json_out_temp, json_inp)
    except:
        raise Exception('could not replace the geojson {} with line-delimited file {}'.format(
            json_inp,
            json_out_temp
            ))


def transform_and_enhance_record(
    dict_inp,
    schema,
    available_transformers,
    transformations,
    observation_date,
    enhancement=None
    ):
    """Function takes a single OSM data record and transforms it into a dict
    :param dict dict_inp: One dictionary of the data to be transformed
    :param list schema: A list of schema objects, building on the field schema that is used to create a BigQuery table
    :param list available_transformers: list of strings, representing the names of the transformation functions
    :param OSMTransformation transformations: Class to combine all the transformation functions, includes mapping, cleaning, and general transformations
    :return dict: returns the transformed record with partition field included
    """

    assert type(datetime.datetime.strptime(observation_date, "%Y-%m-%d")) == datetime.datetime, 'observation_date miss specified'

    res = {}
    transform_record({'fields': schema}, [], dict_inp, res, available_transformers, transformations)
    res['observation_date'] = observation_date

    if enhancement:
        res = {**res, **enhancement}

    return res


def transform_records_and_save_to_file(
    schema,
    available_transformers,
    transformations,
    observation_date,
    input_file,
    output_file
    ):
    """
    :param list schema: a list of schema objects, building on the field schema that is used to create a BigQuery table
    :param transformations: Class to combine all the transformation functions, includes
                                                    mapping, cleaning, and general transformations
    :param list available_transformers: list of strings, representing the names of the transformation functions
    :param str folder: folder, where the result file shall be stored
    :param str date.isoformat() observation_date: target partition, in which to upload the data. Typically used, when last day
                                          did not run properly
    :param str input_file: the filename of the input file
    :param str output_file: to filename of the transformed file
    """

    if os.path.isfile(output_file):
        try:
            os.remove(output_file)
            print('deleted old version of transformed records')
        except:
            print('Could not delete file: {}'.format(output_file) )
            pass
    else:
        print('Output target {} does not exist. Will create this file.'.format(
            output_file
            ) )

    with open(output_file, 'w+') as output_file_transformed:
        with open(input_file, 'r') as input_file:
            for line in input_file:
                try:
                    row = json.loads(line)
                except:
                    raise Exception('could not load line to json. line = {}'.format(line) )

                try:
                    #transform the request
                    transformed_req = transform_and_enhance_record(
                        dict_inp = row,
                        schema = schema,
                        available_transformers = available_transformers,
                        transformations = transformations,
                        observation_date = observation_date
                        )
                except Exception as ee:
                    print(ee)
                    print('could not transform the record:\n {}'.format( row ))
                    traceback.print_exc()

                try:
                    output_file_transformed.write(
                        json.dumps(
                            transformed_req,
                            ignore_nan = True,
                            default=_isoencode
                            ) + '\n')
                except:
                    print('could not write the transformed record:\n {}'.format(row))

    print('wrote json file into: {}'.format(output_file))



if __name__ == "__main__":

    # one_record = {'type': 'Feature',
    #         'id': 'node/21385751',
    #         'properties': {'timestamp': '2018-06-16T15:28:45Z',
    #         'version': '20',
    #         'user': '',
    #         'uid': '0',
    #         'contact:phone': '+49 30 297 43333',
    #         'contact:website': 'http://www.s-bahn-berlin.de/fahrplanundnetz/bahnhof/storkower-strasse/142',
    #         'light_rail': 'yes',
    #         'name': 'S Storkower Straße',
    #         'network': 'Verkehrsverbund Berlin-Brandenburg',
    #         'network:short': 'VBB',
    #         'note': 'mangels Weichen nur ein Haltepunkt',
    #         'official_name': 'Berlin Storkower Straße',
    #         'old_name': 'Zentralviehhof',
    #         'operator': 'DB Netz AG',
    #         'public_transport': 'station',
    #         'railway': 'halt',
    #         'railway:ref': 'BSTO',
    #         'railway:station_category': '4',
    #         'station': 'light_rail',
    #         'uic_name': 'Berlin Storkower Str',
    #         'uic_ref': '8089041',
    #         'wheelchair': 'yes',
    #         'wikidata': 'Q800512',
    #         'wikipedia': 'de:Bahnhof Berlin Storkower Straße',
    #         'id': 'node/21385751'},
    #         'geometry': {'type': 'Point', 'coordinates': [13.4648885, 52.5237432]}}

    # osm_tran = OSMTransformations()

    # available_osm_transformers = [x for x in dir(osm_tran) if not x.startswith('_')]

    # with open(os.path.join(os.path.dirname(_schema.__file__), 'poi_schema.json'), 'rb') as source_file:
    #     schema = json.load(source_file)

    # res = {}
    # transform_record(
    #     {'fields': schema}, [], one_record, res, available_osm_transformers, osm_tran)

    # print(
    #     osm_tran.id(one_record)
    # )

    # print(
    #     res
    # )

    osm_tran = OSMStreetTransformations()
    available_osm_transformers = [x for x in dir(osm_tran) if not x.startswith('_')]
    with open(os.path.join(os.path.dirname(_schema.__file__), 'street_schema.json'), 'rb') as source_file:
        schema = json.load(source_file)

    transform_records_and_save_to_file(
        schema=schema,
        available_transformers=available_osm_transformers,
        transformations=osm_tran,
        observation_date='2019-05-29',
        input_file='/tmp/gb/greater-london-latest-highway.geojson',
        output_file='/tmp/gb/greater-london-latest-highway-transformed.json'
    )


    print('END')
