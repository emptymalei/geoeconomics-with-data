import datetime
import logging
import os
import json
from time import sleep as _sleep
import dateutil

import numpy as np
import pandas as pd
import pytz

from shapely.ops import transform

logging.basicConfig()
_logger = logging.getLogger('app.geo.util')


def load_records(data_path_inp):

    data = []

    with open(data_path_inp, 'r') as fp:
        for line in fp:
            line = line.replace('null', ' "None" ')
            try:
                line_data = json.loads(line.strip())
            except Exception as ee:
                logging.warning('could not load ', line, '\n', ee)
            data.append(line_data)

    return data

def save_records(data_inp, output, is_flush=None):

    if is_flush is None:
        is_flush = False

    if isinstance(data_inp, list):
        data = data_inp
    elif isinstance(data_inp, dict):
        data = [data_inp]
    else:
        raise Exception('Input data is neither list nor dict: {}'.format(data_inp))

    try:
        with open(output, 'a+') as fp:
            for i in data:
                json.dump(i, fp)
                fp.write('\n')
                if is_flush:
                    fp.flush()
    except Exception as ee:
        raise Exception('Could not load data to file: {}'.format(ee))


def check_and_convert_to_date(x):
    """
    Check input type and convert input to *date* object
    This function will handle input in the order of
    1. datetime.datetime: simply convert to date
    2. datetime.date: return itself
    3. str: convert to datetime then date
    :params x: input to be converted
    :returns: datetime.date object
    """
    if isinstance(x, datetime.datetime):
        return x.date()
    if isinstance(x, datetime.date):
        return x
    if isinstance(x, str):
        try:
            return datetime.datetime.strptime(x, '%Y-%m-%d').date()
        except ValueError as v:
            raise ValueError('Could not convert date - error: {}'.format(v))
    else:
        raise ValueError('Could not convert input {} to date'.format(x))


def check_and_convert_to_datetime(input_date, convert_to_utc=False):
    """
    Convert input to *datetime* object.
    This is the last effort of converting input to datetime.
    The order of instance check is
    1. datetime.datetime
    2. str
    3. float or int
    >>> handle_strange_dates(1531323212311)
    datetime(2018, 7, 11, 17, 33, 32, 311000)
    >>> handle_strange_dates(datetime(2085,1,1))
    datetime(2050, 1, 1)
    """

    if isinstance(input_date, datetime.datetime):
        if input_date.tzinfo is not None:
            if convert_to_utc:
                input_date = input_date.astimezone(tz=pytz.utc).replace(tzinfo=None)
            else:
                input_date = input_date.replace(tzinfo=None)
        cur_year = datetime.datetime.now().year
        if abs(input_date.year - cur_year) > 50:
            return datetime.datetime(2050, 1, 1)
        return input_date
    if isinstance(input_date, str):
        try:
            input_date = dateutil.parser.parse(input_date)
        except:
            return None

        if input_date.tzinfo is not None:
            if convert_to_utc:
                input_date = input_date.astimezone(tz=pytz.utc).replace(tzinfo=None)
            else:
                input_date = input_date.replace(tzinfo=None)
        cur_year = datetime.datetime.now().year
        if abs(input_date.year - cur_year) > 50:
            return datetime.datetime(2050, 1, 1)
        return input_date
    if isinstance(input_date, (float, int)):
        try:
            res = datetime.datetime.fromtimestamp(input_date / 1000)
        except:
            res = None
        return res


def split_dataframe(df_inp, chunk_size=None, chunks=None):
    """Split dataframe into chunks according to the chunk_size or number of chunks

    :param df_inp: DataFrame to be splitted
    :param chunk_size: the size of each chunk
    :param chunks: the number of chunks, this will override the settings from chunk_size
    """

    if chunks:
        chunk_size = int(df_inp.shape[0]/chunks)

    batch_df = [
            df_inp.iloc[df_inp.index[i:i + chunk_size]]
            for i in range(0, df_inp.shape[0], chunk_size)
            ]

    return batch_df


def file_exists(file_path):
    """Check if a file exists, if a file is found, the stats will be logged
    """

    is_file = False
    if os.path.isfile(file_path):
        is_file = True
        file_size = round(float(os.stat(file_path).st_size/float(1<<20)))
    else:
        file_size = None

    return is_file, file_size

def distance_between_geom(geom1, geom2, proj):
    """Calculate distance between two shapely objects
    """

    geom1_conv = transform( proj, geom1 )
    geom2_conv = transform( proj, geom2 )

    return geom1_conv.distance( geom2_conv )


def insert_to_dict_at_level(dictionary, dict_key_path, dict_value):
    """Insert values to dictioinary according to path specified
    """

    dictionary_nested_in = dictionary

    for key in dict_key_path[:-1]:
        if key not in dictionary_nested_in:
            dictionary_nested_in[key] = {}
        dictionary_nested_in = dictionary_nested_in[key]

    dictionary_nested_in[dict_key_path[-1]] = dict_value

    return dictionary

def get_dict_val_recursively(dictionary, names):
    """
    Get value of a dictionary according to specified path (names)
    :param dict dictionary: input dictionary
    :param names: path to the value to be obtained
    **Attention**: Function can't fail: will always return value or None.
    >>> get_val_recursively({1:{2:{'3':'hi'}}},[1,2])
    {'3': 'hi'}
    >>> get_val_recursively({1:{2:{3:'hi'}}},[1,'2',3])
    {'hi'}
    """
    if isinstance(names, list):
        tmp = names.copy()
    elif isinstance(names, str):
        tmp = [names].copy()
    else:
        raise ValueError('names must be str or list')
    if len(tmp) > 1:
        pop = tmp.pop(0)
        try:
            pop = int(pop)
        except ValueError:
            pass

        try:
            return get_dict_val_recursively(dictionary[pop], tmp)
        except:
            _logger.error('Could not get: '.format(pop))
            return None
    elif len(tmp) == 0:
        return None
    else:
        try:
            val = int(tmp[0])
        except:
            val = tmp[0]
        try:
            return dictionary[val]
        except KeyError:
            _logger.error('KeyError: Could not find {}'.format(tmp[0]))
            return None
        except TypeError:
            _logger.error('TypeError: Could not find {}'.format(tmp[0]))
            return None

def isoencode(obj):
    """
    used to decode JSON, handles datetime -> ISOFORMAT,
    and np.bool -> regular bool
    This function checks the following types in order
    * pandas._libs.tslibs.nattype.NaTType -> np.nan
      It is worth noticing that this is a private class.
    * datetime.datetime -> *.isoformat()
    * datetime.date -> *.isoformat()
    * np.ndarray -> *.tolist()
    * (np.int64, np.int32, np.int16, np.int) -> int(*)
    * (np.float64, np.float32, np.float16, np.float, float) -> None (if is np.nan) or float(*)
    * np.bool_ -> bool(*)
    There is a reason that we are checking if the input is float. np.nan is recoginized as float.
    However, we can not allow np.nan passed on and show up as NaN in the file.
    Another caveat is that json.dumps will never pass np.nan to any encoder.
    Thus we will not be able to encode np.nan using this function.
    The solution is to use simplejson instead of json.
    ```
    import simplejson as json
    json.dumps(blabla, ignore_nan=True, default=isoencode)
    ```
    """
    if isinstance(obj, pd._libs.tslibs.nattype.NaTType ):
        # TODO
        # This NaTType is a private class
        # This is considered as a temporary solution to the NaTType check
        return None
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    if isinstance(obj, datetime.date):
        return obj.isoformat()
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if isinstance(obj, (np.int64, np.int32, np.int16, np.int)):
        return int(obj)
    if isinstance(obj, (np.float64, np.float32, np.float16, np.float, float) ):
        if obj is not np.nan:
            return float(obj)
        else:
            return None
    if isinstance(obj, np.bool_):
        return bool(obj)
