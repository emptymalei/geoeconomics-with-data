import datetime
import logging
import os
from time import sleep as _sleep

import numpy as np
import pandas as pd
import pytz

from shapely.ops import transform

logging.basicConfig()
_logger = logging.getLogger('app.geo.util')


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
