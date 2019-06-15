import subprocess
from subprocess import check_output, STDOUT, CalledProcessError

import urllib.request
import logging
import os
from shutil import which as _which


def data_downloader(data_source_url, target_file):
    """Download data from remote
    """
    download_success = False

    try:
        target_file_dir = "/".join(target_file.split("/")[:-1])
        os.makedirs(target_file_dir, exist_ok=True)
        logging.info("Created folders: {}".format(target_file_dir) )
    except Exception as ee:
        raise Exception("Could not create folders to store the data: {}".format(
            target_file_dir
        ))

    try:
        res = urllib.request.urlretrieve(data_source_url, target_file)
        download_success = True
    except Exception as ee:
        if ee.code == 404:
            logging.exception('File does not exist: {} \n'.format(data_source_url) )
            raise Exception(ee)
        else:
            logging.exception('Can not download {}\n'.format(data_source_url), ee)

    return {
        "data_source": data_source_url,
        "data_file": target_file,
        "success": download_success
    }


def bash_command_exists(bash_command_name):
    """Check whether `name` is on PATH and marked as executable.
    """

    return _which(bash_command_name) is not None


def pbf2geojson(pbf_file, geojson_file):
    """Convert pbf data to geojson data
    """

    command_success = False

    if not bash_command_exists("osmium"):
        raise Exception("'osmium' is not detected" )

    osmium_call = [
        "osmium",
        "export",
        pbf_file,
        "-o",
        geojson_file,
        "-u",
        "type_id",
        "--overwrite",
        "--output-format",
        "geojson"
    ]

    try:
        print(' '.join(osmium_call) )
        with open(geojson_file, "w") as sp_log:
            with subprocess.Popen(osmium_call, stdout=sp_log, stderr=subprocess.PIPE) as sp:
                sp.wait()
                sp_stdout, sp_stderr = sp.communicate()

            sp_log.flush()

        if sp_stderr:
            logging.warning(sp_stderr)
        command_success = True
    except CalledProcessError as ee:
        #logging.exception("Could not execute command! stderr:\n{}".format(sp_stderr) )
        raise Exception(ee)


    return {
        "pbf_file": pbf_file,
        "geojson_file": geojson_file,
        "success": command_success
    }


def pbf_filter(pbf_file, filtered_pbf_file, filter_params, bounding_box=None):
    """Convert pbf data to geojson data
    """

    if filter_params is None:
        filter_params = []

    command_success = False

    if not bash_command_exists("osmium"):
        raise Exception("'osmium' is not detected" )

    if bounding_box is None:
        osmium_call_bounding = []
        osmium_call = [
            "osmium",
            "tags-filter",
            "-o",
            filtered_pbf_file,
            pbf_file
        ] + filter_params + ["--overwrite"]
    else:
        # bounded_pbf_file_sufix = '.bounded.osm.pbf'
        bounded_pbf_file = pbf_file.split('/')
        bounded_pbf_file[-1] = 'bounding_' + bounded_pbf_file[-1]
        bounded_pbf_file = '/'.join(bounded_pbf_file)
        osmium_call_bounding = [
            "osmium",
            "extract",
            "-b",
            f"{bounding_box}",
            f"{pbf_file}",
            "-o",
            f"{bounded_pbf_file}",
            "--overwrite"
        ]
        osmium_call = [
            "osmium",
            "tags-filter",
            "-o",
            filtered_pbf_file,
            bounded_pbf_file
        ] + filter_params + ["--overwrite"]

    try:
        if osmium_call_bounding:
            print(' '.join(osmium_call_bounding) )
            logging.debug('Applying bounding box to pbf data')
            with open(filtered_pbf_file+'.log', "w") as sp_log:
                with subprocess.Popen(osmium_call_bounding, stdout=sp_log, stderr=subprocess.PIPE) as sp:
                    sp.wait()
                    sp_stdout, sp_stderr = sp.communicate()

                sp_log.flush()

        print(' '.join(osmium_call) )
        with open(filtered_pbf_file+'.log', "w+") as sp_log:
            with subprocess.Popen(osmium_call, stdout=sp_log, stderr=subprocess.PIPE) as sp:
                sp.wait()
                sp_stdout, sp_stderr = sp.communicate()

            sp_log.flush()

        if sp_stderr:
            logging.warning(sp_stderr)
        command_success = True
    except CalledProcessError as ee:
        #logging.exception("Could not execute command! stderr:\n{}".format(sp_stderr) )
        raise Exception(ee)

    return {
        "pbf_file": pbf_file,
        "filtered_pbf_file": pbf_file,
        "success": command_success
    }


if __name__ == "__main__":
    print('END OF GAME')



