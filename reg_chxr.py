#!/usr/bin/env python

from pathlib import Path
from argparse import ArgumentParser, Namespace, ArgumentDefaultsHelpFormatter
from chris_pacs_service import PACSClient
from loguru import logger
from chris_plugin import chris_plugin, PathMapper
from chrisClient import ChrisClient
import time
import json
import copy
import sys

LOG = logger.debug

logger_format = (
    "<green>{time:YYYY-MM-DD HH:mm:ss}</green> │ "
    "<level>{level: <5}</level> │ "
    "<yellow>{name: >28}</yellow>::"
    "<cyan>{function: <30}</cyan> @"
    "<cyan>{line: <4}</cyan> ║ "
    "<level>{message}</level>"
)
logger.remove()
logger.add(sys.stderr, format=logger_format)

__version__ = '1.0.6'

DISPLAY_TITLE = r"""
       _                               _               
      | |                             | |              
 _ __ | |______ _ __ ___  __ _     ___| |__ __  ___ __ 
| '_ \| |______| '__/ _ \/ _` |   / __| '_ \\ \/ / '__|
| |_) | |      | | |  __/ (_| |  | (__| | | |>  <| |   
| .__/|_|      |_|  \___|\__, |   \___|_| |_/_/\_\_|   
| |                       __/ |_____                   
|_|                      |___/______|                  
"""


parser = ArgumentParser(description='A plugin to wait till a particular set of PACS files are registered to a CUBE instance',
                        formatter_class=ArgumentDefaultsHelpFormatter)
parser.add_argument(
    "--CUBEurl",
    default="http://localhost:8000/api/v1/",
    help="CUBE URL"
)
parser.add_argument(
    "--pluginInstanceID",
    default="",
    help="plugin instance ID from which to start analysis",
)
parser.add_argument(
    "--CUBEuser",
    default="chris",
    help="CUBE/ChRIS username"
)
parser.add_argument(
    "--CUBEpassword",
    default="chris1234",
    help="CUBE/ChRIS password"
)
parser.add_argument(
    '--inputJSONfile',
    default='',
    type=str,
    help='JSON file containing DICOM data to be retrieved'
)
parser.add_argument(
    '--tagStruct',
    default='',
    type=str,
    help='directive to use to anonymize DICOMs'
)
parser.add_argument(
    '--orthancUrl', '-o',
    dest='orthancUrl',
    type=str,
    help='Orthanc server url',
    default='http://0.0.0.0:8042'
)

parser.add_argument(
    '--orthancUsername', '-u',
    dest='username',
    type=str,
    help='Orthanc server username',
    default='orthanc'
)

parser.add_argument(
    '--orthancPassword', '-p',
    dest='password',
    type=str,
    help='Orthanc server password',
    default='orthanc'
)

parser.add_argument(
    '--pushToRemote', '-r',
    dest='pushToRemote',
    type=str,
    help='Remote modality',
    default=''
)
parser.add_argument('-V', '--version', action='version',
                    version=f'%(prog)s {__version__}')


# The main function of this *ChRIS* plugin is denoted by this ``@chris_plugin`` "decorator."
# Some metadata about the plugin is specified here. There is more metadata specified in setup.py.
#
# documentation: https://fnndsc.github.io/chris_plugin/chris_plugin.html#chris_plugin
@chris_plugin(
    parser=parser,
    title='A ChRIS plugin to verify PACS file registration in CUBE',
    category='',                 # ref. https://chrisstore.co/plugins
    min_memory_limit='100Mi',    # supported units: Mi, Gi
    min_cpu_limit='1000m',       # millicores, e.g. "1000m" = 1 CPU core
    min_gpu_limit=0              # set min_gpu_limit=1 to enable GPU
)
def main(options: Namespace, inputdir: Path, outputdir: Path):
    """
    *ChRIS* plugins usually have two positional arguments: an **input directory** containing
    input files and an **output directory** where to write output files. Command-line arguments
    are passed to this main method implicitly when ``main()`` is called below without parameters.

    :param options: non-positional arguments parsed by the parser given to @chris_plugin
    :param inputdir: directory containing (read-only) input files
    :param outputdir: directory where to write output files
    """

    print(DISPLAY_TITLE)

    # Typically it's easier to think of programs as operating on individual files
    # rather than directories. The helper functions provided by a ``PathMapper``
    # object make it easy to discover input files and write to output files inside
    # the given paths.
    #
    # Refer to the documentation for more options, examples, and advanced uses e.g.
    # adding a progress bar and parallelism.
    if not health_check(options): return

    cube_cl = PACSClient(options.CUBEurl, options.CUBEuser, options.CUBEpassword)
    mapper = PathMapper.file_mapper(inputdir, outputdir, glob=options.inputJSONfile)
    for input_file, output_file in mapper:

        # Open and read the JSON file
        with open(input_file, 'r') as file:
            data = json.load(file)

            # null check
            if len(data) == 0:
                raise Exception(f"Cannot verify registration for empty pacs data.")

            # for each individual series, check if total file count matches total file registered
            for series in data:
                pacs_search_params = sanitize_for_cube(series)
                file_count = int(series["NumberOfSeriesRelatedInstances"])
                registered_file_count = cube_cl.get_pacs_registered(pacs_search_params)

                # poll CUBE at regular interval for the status of file registration
                poll_count = 0
                total_polls = 10
                wait_poll = 2
                while registered_file_count < 1 and poll_count <= total_polls:
                    poll_count += 1
                    time.sleep(wait_poll)
                    registered_file_count = cube_cl.get_pacs_registered(pacs_search_params)
                    LOG(f"{registered_file_count} series found in CUBE.")

                # check if polling timed out before registration is finished
                if registered_file_count == 0:
                    raise Exception(f"PACS file registration unsuccessful. Please try again.")
                LOG(f"{file_count} files successfully registered to CUBE.")
                send_params = {
                    "url": options.orthancUrl,
                    "username": options.username,
                    "password": options.password,
                    "aec": options.pushToRemote
                }
                dicom_dir = cube_cl.get_pacs_files(pacs_search_params)

                # create connection object
                cube_con = ChrisClient(options.CUBEurl, options.CUBEuser, options.CUBEpassword)
                cube_con.anonymize(dicom_dir, options.tagStruct,send_params, options.pluginInstanceID)


def sanitize_for_cube(series: dict) -> dict:
    """
    TBD
    """
    params = {}
    params["SeriesInstanceUID"] = series["SeriesInstanceUID"]
    params["StudyInstanceUID"] = series["StudyInstanceUID"]
    return params

def health_check(options) -> bool:
    """
    check if connections to pfdcm, orthanc, and CUBE is valid
    """
    try:
        if not options.pluginInstanceID:
            options.pluginInstanceID = os.environ['CHRIS_PREV_PLG_INST_ID']
    except Exception as ex:
        LOG(ex)
        return False
    try:
        # create connection object
        cube_con = ChrisClient(options.CUBEurl, options.CUBEuser, options.CUBEpassword)
        cube_con.health_check()
    except Exception as ex:
        LOG(ex)
        return False
    return True


if __name__ == '__main__':
    main()
