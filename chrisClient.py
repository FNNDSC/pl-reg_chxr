### Python Chris Client Implementation ###

from base_client import BaseClient
from chrisclient import client
from chris_pacs_service import PACSClient
import json
import time
from loguru import logger
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

class ChrisClient(BaseClient):
    def __init__(self, url: str, username: str, password: str):
        self.cl = client.Client(url, username, password)
        self.cl.pacs_series_url = f"{url}pacs/series/"
        self.req = PACSClient(self.cl.pacs_series_url,username,password)

    def create_con(self,params:dict):
        return self.cl

    def health_check(self):
        return self.cl.get_chris_instance()

    def pacs_pull(self):
        pass
    def pacs_push(self):
        pass

    def anonymize(self, dicom_dir: str, tag_struct: str, pv_id: int):

        pl_id = self.__get_plugin_id({"name": "pl-dsdircopy", "version": "1.0.2"})
        pl_sub_id = self.__get_plugin_id({"name": "pl-pfdicom_tagsub", "version": "3.3.4"})
        pl_dcm_id = self.__get_plugin_id({"name": "pl-orthanc_push", "version": "1.2.7"})

        # 1) Run dircopy
        # empty directory check
        if len(dicom_dir) == 0:
            LOG(f"No directory found in CUBE containing files for search : {tag_struct}")
            return
        pv_in_id = self.__create_feed(pl_id, {"previous_id": pv_id, 'dir': dicom_dir})

        # 2) Run anonymization
        data = {"previous_id": pv_in_id, "tagStruct": tag_struct, 'fileFilter': '.dcm'}
        tag_sub_id = self.__create_feed(pl_sub_id, data)

        # 3) Push results
        dir_send_data = {
            "previous_id": tag_sub_id,
            'inputFileFilter': "**/*dcm",
            "orthancUrl": params["push"]["url"],
            "username":params["push"]["username"],
            "password": params["push"]["password"],
            "pushToRemote": params["push"]["aec"]
        }
        pl_inst_id = self.__create_feed(pl_dcm_id, dir_send_data)

    def __create_feed(self, plugin_id: str,params: dict):
        response = self.cl.create_plugin_instance(plugin_id, params)
        return response['id']

    def __get_plugin_id(self, params: dict):
        response = self.cl.get_plugins(params)
        if response['total'] > 0:
            return response['data'][0]['id']
        raise Exception(f"No plugin found with matching search criteria {params}")



