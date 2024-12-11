from chrisclient import client


class PACSClient(object):
    def __init__(self, url: str, username: str, password: str):
        self.cl = client.Client(url, username, password)
        self.cl.pacs_series_url = f"{url}pacs/series/"

    def get_pacs_registered(self, params: dict):
        """
        Get the list of PACS series registered to _this_
        CUBE instance
        """
        response = self.cl.get_pacs_series_list(params)
        if response:
            return response['total']
        raise Exception(f"No PACS details with matching search criteria {params}")
