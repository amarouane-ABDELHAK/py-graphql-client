from .BaseClassClient import BaseClassClient
from .CMRHelpers import get_download_link, get_cmr_file

class Granule(BaseClassClient):
    """
    Query for a granule using GraphQL
    """
    def __init__(self, fields,service="granule", **kwargs):
        super().__init__(service=service,fields=fields, **kwargs)

    def download_granule(self, destination="/tmp"):
        """
        Download the queried granules
        :return:
        :rtype:
        """
        self.query += "links\n"

        a = self.execute_query()
        links = a['granule']['links']
        hrefs = get_download_link(links)
        urls = [href['href'] for href in hrefs]
        get_cmr_file(urls=urls, destination=destination)
