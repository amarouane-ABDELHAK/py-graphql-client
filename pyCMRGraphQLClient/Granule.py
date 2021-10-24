from BaseClassClient import BaseClassClient
import CMRHelpers

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
        hrefs = CMRHelpers.get_download_link(links)
        urls = [href['href'] for href in hrefs]
        print(urls)
        CMRHelpers.get_cmr_file(urls=urls, destination=destination)
