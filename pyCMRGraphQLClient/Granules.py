from .Granule import Granule
from .BaseClassClient import BaseClassClient
from .CMRHelpers import get_download_link, get_cmr_file, get_urs_username_password

class Granules(Granule):
    """
    Query for granules using GraphQL
    """
    def __init__(self, service="granules", **kwargs):

        granules_fields, fields, kwargs = self.sanitize_fields(list_of_items=["count", "cursor"], **kwargs)
        super().__init__(service=service,fields=granules_fields, **kwargs)
        items = BaseClassClient(service="items", fields=fields)
        self.append_service(items)

    def download_granules(self, destination="/tmp"):
        """
        Download the queried granules
        :return:
        :rtype:
        """
        items = BaseClassClient(service="items", fields=['links'])
        self.append_service(items)
        result = self.execute_query()
        items = result['granules']['items']
        username, password = get_urs_username_password()
        for item in items:
            links = item['links']
            hrefs = get_download_link(links)
            urls = [href['href'] for href in hrefs]
            get_cmr_file(urls=urls,username=username, password=password, destination=destination)
