from Granule import Granule
from BaseClassClient import BaseClassClient


class Granules(Granule):
    """
    Query for granules using GraphQL
    """
    def __init__(self, service="granules", **kwargs):

        fields = kwargs.pop('fields', [])
        granules_fields = []
        for ele in ['count', 'cursor']:
            if ele in fields:
                granules_fields.append(ele)
                fields.remove(ele)

        super().__init__(service=service,fields=granules_fields, **kwargs)
        items = BaseClassClient(service="items", fields=fields)
        self.append_service(items)

    def download_granule(self, destination="/tmp"):
        """
        Download the queried granules
        :return:
        :rtype:
        """
        items = BaseClassClient(service="items", fields=['links'])
        self.append_service(items)
        result = self.execute_query()
        items = result['granules']['items']
        for item in items:
            links = item['links']
            hrefs = self.get_download_link(links)
            urls = [href['href'] for href in hrefs]
            self.get_cmr_file(urls=urls, destination=destination)

        