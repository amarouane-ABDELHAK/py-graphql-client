from .BaseClassClient import BaseClassClient
from .CMRHelpers import get_download_link, get_cmr_file

class Collections(BaseClassClient):
    """
    Query for a collections using GraphQL
    """
    def __init__(self, service="collections", **kwargs):
        collections_fields, fields, kwargs = self.sanitize_fields(list_of_items=["count", "cursor", "facets"], **kwargs)
        super().__init__(service=service,fields=collections_fields, **kwargs)
        self.items = BaseClassClient(service="items", fields=fields)
        self.query += self.items.get_query()
        self.kwargs = kwargs

    def append_service(self, subservice):
        self.items.query += subservice.get_query()
        self.query += self.items.get_query()



    def download_granules(self, destination="/tmp"):
        """
        Download the queried granules
        :return:
        :rtype:
        """

        results = self.execute_query()
        coll_items = results['collections']['items']

        for coll_item in coll_items:
            for item in coll_item['granules']['items']:
                links = item['links']
                hrefs = get_download_link(links)
                urls = [href['href'] for href in hrefs]
                get_cmr_file(urls=urls, destination=destination)




if __name__ == "__main__":
    from Granules import Granules
    collections = Collections(provider="GHRC_DAAC", limit=1, fields=["count", "shortName", "conceptId"])
    granules = Granules(fields=["granuleUr", "links"], limit=1)
    collections.append_service(granules)
    print(collections.execute_query(pretty=True))
    collections.download_granules()
