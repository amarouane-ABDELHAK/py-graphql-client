from BaseClassClient import BaseClassClient
import CMRHelpers

class Service(BaseClassClient):
    """
    Query for a Service using GraphQL
    """
    def __init__(self, fields,conceptId, service="service"):
        self.conceptId = conceptId
        super().__init__(service=service,fields=fields,conceptId=self.conceptId)
    
    def download_granules(self, destination="/tmp"):
        """
        Download the queried granules
        :return:
        :rtype:
        """

        results = self.execute_query()
        coll_items = results['service']['collections']['items']         
        for coll_item in coll_items:
            for item in coll_item['granules']['items']:
                links = item['links']
                hrefs = CMRHelpers.get_download_link(links)
                urls = [href['href'] for href in hrefs]
                CMRHelpers.get_cmr_file(urls=urls, destination=destination) 






if __name__ == "__main__":
    from Collections import Collections
    from Granules import Granules
    service = Service(fields=["description","longName"], conceptId="S1993846778-SCIOPS")
    collections = Collections(fields=["shortName"]) 
    granules = Granules(fields=["granuleUr", "links"], limit=1)
    collections.append_service(granules)
    service.append_service(collections)
    # print(service.get_query())
    #print(service.execute_query(pretty=True))
    service.download_granules()