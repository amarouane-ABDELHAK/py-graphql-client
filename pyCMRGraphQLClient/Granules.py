from Granule import Granule
from BaseClassClient import BaseClassClient


class Granules(Granule):
    """
    Query for granules using GraphQL
    """
    def __init__(self, service="granules", **kwargs):

        count = kwargs.pop("count", "")
        cursor = kwargs.pop("cursor", "")

        kwargs['fields'] = [count, cursor]
        super().__init__(service=service, **kwargs)
        items = BaseClassClient(service="items", fields=[])
        self.append_service(items)

