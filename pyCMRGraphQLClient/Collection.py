from BaseClassClient import BaseClassClient


class Collection(BaseClassClient):
    """
    Query for a colletion using GraphQL
    """
    def __init__(self, service="collection", **kwargs):
        super().__init__(service=service, **kwargs)
