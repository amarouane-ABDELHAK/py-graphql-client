from BaseClassClient import BaseClassClient


class Collections(BaseClassClient):
    """
    Query for a collections using GraphQL
    """
    def __init__(self, service="collections", **kwargs):
        super().__init__(service=service, **kwargs)
