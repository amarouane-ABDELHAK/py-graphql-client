from BaseClassClient import BaseClassClient

class Services(BaseClassClient):
    """
    Query for a collections using GraphQL
    """
    def __init__(self, service="services", **kwargs):
        services_fields, fields, kwargs = self.sanitize_fields(list_of_items=["count", "cursor"], **kwargs)
        super().__init__(service=service,fields=services_fields, **kwargs)
        items = BaseClassClient(service="items", fields=fields)
        self.append_service(items)
