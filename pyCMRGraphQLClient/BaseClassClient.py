from abc import ABC
from typing import List
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
from dotenv import load_dotenv
from os import getenv
import requests
from json import dumps

load_dotenv()


class BaseClassClient(ABC):
    """
    Base class for pyCMRGraphQL client
    """

    def __init__(self, service: str, fields: List[str], **kwargs) -> None:
        """
        Initiate the client
        :param service: Service as defined by GraphQL schema
        :type service: string
        :param fields: Fields to be returned
        :type fields: list of strings
        :param kwargs: arbitrary parameters to query GraphQL
        :type kwargs:
        """
        params = []
        for k, v in kwargs.items():
            if isinstance(v, bool):
                v = str(v).lower()
            try:
                v = int(v)
            except:
                v = f'"{v}"'
            params.append(f'{k}: {v}')
        passed_params = f"({','.join(params)})" if params else ""
        self.query = """
        %s%s{
        %s
        """ % (service, passed_params, '\n'.join(fields))
    
    def sanitize_fields(self,list_of_items=[], **kwargs):
        """.env.example"""
        fields = kwargs.pop('fields', [])
        service_fields = []
        for ele in list_of_items:
            if ele in fields:
                service_fields.append(ele)
                fields.remove(ele)
        return [service_fields, fields, kwargs]

    def get_query(self) -> str:
        """
        A helper function to see your GQL query
        :return: GQL query
        :rtype: string
        """
        return """
        %s
        }
        """ % self.query

    def append_service(self, service):
        """
        Add service query
        :param service: the service initiated by this class
        :type service: Class BaseClassClient
        :return: None
        :rtype: None
        """
        
        self.query += service.get_query()

    def generate_query(self):
        """
        Generate GQL query
        :return:
        :rtype:
        """
        return """
        {
            %s
        }
        """ % (self.get_query())

    def execute_query(self, graphql_host_url="https://graphql.earthdata.nasa.gov/api", pretty=False):
        """
        Execute the query
        :param graphql_host_url: GraphQL host URL
        :type graphql_host_url: str
        :param pretty: Return a pretty json
        :type pretty: Boolean
        """
        gql_query = self.generate_query()
        graphql_host = getenv('GraphQL_Host', graphql_host_url)
        # Select your transport with a defined url endpoint
        transport = RequestsHTTPTransport(
            url=graphql_host,
            headers={
                "Content-type": "application/json",
            },
            verify=False,
            retries=3,
        )
        # Create a GraphQL client using the defined transport
        client = Client(transport=transport, fetch_schema_from_transport=True)
        # Provide a GraphQL query
        query_ex = gql(gql_query)
        # Execute the query on the transport
        result = client.execute(query_ex)
        result = dumps(result, indent=2, sort_keys=True) if pretty else result
        return result
