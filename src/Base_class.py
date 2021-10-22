from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
from json import dumps


class CMRGraphQLBase:

    def __init__(self, service, fields, **kwargs) -> None:
        params = []
        for k, v in kwargs.items():
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

    def get_query(self):
        return """
        %s
        }
        """ % (self.query)

    def append_service(self, service):
        self.query += service.get_query()
        

    def generate_query(self):
        return """
        {
            %s
        }
        """ % (self.get_query())

    def execute_query(self, pretty=False):
        """
        :param pretty: Return a pretty json
        :type pretty: Boolean
        """
        gql_query = self.generate_query()
        graphql_host = getenv('GraphQL_Host', 'https://graphql.earthdata.nasa.gov/api')
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