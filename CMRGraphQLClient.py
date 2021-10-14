from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
from dotenv import load_dotenv
from os import getenv, path
import requests
from json import dumps

load_dotenv()


class GenerateExecuteQuery:

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
        graphql_host = getenv('GraphQL_Host', 'http://localhost:9100/graphql')
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

    @staticmethod
    def get_cmr_file(url, username, password, destination):
        filename = path.basename(url)
        with requests.Session() as s:
            s.auth = (username, password)
            r = s.get(s.get(url).url, auth=(username, password), stream=True)

            if not r.ok:
                print(f"Error downloading: {r.reason}")
                return {
                    "success": False,
                    "path": "",
                    "error": r.reason
                }
            file_destination = f"{destination.rstrip('/')}/{filename}"
            with open(file_destination, 'wb') as fd:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    fd.write(chunk)
            return {
                "success": True,
                "path": file_destination,
                "error": ""
            }

    def download_granules(self, dest_path):
        """
        Download the queried granules
        :return:
        :rtype:
        """
        result_returned = self.execute_query()

        granules = result_returned.get('collection').get('granules')
        username, password = getenv('USERNAME'), getenv('PASSWORD')
        for gran in granules:
            granule_link = gran['download_link']
            print(f'Downloading {path.basename(granule_link)} to {dest_path}')
            self.get_cmr_file(granule_link, username=username, password=password,
                              destination=dest_path)


if __name__ == "__main__":
    collection = GenerateExecuteQuery("collection", conceptId="C1976712047-GHRC_DAAC",
                                      fields=["processingLevel", "shortName", "spatialExtent"])
    # granule = GenerateExecuteQuery("granules", page_size="5", fields=["download_link"])
    # plt = GenerateExecuteQuery("Platforms", fields=["ShortName"])
    # collection.append_service(granule)
    # collection.append_service(plt)
    query_result = collection.execute_query(pretty=True)
    print(query_result)
    #collection.download_granules(dest_path="/tmp/dest")
