import os
import requests
import getpass

def get_download_link(links, esipfed_link ="http://esipfed.org/ns/fedsearch/1.1/data#"):
    """

    :param links:
    :type links:
    :return:
    :rtype:
    """
    return [ele for ele in links if ele['rel'] == esipfed_link and not ele.get('inherited')]


def get_cmr_file(urls, destination="/tmp/"):
    """

    :param urls:
    :type urls:
    :param destination:
    :type destination:
    :return:
    :rtype:
    """
    username = os.environ.get('URS_USERNAME') if os.environ.get('URS_USERNAME') else input ("Enter URS username :")
    password = os.environ.get('URS_PASSWORD') if os.environ.get('URS_PASSWORD') else getpass.getpass(prompt="Enter URS password :")
    os.makedirs(destination) if not os.path.exists(destination) else ""
    number_of_granules = len(urls)
    errors = []
    with requests.Session() as s:
        s.auth = (username, password)
        for url in urls:
            filename = os.path.basename(url)
            r = s.get(s.get(url).url, auth=(username, password), stream=True)

            print(f"Downloading {filename} to {destination}")
            if not r.ok:
                print(f"Error downloading: {r.reason}")
                errors.append(f"Error downloading: {r.reason}")
                number_of_granules -= 1
            file_destination = f"{destination.rstrip('/')}/{filename}"

            with open(file_destination, 'wb') as fd:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    fd.write(chunk)
            print(f"Successfully downloaded {filename}")
        return {
            "status": f"Downloaded {len(urls)} out of {number_of_granules}",
            "path": destination,
            "errors": errors
        }