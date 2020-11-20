import requests as request
from lxml import html
from datetime import datetime, timedelta
import google.cloud.storage as storage
import zipfile, os
try:
    from StringIO import StringIO ## for Python 2
except ImportError:
    from io import BytesIO ## for Python 3


def ingest_data(request):

    print("Building Paths!")
    downlaod_event_base_path = get_paths("events")

    print("Calculating Dates!")
    d1_date = get_retro_date(1, "%Y%m%d")

    # get list of links to download
    print("Getting list of links!")
    file_list = get_list_of_links(downlaod_event_base_path, d1_date)

    for file_name in file_list:
        print("Downloading files: {}!".format(file_name))
        download_link = downlaod_event_base_path + file_name
        zip_file = download_results(download_link)

        print("Extracting files!")
        zip = extract_zip_file(zip_file, "raw/")

        destination_path = "datalake/raw/{}".format(file_name.replace(".zip", ""))
        raw_file_path = "raw/{}".format(file_name.replace(".zip", ""))
        print("Uploading to GCS: {}!".format(raw_file_path))
        upload_blob("poc-bayer-gdelt", raw_file_path, destination_path)
        try:
            print("Removing tmp files!")
            os.remove(raw_file_path)
        except Exception as e:
            print("File does not Exists!")

def get_paths(context):
    if context == 'events':
        return "http://data.gdeltproject.org/events/"
    elif context == 'gkg':
        return "http://data.gdeltproject.org/gkg/"
    else:
        raise print("Invalid Context: valid contexts are: 'events' or 'gkg'")

def get_retro_date(ret, fmt):
    today = datetime.now()
    return (today - timedelta(days=ret)).strftime(fmt)

def get_list_of_links(url, date=None):
    page = request.get(url)
    doc = html.fromstring(page.content)
    link_list = doc.xpath("//*/ul/li/a/@href")
    if date:
        file_list = [x for x in link_list if str.isdigit(x[0:4]) and x[0:6] == date[0:6]]
    else:
        file_list = [x for x in link_list if str.isdigit(x[0:4])]
    return file_list

def upload_blob(bucket_name, source_file_name, destination_blob_name):
  """Uploads a file to the bucket."""
  storage_client = storage.Client()
  bucket = storage_client.get_bucket(bucket_name)
  blob = bucket.blob(destination_blob_name)
  blob.upload_from_filename(source_file_name)

  print('File {} uploaded to {}.'.format(
      source_file_name,
      destination_blob_name))

def download_results(url):
    r = request.get(url)
    zip_file = zipfile.ZipFile(BytesIO(r.content))
    return zip_file

def extract_zip_file(zip_file, path):
    zip_file.extractall(path)

##################################
if __name__ == "__main__":
    print("Executando Function!")
    ingest_data(request)
