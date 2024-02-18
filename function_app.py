import logging
from datetime import datetime

import azure.functions as func
import requests
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobClient

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.function_name('function_app')
@app.route(route = 'function_app')
def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    credential = DefaultAzureCredential(exclude_shared_token_cache_credential=True)
    DefaultAzureCredential(exclude_interactive_browser_credential=False)
    secret_client = SecretClient(vault_url = 'https://keyvault-aroguska-poc1.vault.azure.net/', credential = credential)

    output_blob_data = {
        'blob_name' : f'{datetime.now().strftime("%X %d.%m.%Y_")}airquality_data.txt',
        'container_name' : 'input',
        'connection_string' : 'DefaultEndpointsProtocol=https;AccountName=sa0aroguska0poc1;AccountKey=+VXQWrw/UvQYrJ3Woup62H2Ki7U5l6yyJMnHES7JcqYFQu61uidOSHt7Fq52jiBZgpTVboEKYDMo+AStMt3WuQ==;EndpointSuffix=core.windows.net'#(secret_client.get_secret('connection-string-aroguska-poc1')).value
        }
    blob_client = BlobClient.from_connection_string(
         output_blob_data['connection_string'],
         container_name = output_blob_data['container_name'],
         blob_name = output_blob_data['blob_name']
    )
    input_data_url = 'https://api.gios.gov.pl/pjp-api/rest/aqindex/getIndex/52'

    with requests.Session() as rs:
            request = rs.get(input_data_url)
            data = request.content
    try:
        blob_client.upload_blob(data)
    except:
        return func.HttpResponse(f'Hello, some issue occurred and loading data into blob storage was not possible :(')
    
    return func.HttpResponse(f'Data was successfully loaded into blob storage <3', status_code = 200)