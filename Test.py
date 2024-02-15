

import logging
import azure.functions as func
import requests
import pandas as pd



# Define the URL of the web service
url = "https://silversense.azurewebsites.net/data"
params = {
"member": "Griffiths0001",
"useday": "false",
"starthour": "21",
"mincluster": "3",
"debug": "none",
"threshold": "10"
}

# Make the GET request
response = requests.get(url, verify=False , params=params)

logging.info('Requested Submitted.')

# Check if the request was successful
responsemessage = f"code: {response.status_code} . Message: {response.text}"

if response.status_code == 200:
    # Convert the JSON response to a Pandas DataFrame
    data = pd.DataFrame(response.json())
    logging.info('Got Data')
else:
    logging.info(f"Failed to fetch data: {responsemessage}")