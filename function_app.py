import logging
import azure.functions as func
import requests
import pandas as pd

app = func.FunctionApp()

@app.schedule(schedule="0 */5 * * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def SilververSenseFirstResponder(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function executed.')

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
    response = requests.get(url, params=params)

    # Check if the request was successful
    if response.status_code == 200:
        # Convert the JSON response to a Pandas DataFrame
        data = pd.DataFrame(response.json())
        print(data)
    else:
        print("Failed to fetch data: ", response.status_code)