import logging
import azure.functions as func
import requests
import pandas as pd



def find_earliest_unpaired_event(df):
    events = {}

    event_split = df['Event'].str.rsplit('-', n=1, expand=True)

# Assign the split results to new columns in the DataFrame
    df['Event'] = event_split[0]
    df['Value'] = event_split[1]

    for index, row in df.iterrows():
        event = row['Event']
        value = row['Value']
        timestamp = pd.to_datetime(row['Event_Time'])
        type = row['Type']
        expected_end= pd.to_datetime(row['Expected_End'])
        delta = row['Delta']

        if event not in events:
            events[event] = {'on': [], 'off': []}
        
        events[event][value].append(timestamp)

    earliest_unpaired_events = []
    for event, times in events.items():
        on_times = sorted(times['on'])
        off_times = sorted(times['off'])

        paired = min(len(on_times), len(off_times))
        unpaired_on = on_times[paired:]
        unpaired_off = off_times[paired:]

        if unpaired_on:  # Unpaired 'on' events
            for timestamp in unpaired_on:
                earliest_unpaired_events.append({
                    'Event': event,
                    'Missing_status': 'off',
                    'Event_Time': timestamp.isoformat(),
                    'Type': type,
                    'Expected_End':expected_end,
                    'Delta': delta

                })
                break  # Report only the earliest unpaired 'on' event

        elif unpaired_off:  # Unpaired 'off' events, less likely but possible
            for timestamp in unpaired_off:
                earliest_unpaired_events.append({
                    'Event': event,
                    'Missing_status': 'on',
                    'Event_Time': timestamp.isoformat(),
                    'Type': type,
                    'Expected_End':expected_end,
                    'Delta': delta
                })
                break  # Report only the earliest unpaired 'off' event

    # Filter the array for items where Type is either "Missed" or "Missing"
   # filtered_earliest_unpaired_events = [item for item in earliest_unpaired_events if item['Type'] in ('Missed', 'Missing')]


    return earliest_unpaired_events

app = func.FunctionApp()

@app.schedule(schedule="0 */5 * * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def SilververSenseFirstResponder(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function executed with Pandas and request imported.')

 
try:
    url = "https://silversense.azurewebsites.net/data"
    params = {
        "member": "Griffiths0001",
        "useday": "false",
        "starthour": "21",
        "mincluster": "3",
        "debug": "none",
        "threshold": "20",
        "grouptime": "0"
    }


    # Make the GET request
    response = requests.get(url, verify=False , params=params)

    logging.info(f"Requested Submitted with parameters: {params}")

except Exception as e:
        # Check if the request was successful
        responsecode = f"code: {response.status_code} . Message: {response.text}"
        logging.error(responsecode)
        raise e

responsecode = f"code: {response.status_code} . Message: {response.text}"

if response.status_code == 200:
    # Convert the JSON response to a Pandas DataFrame
    data = pd.DataFrame(response.json())
    logging.info('Data Loaded Into Dataframe')
    # Find the earliest unpaired events
    df_query_string = 'Type =="Missed" or Type == "Missing"'
    anomalydata = data.query(df_query_string)
    try:
        alertdata = find_earliest_unpaired_event(anomalydata)
        logging.info(f"Alert Events: {alertdata}")
    except Exception as e:
            logging.error("Error Processing Alert Events")
            raise e
    
    df_query_string = 'Type =="Late" or Type == "Early"'
    anomalydata = data.query(df_query_string)
    try:
        warningdata = find_earliest_unpaired_event(anomalydata)
        logging.info(f"Warning Events: {warningdata}")
    except Exception as e:
            logging.error("Error Processing Warning Events")
            raise e    

else:
     logging.error(f"Failed to fetch data: {responsecode}")