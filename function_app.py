import logging
import azure.functions as func
import requests
import pandas as pd
import pymysql
from azure.communication.sms import SmsClient
from azure.communication.email import EmailClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
import os
import json
import datetime 

def FindUnpairedEvents(df):
   # Ensure Event_Time is in datetime format and sort the DataFrame
    df['Event_Time'] = pd.to_datetime(df['Event_Time'])
    df = df.sort_values(by='Event_Time', ascending=True)
    
    event_split = df['Event'].str.rsplit('-', n=1, expand=True)
    df['EventName'] = event_split[0]
    df['State'] = event_split[1]

    events = {}
    unpaired_events = {}

    for index, row in df.iterrows():
        event = row['EventName']
        state = row['State']
        event_type = row['Type']
        timestamp = pd.to_datetime(row['Event_Time'])
        expected_end = pd.to_datetime(row['Expected_End'])
        delta = row['Delta']

        if event not in events:
            events[event] = {'on': None, 'off': None, 'details': {}}

        if state == 'on':
            if events[event]['on'] is not None and events[event]['off'] is None:
                # If an "on" event occurs without an "off", update the unpaired event
                unpaired_events[event] = f'Missing "off" before new "on" at {timestamp}'
            events[event]['on'] = timestamp
            events[event]['off'] = None  # Reset "off" when a new "on" is encountered
        elif state == 'off':
            if events[event]['on'] is None:
                # If an "off" event occurs without an "on", update the unpaired event
                unpaired_events[event] = {
                    'Event':event,
                    'Missing_Status' :'on',
                    'Type': event_type,
                    'Timestamp': timestamp,
                    'Expected_End': expected_end,
                    'Delta': delta
                    }
            else:
                # Once a proper pair is completed, remove the event from unpaired if it exists
                unpaired_events.pop(event, None)
            events[event]['on'] = None  # Reset "on" after an "off" is encountered
            events[event]['off'] = timestamp

    # After processing all rows, check for any "on" events without a corresponding "off"
    for event, info in events.items():
        if info['on'] is not None:
            # Assuming event type, expected time, and delta for the last "on" event
            unpaired_events[event] = {
                'Missing_Status': 'off',
                'Type': event_type,  # This might need adjustment to reflect accurate event details
                'Timestamp': info['on'],
                'Expected_End': expected_end,  # Similar note as above
                'Delta': delta  # Similar note as above
            }

   # return [(event, details['Missing_Status'], details['Type'], details['Timestamp'], details['Expected_Time'], details['Delta']) for event, details in unpaired_events.items()]
    return unpaired_events

# Example usage:
# Ensure your DataFrame 'df' is properly defined with 'Event', 'Event_Time', and other columns as per your setup
# unpaired_events = find_last_unpaired_events(df)
# for event_info in unpaired_events:
#     print(event_info)



def SendEmail(AlertData):

   # Iterate through the AlertData array
    messagetext = ''
    logmessage = ''
    sms_response = 'No SMS Required'
    emailto = "matt@griffiths.uk.net"

    logging.info(f"Preparing email to matt@griffiths.uk.net:{messagetext}")
    for event, data in AlertData.items():
        
        # Access elements of the first entry

        if data['Type'] == 'Missed':
            # Build the message string with other values except for delta since that creates a unique value for each event
            logmessage += f"Silversense Alert: Event: {event} : {data['Missing_Status']}, Status: {data['Type']}, Expected From: {data['Expected_End']}."
            #This is the message that will be sent to the email
            messagetext += f"Silversense Alert: Event: {event} : {data['Missing_Status']}, Status: {data['Type']}, Expected From: {data['Expected_End']}. Variance {data['Delta']}. "

    if messagetext !=  '':
        try:
            # Send an SMS
            logging.info(f"Preparing email to matt@griffiths.uk.net:{messagetext}")

                #Get the connection string from the environment variable
            connection_string = os.getenv('CUSTOMCONNSTR_SilverSenseEmail')
            #connection_string = "endpoint=https://firstrespondersms.unitedstates.communication.azure.com/;accesskey=3OWojht0Xjuilyyb7g5DCA4riP4Y3OnjyAGXozVK9/Sj/uoqSZvXpbyM6O4ssG6APkRAkqeGKiz1TwpDKwppGw=="
            client = EmailClient.from_connection_string(connection_string)
            
            try:
                #Log transaction into database. If its a unique event, it will be logged.  If its a duplicate, it will fail and be ignored.
                LogResponse('Griffiths0001',logmessage,'email',emailto) 
                # Create the email message
                message = {
                                "senderAddress": "Silversense@880cace6-bb7a-463e-8e0f-ad8ce0e38166.azurecomm.net",
                                "recipients":  {
                                    "to": [{"address": emailto }],
                                },
                                "content": {
                                    "subject": "SilverSenseAlert",
                                    "plainText": messagetext,
                                }
                            }
                # Send the email
                poller = client.begin_send(message)
                result = poller.result()
                logging.info(f"Email sent to matt@griffiths.uk.net. Result:{result}")
                    
            except Exception as e:
                #expect error here when pulling duplicate events / actions
                 logging.info(f"Database Response {e}")

        except Exception as e:
            logging.error("Error Sending Email", e)
            raise e

def SendWhatsApp(AlertData):
    import os
    from azure.communication import NotificationMessagesClient
    from azure.communication import SendMessageOptions

    # This code retrieves your connection string from an environment variable.
    connectionString = "endpoint=https://firstrespondersms.unitedstates.communication.azure.com/;accesskey=3OWojht0Xjuilyyb7g5DCA4riP4Y3OnjyAGXozVK9/Sj/uoqSZvXpbyM6O4ssG6APkRAkqeGKiz1TwpDKwppGw==";
    notification_messages_client = NotificationMessagesClient.from_connection_string(connectionString)

    recipient_list = ['+18573030250']

    send_text_message_options = SendMessageOptions(channel_registration_id="<channel_registration_id>",recipients=recipient_list,content="")

    text_response = notification_messages_client.send_message(send_text_message_options)

def LogResponse(Member,ResponseMessage, Action, ResponseAddress):

    
    #Get the connection string from the environment variable
    connection_string = os.getenv('MYSQLCONNSTR_SilverSenseMySQL')

    try:
        # Connect to MySQL
        logging.info(f"ConnectionString: {connection_string}")
        logging.info("Starting mySQL Connection")

        # Create an engine to connect to the database
        engine = create_engine(connection_string, echo=True)
        Session = sessionmaker(bind=engine)
        session = Session()

        # Define your SQL query using text()
        sql = text("INSERT INTO actionresponse (Member, ResponseMessage, Action, ResponseAddress, ResponseTime) VALUES (:member, :response_message, :action, :response_address, :response_time")
        ResponseTime = datetime.now().isoformat()
        logging.info(f"Adding Parameters to SQL Query: {Member}, {ResponseMessage}, {Action}, {ResponseAddress}, {ResponseTime}")
        params = {
            'member': Member,  # Ensure Member and others are defined
            'response_message': ResponseMessage,
            'action': Action,
            'response_address': ResponseAddress,
            'response_time': ResponseTime
        }
        logging.info("Executing SQL Query")
        # Execute the query
        with engine.connect() as connection:
            # Execute the query with parameters as a dictionary
            connection.execute(sql, params)
            logging.info("Data inserted successfully")
               
    except Exception as e:
        logging.error("Database Execution Error", e)
        raise e


def LogResponse(Member,ResponseMessage, Action, ResponseAddress, ):

    host = 'silversensemysql.mysql.database.azure.com'
    user = 'MattRGriffiths'
    password = 'AgeInPlace1576'
    database = 'silversense'
    
    try:
        # Connect to MySQL
        logging.info("Starting mySQL")
        connection = pymysql.connect(host=host, user=user, password=password, database=database)
        cursor = connection.cursor()
        
        # Define the SQL query with placeholders
        sql = "INSERT INTO actionresponse (Member, ResponseMessage, Action, ResponseAddress, ResponseTime) VALUES (%s, %s, %s, %s, NOW())"

        # Execute the query with the provided values
        cursor.execute(sql, (Member, ResponseMessage, Action, ResponseAddress ))
        connection.commit()
        
        logging.info("Data inserted successfully")
        
    except pymysql.MySQLError as e:
        
        raise e

    finally:
        if connection:
	        connection.close()


def SendSMS(AlertData):
     
    ConnectionString = 'endpoint=https://firstrespondersms.unitedstates.communication.azure.com/;accesskey=3OWojht0Xjuilyyb7g5DCA4riP4Y3OnjyAGXozVK9/Sj/uoqSZvXpbyM6O4ssG6APkRAkqeGKiz1TwpDKwppGw=='

    # Create an SMS client using the connection string
    sms_client = SmsClient.from_connection_string(ConnectionString)
    sms_phonenumber = '+18332419135'
    #sms_phonenumber ='5555'
    recipiant_number = '+18573030250'
    print(AlertData)
    
    # Get the first entry from the array

    # Iterate through the AlertData array
    messagetext = ''
    sms_response = 'No SMS Required'
    for data in AlertData:
        
        # Access elements of the first entry

        if data['Type'] == 'Missed':
            # Build the message string with other values
            sms_response += f"Silversense Alert: Event: {data['Event']}, Status: {data['Type']}, Expected: {data['Expected_End']}. Variance {data['Delta']} \n"


    print(sms_response)

    if sms_response !=  '':
        try:
            # Send an SMS
            logging.info(f"Sending SMS to {sms_phonenumber}:{sms_response}")
            
        #    sms_response = sms_client.send(
        #        from_=sms_phonenumber,  # The phone number associated with your Azure Communication Services resource
        #        to=[recipiant_number],  # The recipient's phone number
        #        message=messagetext,
        #        # Optional parameters
        #        enable_delivery_report=True,  # For delivery reports
        #        tag="")  # Custom tag to identify messages



            # SMS response provides message IDs and status
            for message in sms_response:
                print(f"Message ID: {message.message_id} Sent to: {message.to}")
        except Exception as e:
            logging.error(f"Sending SMS Failed")
            
    return sms_response


app = func.FunctionApp()

@app.schedule(schedule="0 */5 * * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 

def SilververSenseFirstResponder(myTimer: func.TimerRequest) -> None:

    try:
        if myTimer.past_due:    
            logging.info('The timer is past due!')

        logging.info('First Responder Version 1.3 Build 9. Startng.')

        
        url = "https://silversense.azurewebsites.net/data"

         
        logging.info('Loeading Strings')
        #params_env_str = "{\"member\": \"Griffiths0001\", \"useday\": \"false\", \"starthour\": \"05\", \"mincluster\": \"3\", \"debug\": \"none\", \"threshold\": \"20\", \"grouptime\": \"0\"}"
        params_env_str = os.getenv('SilverSenseURLParam')
       

        # Convert the string back to a dictionary
        logging.info(f"Loading {params_env_str} into json object.  Checking type.")
        params_env = json.loads(params_env_str)
        params_env_json_type = type(params_env)

        logstring = f"Using Parameters from Env Type {params_env_json_type}. {params_env}"
        
        logging.info(logstring)
        
        try:
            logging.info(f"Requested Submitted with env parameters: {params_env}")

            # Make the GET request
            response = requests.get(url, verify=False, params=params_env)

            logging.info(f"Response Success with parameters: {params_env}")

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

            try:
                #Check returned data set for 2 things
                # 1.  Are there any events that are on but no off, or off but no on.  E.g. Not returned from dogwalk, not returned to bed, door open not closed. 
                UnpairedData = FindUnpairedEvents(data)
                logging.info(f"Alert Events: {UnpairedData}")
                # Then 2. Are any of those considered Missing
                SendEmail(UnpairedData)
                #SendSMS(UnpairedData)

            except Exception as e:
                    logging.error("Error Processing Alert Events")
                    raise e
            


        else:
            logging.error(f"Failed to fetch data: {responsecode}")
            
    except Exception as e:
        logging.error("Error Processing First Responder", e)
        raise e


