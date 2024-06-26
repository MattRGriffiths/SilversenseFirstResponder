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
from sqlalchemy.exc import SQLAlchemyError
import os
import json
from datetime import datetime
import pytz
#from durable.lang import *

def FindUnpairedEvents(df, localtime):
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
        data_time = pd.to_datetime(localtime)
        expected_start = pd.to_datetime(row['Expected_Start'])
        expected_end = pd.to_datetime(row['Expected_End'])

        if expected_start.tzinfo is None:
            expected_start = pytz.utc.localize(expected_start)
        if expected_end.tzinfo is None:
            expected_end = pytz.utc.localize(expected_end)

        delta = row['Delta']
        if event_type == 'Active':

            if (state == 'on' and data_time < expected_start) :
                unpaired_events[event] = {
                    'Event':event,
                    'Missing_Status' :state,
                    'Type': event_type,
                    'Timestamp': data_time ,
                    'Expected_Start': expected_start,
                    'Expected_End': timestamp,
                    'Delta': delta}
            
            
            elif  (state == 'off' and data_time > expected_end):

                    unpaired_events[event] = {
                        'Event':event,
                        'Missing_Status' :state,
                        'Type': event_type,
                        'Timestamp': timestamp,
                        'Expected_Start': expected_start,
                        'Expected_End': expected_end,
                        'Delta': delta}

   # return [(event, details['Missing_Status'], details['Type'], details['Timestamp'], details['Expected_Time'], details['Delta']) for event, details in unpaired_events.items()]
    return unpaired_events



def SendEmail(AlertData):

   # Iterate through the AlertData array
    messagetext = ''
    logmessage = ''
    sms_response = 'No SMS Required'
    emailto = "matt@griffiths.uk.net"


    for event, data in AlertData.items():
        
        # Access elements of the first entry

        if data['Type'] == 'Missed' or data['Type'] == 'Active':
            # Build the message string with other values except for delta since that creates a unique value for each event
            logmessage += f"Event: {event} : {data['Missing_Status']}. Status: {data['Type']}. Expected at: {data['Expected_End']}."
            #This is the message that will be sent to the email
            messagetext += f"Event: {event} : {data['Missing_Status']}. Status: {data['Type']}. Expected At: {data['Expected_End']}. Variance: {data['Delta']} mins. "

    if messagetext !=  '':
        try:
            # Send an SMS
            messagetext = f"Silversense Alert: {messagetext}"
            logging.info(f"Preparing email to {emailto}:{messagetext}")

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
                logging.info(f"Email sent to {emailto}. Result:{result}")
                    
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
        engine = create_engine(connection_string, echo=False)
        logging.info("Connected")
        Session = sessionmaker(bind=engine)
        session = Session()
        
        try:
            # Attempt to execute a simple query to check the connection
            with engine.connect() as connection:
                connection.execute(text("SELECT 1"))
                logging.info("Database connection Successful.")
        except SQLAlchemyError as e:
            logging.error(f"Database connection failed: {e}")

        # Define your SQL query using text()
        ResponseTime = datetime.now().isoformat()
        truncated_string = ResponseMessage[:675]
        logging.info(f"Adding Parameters to SQL Query: {Member}, {ResponseMessage}, {Action}, {ResponseAddress}, {ResponseTime}")

        sql = text("INSERT INTO actionresponse (Member, ResponseMessage, Action, ResponseAddress, ResponseTime) VALUES (:member, :response_message, :action, :response_address, :response_time)")
        
        logging.info("SQL Created. Adding Parameters to params object")
                

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
            connection.commit()
               
    except SQLAlchemyError as e:
        logging.error("Database Execution Error", e)
        raise e


def LogResponse2(Member,ResponseMessage, Action, ResponseAddress ):

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

@app.schedule(schedule="0 */15 * * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 

def SilververSenseFirstResponder(myTimer: func.TimerRequest) -> None:

   
        if myTimer.past_due:    
            logging.info('The timer is past due!')

        logging.info('First Responder Version 1.4 Build 1. Starting.')

        Main()

def Main() -> None:

    try:
        url = "https://silversense.azurewebsites.net/data"

         
        logging.info('Loeading Strings')
        #params_env_str = "{\"member\": \"Griffiths0001\", \"useday\": \"false\", \"starthour\": \"05\", \"mincluster\": \"3\", \"debug\": \"none\", \"threshold\": \"20\", \"grouptime\": \"0\"}"
        params_env_str = os.getenv('SilverSenseURLParam')
       
        if params_env_str is None:params_env_str = "{\"member\": \"Griffiths0001\", \"useday\": \"true\", \"starthour\": \"05\", \"mincluster\": \"3\", \"debug\": \"none\", \"threshold\": \"20\", \"grouptime\": \"0\"}"
        
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
                local_timezone = pytz.timezone('Europe/London')
                data_timezone =  pytz.timezone('UTC')         

                # Get the current time in the specified timezone    
                data_time = datetime.now(data_timezone)
                UnpairedData = FindUnpairedEvents(data, data_time)
                logging.info(f"Alert Events: {UnpairedData}")
                # Then 2. Are any of those considered Missing
                if len(UnpairedData) > 0: SendEmail(UnpairedData)
                #SendSMS(UnpairedData)

            except Exception as e:
                    logging.error("Error Processing Alert Events")
                    raise e
            


        else:
            logging.error(f"Failed to fetch data: {responsecode}")
            
    except Exception as e:
        logging.error("Error Processing First Responder", e)
        raise e


