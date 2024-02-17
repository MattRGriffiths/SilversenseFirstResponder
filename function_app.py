import logging
import azure.functions as func
import requests
import pandas as pd
from azure.communication.sms import SmsClient
from azure.communication.email import EmailClient

def FindUnpairedEvents(df):
    #Identifes Any 
    events = {}

    event_split = df['Event'].str.rsplit('-', n=1, expand=True)

# Assign the split results to new columns in the DataFrame
    df['Event'] = event_split[0]
    df['Value'] = event_split[1]

    for index, row in df.iterrows():
        event = row['Event']
        value = row['Value']
        timestamp = row['Event_Time']

        if event not in events:
            events[event] = {'on': [], 'off': [] ,'Timestamp': pd.to_datetime(row['Event_Time']), 'Value': row['Value'], 'Type': row['Type'], 'Expected_End': pd.to_datetime(row['Expected_End']), 'Delta': row['Delta']}
        
        events[event][value].append(timestamp)
        #events[event][value].append({'Timestamp': pd.to_datetime(row['Event_Time']), 'Value': row['Value'], 'Type': row['Type'], 'Expected_End': pd.to_datetime(row['Expected_End']), 'Delta': row['Delta']})

    earliest_unpaired_events = []
   # for event, times, Type, Expected_End, Timestamp, Delta in events.items():
    for event, data in events.items():

        Type = data['Type']
        Expected_End = data['Expected_End']
        Timestamp = data['Timestamp']
        Delta = data['Delta']

        on_times = sorted(data['on'])
        off_times = sorted(data['off'])        


        paired = min(len(on_times), len(off_times))
        unpaired_on = on_times[paired:]
        unpaired_off = off_times[paired:]

        if unpaired_on:  # Unpaired 'on' events
            #Event didnt start - we are missing an On event
            for timestamp in unpaired_on:
                earliest_unpaired_events.append({
                    'Event':event,
                    'Event_Status': 'off',
                    'Event_Time': Timestamp.isoformat(),
                    'Type': Type,
                    'Expected_End':Expected_End,
                    'Delta': Delta

                })
                break  # Report only the earliest unpaired 'on' event

        elif unpaired_off:  
            #Event started but didnt finish
            for timestamp in unpaired_off:
                earliest_unpaired_events.append({
                    'Event': event,
                    'Event_Status': 'on',
                    'Event_Time': Timestamp.isoformat(),
                    'Type': Type,
                    'Expected_End':Expected_End,
                    'Delta': Delta
                })
                break  # Report only the earliest unpaired 'off' event

    # Filter the array for items where Type is either "Missed" or "Missing"
   # filtered_earliest_unpaired_events = [item for item in earliest_unpaired_events if item['Type'] in ('Missed', 'Missing')]


    return earliest_unpaired_events



def SendEmail(AlertData):

   # Iterate through the AlertData array
    messagetext = ''
    sms_response = 'No SMS Required'
    emailto = "matt@griffiths.uk.net"
    for data in AlertData:
        
        # Access elements of the first entry

        if data['Type'] == 'Missed':
            # Build the message string with other values
            messagetext += f"Silversense Alert: Event: {data['Event']}, Status: {data['Type']}, Expected: {data['Expected_End']}. Variance {data['Delta']}"


    print(messagetext)

    if messagetext !=  '':
        try:
            # Send an SMS
            logging.info(f"Sending SMS to matt@griffiths.uk.net:{messagetext}")

            connection_string = "endpoint=https://firstrespondersms.unitedstates.communication.azure.com/;accesskey=3OWojht0Xjuilyyb7g5DCA4riP4Y3OnjyAGXozVK9/Sj/uoqSZvXpbyM6O4ssG6APkRAkqeGKiz1TwpDKwppGw=="
            client = EmailClient.from_connection_string(connection_string)
            try:
                LogResponse('Griffiths0001',messagetext,'email',emailto) 
           
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

                poller = client.begin_send(message)
                result = poller.result()
                    
            except Exception as e:
                #expect error here when pulling duplicate events / actions
                print(e)

        except Exception as e:
            print(e)
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
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('First Responder Version 1.1 Triggered from Timer.')

    try:
        url = "https://silversense.azurewebsites.net/data"
        params = {
            "member": "Griffiths0001",
            "useday": "true",
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


