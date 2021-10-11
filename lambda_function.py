import sys
import logging
import rds_config
import pymysql
import main_file
import os
import subprocess
#rds settings
rds_host  = "database.cxmsifyr3lmy.us-east-1.rds.amazonaws.com"
name = rds_config.db_username
password = rds_config.db_password
db_name = rds_config.db_name

logger = logging.getLogger()
logger.setLevel(logging.INFO)

try:
    conn = pymysql.connect(host=rds_host, user=name, passwd=password, db=db_name, connect_timeout=5)
except pymysql.MySQLError as e:
    logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
    logger.error(e)
    sys.exit()

logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")

def get_slots(intent_request):
    return intent_request['sessionState']['intent']['slots']
    
def get_slot(intent_request, slotName):
    slots = get_slots(intent_request)
    if slots is not None and slotName in slots and slots[slotName] is not None:
        return slots[slotName]['value']['interpretedValue']
    else:
        return None    

def get_session_attributes(intent_request):
    sessionState = intent_request['sessionState']
    if 'sessionAttributes' in sessionState:
        return sessionState['sessionAttributes']

    return {}

def elicit_intent(intent_request, session_attributes, message):
    return {
        'sessionState': {
            'dialogAction': {
                'type': 'ElicitIntent'
            },
            'sessionAttributes': session_attributes
        },
        'messages': [ message ] if message != None else None,
        'requestAttributes': intent_request['requestAttributes'] if 'requestAttributes' in intent_request else None
    }

def delegate(intent_request, session_attributes, fulfillment_state, message):
    intent_request['sessionState']['intent']['state'] = fulfillment_state
    return {
        'sessionState': {
            'sessionAttributes': session_attributes,
            'dialogAction': {
                'type': 'Delegate'
            },
            'intent': intent_request['sessionState']['intent']
        },
        'messages': [message],
        'sessionId': intent_request['sessionId'],
        'requestAttributes': intent_request['requestAttributes'] if 'requestAttributes' in intent_request else None
    }

def close(intent_request, session_attributes, fulfillment_state, message):
    intent_request['sessionState']['intent']['state'] = fulfillment_state
    input = get_slot(intent_request,'Input')
    operation = get_slot(intent_request,'CRUD')
    db_name = get_slot(intent_request,'DatabaseCheck')
    if input is not None and intent_request['interpretations'][0]['intent']['confirmationState'] == 'Confirmed':
        user_input = intent_request['interpretations'][0]['intent']['slots']['Input']['value']['interpretedValue'] 
        query = main_file.process_sentence(user_input)
        
        """
        This function fetches content from MySQL RDS instance
        """
    
        item_count = 0
        result = []
        with conn.cursor() as cur:
            conn.commit()
            cur.execute(query)
            print(cur)
            for row in cur:
                item_count += 1
                # logger.info(row)
                result.append(row)
                print(row)
        conn.commit()
        
    return {
        'sessionState': {
            'sessionAttributes': session_attributes,
            'dialogAction': {
                'type': 'Close'
            },
            'intent': intent_request['sessionState']['intent']
        },
        'messages': [message],
        'sessionId': intent_request['sessionId'],
        'requestAttributes': intent_request['requestAttributes'] if 'requestAttributes' in intent_request else None
    }

def confirm_intent(intent_request, session_attributes, fulfillment_state, message, query):
    intent_request['sessionState']['intent']['state'] = fulfillment_state
    query = 'select * from Employee'
    user_input = intent_request['interpretations'][0]['intent']['slots']['Input']['value']['interpretedValue']
    query = main_file.process_sentence(user_input)
    message =  {
        'contentType': 'PlainText',
        'content': "Thank you! The deciphered query is, "+query+" Please confirm if you would like to execute the same."
    }
    return {
        'sessionState': {
            'sessionAttributes': session_attributes,
            'dialogAction': {
                'type': 'ConfirmIntent'
            },
            'intent': intent_request['sessionState']['intent']
        },
        "intentName": intent_request['sessionState']['intent']['name'],
        "slots": get_slots(intent_request),
       "responseCard": {
          "version": 1,
          "contentType": "application/vnd.amazonaws.card.generic",
          "genericAttachments": [
              {
                 "title":"Title",
                 "buttons":[ 
                     {
                        "text":"Yes, Go ahead and execute!",
                        "value":"yes"
                     },
                     {
                        "text":"No, I want to change my request",
                        "value":"no"
                     }
                  ]
               } 
           ] 
        },
        'messages': [message],
        'sessionId': intent_request['sessionId'],
        'requestAttributes': intent_request['requestAttributes'] if 'requestAttributes' in intent_request else None
    }

def convertToSQL(intent_request):
    session_attributes = get_session_attributes(intent_request)
    slots = get_slots(intent_request)
    databaseName = get_slot(intent_request, 'DatabaseCheck')
    operation = get_slot(intent_request, 'CRUD')
    if(operation == 'Update' or operation == 'Delete'):
        message =  {
            'contentType': 'PlainText',
            'content': 'Sorry! We currently support only Create and Read operations. Other features are coming soon!'
        }
        return close(intent_request, session_attributes, "Fulfilled", message) 
    input = get_slot(intent_request,'Input')
    query = "select * from Employee"
    text = "Thank you. The balance on your account is $0 dollars."
    message =  {
            'contentType': 'PlainText',
            'content': text
        }
    fulfillment_state = "Fulfilled"
    if input is not None:
        query = "select * from Employee where Name = 'Abhinav'"
    else :
        return delegate(intent_request, session_attributes, fulfillment_state, message)
    if intent_request['interpretations'][0]['intent']['confirmationState'] == 'Denied':
        message =  {
        'contentType': 'PlainText',
        'content': "Alright! Please let me know if we can assist you with anything else."
        }
        close(intent_request, session_attributes, "Fulfilled", message)
    elif input is not None and intent_request['interpretations'][0]['intent']['confirmationState'] != 'Confirmed':
        return confirm_intent(intent_request, session_attributes, fulfillment_state, message, query)
    else:\
        return close(intent_request, session_attributes, fulfillment_state, message)   

def createUser(intent_request):
    session_attributes = get_session_attributes(intent_request)
    slots = get_slots(intent_request)
    firstname = get_slot(intent_request, 'firstname')
    lastname = get_slot(intent_request, 'lastname')
    emailid = get_slot(intent_request, 'emailid')
    text = "Success! The user "+ firstname +" "+lastname+" with email ID: "+emailid +" has been added to the USER DB"
    message =  {
            'contentType': 'PlainText',
            'content': text
        }
    fulfillment_state = "Fulfilled"
    intent_request['sessionState']['intent']['state'] = fulfillment_state
    with conn.cursor() as cur:
        cur.execute('insert into user (firstname, lastname, emailid) values("'+firstname+'", "'+lastname+'", "'+emailid+'")')
        conn.commit()
        for row in cur:
            item_count += 1
            logger.info(row)
            result.append(row)
    conn.commit()
    return {
        'sessionState': {
            'sessionAttributes': session_attributes,
            'dialogAction': {
                'type': 'Close'
            },
            'intent': intent_request['sessionState']['intent']
        },
        'messages': [message],
        'sessionId': intent_request['sessionId'],
        'requestAttributes': intent_request['requestAttributes'] if 'requestAttributes' in intent_request else None
    }
    
    
def dispatch(intent_request):
    intent_name = intent_request['sessionState']['intent']['name']
    response = None
    # Dispatch to your bot's intent handlers
    if intent_name == 'Command2Convert':
        return convertToSQL(intent_request)
    elif intent_name == 'CreateUser':
        return createUser(intent_request)

    raise Exception('Intent with name ' + intent_name + ' not supported')

def lambda_handler(event, context):
    response = dispatch(event)
    return response