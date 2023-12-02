import requests
import json

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from decouple import config

import numpy as np
import pandas as pd
from datetime import datetime, timedelta

import os

def lambda_handler(event, context):
    api_data_loader()

def api_data_loader():
    #### Read previous data from google sheets

    # Define the scope
    SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    CREDENTIALS = json.loads(config('CRED_GCP'))

    # Credentials
    # add credentials to the account
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(CREDENTIALS, SCOPE)

    # authorize the clientsheet
    client = gspread.authorize(credentials)

    # get the instance of the Spreadsheet
    sheet = client.open('Copy of N2N - Database')

    # get the first sheet of the Spreadsheet
    sheet_instance = sheet.get_worksheet(0)

    # Read google sheet
    data = sheet_instance.get_all_values()
    dfToUpdate = pd.DataFrame(data[1:], columns=data[0])

    # Read relevant variables
    # Define actual meeting number
    dfToUpdate['#'] = dfToUpdate['#'].astype(int)
    MAX_NUMBER = dfToUpdate['#'].max()


    #### Read new data -----------------------------------------------
    # Read eventbrite credentials

    with open("gcp_api/evenbrite_credentials.json", "r") as file:
        credentials = json.load(file)

    MY_PRIVATE_TOKEN = credentials["token"]
    ID_N2N = credentials["id_n2n"]


    url_all_events = f'https://www.eventbriteapi.com/v3/organizations/{ID_N2N}/events/' # all events from organization

    headers = {
        'Authorization': f'Bearer {MY_PRIVATE_TOKEN}',
    }

    # extract dates
    dfToUpdate['Date'] = pd.to_datetime(dfToUpdate['Date'], format='%B %d, %Y')
    dfToUpdate['Date'] = dfToUpdate['Date'].dt.strftime('%Y-%m-%d')
    dfToUpdate['Date'] = pd.to_datetime(dfToUpdate['Date'], format='%Y-%m-%d')

    start_date = dfToUpdate['Date'].max() + timedelta(days=1)
    start_date = start_date.strftime('%Y-%m-%d')

    end_date = (datetime.today() - timedelta(days=1))
    end_date = end_date.strftime("%Y-%m-%d")

    params_all_events = {
        'start_date.range_start': start_date,
        'start_date.range_end': end_date
    }

    # Extract id of all the meeting that are not in the spreadheet
    response_events = requests.get(url_all_events, headers=headers, params=params_all_events).json()
    #number_events = len(response_events['events'])
    try:
        number_events = len(response_events['events'])
    except KeyError as error:
        raise ValueError(f'There are no new meetings from {start_date} to {end_date}') from error

    all_events = response_events['events']

    id_events = [x['id'] for x in all_events]

    ##----------------------------------

    def join_attendees_information(event_id: str) -> list:
        """
        Arrange in a list of dictionaries all the information of the attendees

        Args:
            event_id (str): the event id from a meeting

        Returns:
            list: list made up of dictionaries with the information of each attendant by event
        """

        url_individual_event = f'https://www.eventbriteapi.com/v3/events/{event_id}/'
        response_individual_event = requests.get(url_individual_event, headers=headers).json()

        event_name =  response_individual_event['name']['text']
        date_attending = response_individual_event['start']['local']
        #date_attending = datetime.strptime(date_attending, "%Y-%m-%dT%H:%M:%S")
        #date_attending = date_attending.strftime("%Y-%m-%d")

        url_first_page = f'https://www.eventbriteapi.com/v3/events/{event_id}/attendees/' # url individual event
        response_first_page = requests.get(url_first_page, headers=headers).json()

        pages = response_first_page['pagination']
        page_count = pages['page_count']


        attendees = []
        for i in range(1,page_count+1):
            if i == 1:
                attendees = response_first_page['attendees']
            else:
                url_new_page = f'https://www.eventbriteapi.com/v3/events/{event_id}/attendees/?page={i}'
                new_page = requests.get(url_new_page,headers=headers).json()
                page_ateendees = new_page['attendees']
                attendees.extend(page_ateendees)
            #print(i)

        for attendee in attendees:
            attendee["event_name"] = event_name
            attendee["date_attending"] = date_attending

        return attendees


    '''total_attendees = []

    for x in id_events:
        attendees_info = join_attendees_information(x)
        total_attendees.extend(attendees_info)'''
    # join all the attendees information in one list.
    total_attendees = [attendee for x in id_events for attendee in join_attendees_information(x)]


    def list_to_df(total_attendees: list) -> pd.DataFrame:
        """
        Transform the json list of each attendee in a pandas data frame

        Args:
            total_attendees (list): json list with the information of each attendant

        Returns:
            df (dataframe): data frame with the information of each attendant
        """

        general_questions = ['Event Name',
                            'Date Attending',
                            'Attendee Status',
                            'Email',
                            'First Name',
                            'Last Name']

        question_list = ["What country are you from in Latin America? (if applicable)",
                                '¿De qué país eres en América Latina? (si aplica)',

                                'What area/subject do you specialize in? (in this industry)',
                                '¿En qué área/materia te especializas? (en esta industria)',

                                "What\'s your employment status?",
                                '¿Cuál es tu situación laboral?',

                                'If employed, what company do you work for?',
                                'Si estás empleado, ¿para qué empresa trabajas?',

                                'What is your dream job in Canada?',
                                '¿Cuál es tu trabajo soñado en Canadá?',

                                'Provide your LinkedIn if you want to connect with others in this community!',
                                '¡Proporciona tu LinkedIn si quieres conectarte con otros en esta comunidad!']

        columns = general_questions + question_list

        attendees_list = []
        for attendee in total_attendees:
            export_list = []
            export_list.append(attendee["event_name"]) # 'Event Name'
            export_list.append(attendee["date_attending"]) # 'Date Attending'
            export_list.append(attendee["status"]) # 'Attendee Status'

            export_list.append(attendee['profile']['email']) # email
            export_list.append(attendee['profile']['first_name']) # First name
            export_list.append(attendee['profile']['last_name']) # Last name

            for question in question_list:
                exist = False
                for answer in attendee['answers']:
                    if question == answer['question']:
                        exist = True

                        #dictionary, key
                        #dictionary.get(key, None)

                        export_list.append(answer.get('answer',None))
                        #export_list.append(get_value_or_none(answer,'answer'))

                if not exist:
                    export_list.append(None)

            attendees_list.append(export_list)

            df = pd.DataFrame(attendees_list, columns=columns)

            #print(question_list)
        return df

    df = list_to_df(total_attendees)

    #df.to_csv('test.csv', index=False, encoding='utf-8')
    #df.to_excel('test.xlsx', index=False)

    #### Process the data --------------------

    # City
    def extract_city(event_name:str) -> str:
        """
        Extract the name of the city using the event_name

        Args:
            event_name (str): field event_name from the evenbrite page

        Returns:
            str: The name of the city, can be Montreal or Toronto
        """
        if "Montreal" in event_name:
            return "Montreal"
        else:
            return "Toronto"

    df['City'] = df['Event Name'].apply(extract_city)

    # Seasons
    def return_season_toronto(meeting_date: str) -> int:
        """
        Return the seasson according to the meeting day
        Args:
            meeting_date (str): date of the meeting
        Returns:
            int: Number of the Toronto season
        """

        if meeting_date <= datetime(2021,3,4): return 1
        elif meeting_date <= datetime(2021,6,10): return 2
        elif meeting_date <= datetime(2021,12,9): return 3
        elif meeting_date <= datetime(2022,4,14): return 4
        elif meeting_date <= datetime(2022,7,28): return 5
        elif meeting_date <= datetime(2022,11,7): return 6
        elif meeting_date <= datetime(2023,4,6): return 7
        elif meeting_date <= datetime(2023,6,29): return 8
        elif meeting_date <= datetime(2023,12,31): return 9
        else: return 9

    def return_season_montreal(meeting_date: str) -> int:
        """
        Return the seasson according to the meeting day
        Args:
            meeting_date (str): date of the meeting
        Returns:
            int: Number of the Montreal season
        """
        if meeting_date <= datetime(2021,3,4):
            return 1
        else:
            return 1

    # Apply the function according to hte city

    def season_based_on_city(df: pd.DataFrame) -> int:
        """
        Return the season according the city of the meeting

        Args:
            df (dataframe): a dataframe to add the season

        Returns:
            int: Number of the season according to the city
        """

        if df['City'] == 'Toronto':
            return return_season_toronto(df['Date'])
        elif df['City'] == 'Montreal':
            return return_season_montreal(df['Date'])
        else:
            # Handle other cities or cases if needed
            return None

    # transform the data format
    df['Date'] = pd.to_datetime(df['Date Attending'], format='%Y-%m-%dT%H:%M:%S')
    df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
    df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d')

    # Apply the custom function to create a new 'Season' column
    df['Season'] = df.apply(season_based_on_city, axis=1)

    # Save the data according the spreadsheet
    df['Date'] = df['Date'].dt.strftime('%m/%d/%Y')

    # Industry/Event
    def extractName(eventName: str) -> str:
        """
        Extract the Industry or event from event Name

        Args:
            eventName (str): Full event name

        Returns:
            str: Name of the industry
        """
        if "|" in(eventName):
            split_result = eventName.split('|')

            eventNameBefore = split_result[0]
            eventNameAfter = split_result[1]

            if "NotWorking to Networking" in(eventNameBefore) or "NotWorking2Networking" in(eventNameBefore):
                eventName = eventNameAfter
            elif "NotWorking to Networking" in(eventNameAfter) or "NotWorking2Networking" in(eventNameAfter) or "N2N Montreal" in(eventNameAfter) or "N2N" in(eventNameAfter) or 'Not working to Networking Montreal' in(eventNameAfter) :
                eventName = eventNameBefore

            if 'Latinos in ' in eventName:
                split_result = eventName.split('Latinos in ')
                return split_result[1].strip()
            else:
                return eventName.strip()
        else:
            return eventName.strip()

    df['Industry / Event'] = df['Event Name'].apply(extractName)

    # Format
    def returnFormat(eventName: str) -> str:
        """_summary_

        Args:
            eventName (str): Full description of event

        Returns:
            str: Format of the meeting
        """

        return "In Person" if "In Person" in eventName else "Online"

    df['Format'] = df['Event Name'].apply(returnFormat)

    # Create an empty list to store the counter values
    counter_values = []

    # Initialize variables to keep track of the previous date and city
    prev_date = None
    prev_city = None
    counter = 0

    # Iterate through the DataFrame rows
    for index, row in df.iterrows():
        current_date = row['Date']
        current_city = row['City']

        # Check if either the date or city has changed from the previous row
        if current_date != prev_date or current_city != prev_city:
            counter += 1
        counter_values.append(counter)

        # Update the previous date and city values for the next iteration
        prev_date = current_date
        prev_city = current_city

    df['#'] = counter_values
    df['#'] = df['#'] + MAX_NUMBER

    # Attedance
    df['Attendance'] = df['Attendee Status']

    # Function add columns

    def add_cols (df:pd.DataFrame,field1:str, field2:str,result_field:str) -> str:
        """
        Add the information of two fields evaluating if one of both exist

        Args:
            df (pd.DataFrame): dataframe where the function will be applied
            field1 (str): first field to be added
            field2 (str): second field to be added
            result_field (str): name of the result field.

        Returns:
            str: dataframe with the new field
        """
        # Check if the columns exist in the DataFrame
        if field1 in df.columns and field2 in df.columns:
            # If both columns exist, perform the operations
            #df[field1] = df[field1].astype(str)
            #df[field2] = df[field2].astype(str)

            #df[field1].replace('nan','', inplace=True)
            #df[field2].replace('nan','', inplace=True)

            df[field1].fillna('', inplace=True)
            df[field2].fillna('', inplace=True)

            # Concatenate and strip
            df[result_field] = (df[field1] +
                                        df[field2]).str.strip()

            df[result_field] = df[result_field].str.title()

        elif field1 in df.columns:
            df[result_field] = df[field1].astype(str)
            df[result_field] = df[result_field].str.title()

        elif field2 in df.columns:
            df[result_field] = df[field2].astype(str)
            df[result_field] = df[result_field].str.title()
        else:
            # Handle the case where one or both columns are missing
            pass

    # Country of Origin
    add_cols(df,'What country are you from in Latin America? (if applicable)','¿De qué país eres en América Latina? (si aplica)','Country of Origin')

    # Define a dictionary for replacements
    replacements = {
        '': np.nan,
        'Canadá': 'Canada',
        'Perú': 'Peru',
        'España': 'Spain',
        'México': 'Mexico',
        'República Dominicana': 'Dominican Republic',
        '-': np.nan,
        'Not': np.nan,
        'nan': np.nan,
        'Alberta, British Columbia And Calgary.' : 'Canada',
        'Brasil': 'Brazil',
        'Amazonia' : 'Colombia',
        'Dr': np.nan,
        'Yes': np.nan,
        'Spain/Colombia': 'Colombia',
        'X' : np.nan,
        'Na,India': np.nan,
        'None (Spain)': np.nan,
        'India':np.nan,
        'South Africa':np.nan,
        'Puebla, México' : 'Mexico',
        'Ciudad De México' : 'Mexico',
        'Coló Me La' : np.nan,
        'Born In Canada.': np.nan,
        'Mex': np.nan
    }

    df['Country of Origin']=df['Country of Origin'].replace(replacements)

    # Area of Expertise
    add_cols(df,'What area/subject do you specialize in? (in this industry)','¿En qué área/materia te especializas? (en esta industria)','Area of Expertise')

    # Employment Status
    add_cols(df,"What\'s your employment status?",'¿Cuál es tu situación laboral?','Employment Status')

    # Define a dictionary for replacements
    replacementsEmploymentStatus = {
        'Empleado': 'Employed',
        'Empleado en búsqueda de nuevas oportunidades': 'Employed and looking for opportunities',
        'Desempleado y buscando oportunidades': 'Unemployed and looking for opportunities',
        'Empleado y en búsqueda de oportunidades': 'Employed and looking for opportunities',
        '': np.nan
    }

    df['Employment Status']=df['Employment Status'].replace(replacementsEmploymentStatus)
    df['Employment Status'] = df['Employment Status'].str.capitalize()

    # Employer
    add_cols(df,'If employed, what company do you work for?','Si estás empleado, ¿para qué empresa trabajas?','Employer')

    # Dream job
    add_cols(df,'What is your dream job in Canada?','¿Cuál es tu trabajo soñado en Canadá?','Dream Job')

    # Linkedin
    add_cols(df,'Provide your LinkedIn if you want to connect with others in this community!',
            '¡Proporciona tu LinkedIn si quieres conectarte con otros en esta comunidad!',
            'Linkedin')

    # Select and organize fields
    df2 = df[['#','Date', 'City', 'Season', 'Industry / Event', 'Format', 'Attendance',  'Email', 'First Name', 'Last Name', 'Country of Origin', 'Area of Expertise', 'Employment Status', 'Employer', 'Dream Job', 'Linkedin']]

    set_with_dataframe(sheet_instance, df2, row=dfToUpdate.shape[0] + 2, include_column_header=False)
    print('ok')

