import streamlit as st
from azure.storage.blob import BlobServiceClient, ContainerClient

import warnings
import pandas as pd
from io import BytesIO, StringIO
from urllib.parse import quote

from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DateTime, ForeignKey, Text, text
from sqlalchemy.orm import sessionmaker

import os
# from dotenv import load_dotenv, find_dotenv
# load_dotenv(find_dotenv("./application/.env"))

# Suppress FutureWarning messages
warnings.simplefilter(action='ignore', category=FutureWarning)

# setup connect with Blob storage
CONNECTION_STRING = os.getenv("BLOB_CONN_STRING")
CONTAINER_NAME = os.getenv("BLOB_CONTAINER")
BUS_SCHEDULE_FILE = os.getenv("BUS_SCHEDULE_FILE")


def upload_to_blob_storage(filename, uploaded_file):
    # read file content
    file_contents = uploaded_file.read()
    
    processed_filename = filename.replace(" ", "_")
    # create BlobClient
    blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME,
                                                      blob=processed_filename)

    container_client = blob_service_client.get_container_client(container=CONTAINER_NAME)
    blob_list = container_client.list_blobs()
    # Delete only files with .xlsx extension before uploading a new file
    for blob in blob_list:
        if blob.name.endswith('.xlsx'):
            container_client.delete_blob(blob.name)

    # Upload file on Azure Blob Storage
    blob_client.upload_blob(file_contents, overwrite=True)
    # time.sleep(10)  # wait 60s - cheating
    # st.success(f"File '{filename}' uploaded successfully to container!")
    
    # list all files in container
    blob_list = container_client.list_blobs()
    for blob in blob_list:
        print(blob.name)


def processing_uploaded_file(filename: str = None):
    bus_calender_file, excel_filename = get_xlsx_file(filename)
    print(f"Generating StartingTime.csv from {excel_filename}...")
    generate_starting_time(bus_calender_file, "StartingTime.csv")
    print(f"Generating bus_trips.csv from {excel_filename}...")
    generate_bus_trips(bus_calender_file, "bus_trips.csv")
    print(f"Generating bus_schedule.csv from {excel_filename}...")
    generate_bus_schedule(bus_calender_file, "bus_schedule.csv")
    print(f"Generating bus_timetable.csv from {excel_filename}...")
    generate_bus_timetable(bus_calender_file, "bus_timetable.csv")
    print("All files have been generated successfully!")


def update_bus_schedule_database():
    host = os.getenv("PG_VECTOR_HOST")
    user = os.getenv("PG_VECTOR_USER")
    password = os.getenv("PG_VECTOR_PASSWORD")
    database = os.getenv("PGDATABASE2")
    
    # First, removing all data from the table
    engine = create_engine(f'postgresql+psycopg://{user}:{password}@{host}:5432/{database}')
    Session = sessionmaker(bind=engine)
    session = Session()
    
    # Create a MetaData instance
    metadata = MetaData()
    # Reflect the tables
    metadata.reflect(bind=engine)
    try:
        # Truncate each table in reverse order to handle dependencies
        for table in reversed(metadata.sorted_tables):
            print(f"{table.name=}")
            session.execute(text(f'TRUNCATE TABLE {table.name} CASCADE'))
            print(f'Truncated table {table.name}')
        session.commit()
    finally:
        session.close()
    
    # Định nghĩa các bảng
    # bus_trips = Table('bus_trips', metadata,
    #                 Column('trip_id', Integer, primary_key=True),
    #                 Column('route', Text, nullable=False),
    #                 Column('departure_district', Text, nullable=False),
    #                 Column('arrival', Text, nullable=False),
    #                 Column('departure_time', DateTime, nullable=False),
    #                 Column('arrival_time', DateTime, nullable=False))

    # bus_schedule = Table('bus_schedule', metadata,
    #                     Column('trip_id', Integer, ForeignKey('bus_trips.trip_id'), primary_key=True),
    #                     Column('stop_sequence', Integer, primary_key=True),
    #                     Column('stop_name', Text),
    #                     Column('stop_time', DateTime))

    # bus_timetable = Table('bus_timetable', metadata,
    #                     Column('day_of_week', Text, primary_key=True),
    #                     Column('trip_id', Integer, ForeignKey('bus_trips.trip_id'), primary_key=True),
    #                     Column('date', DateTime))
    
    # metadata.drop_all(engine)

    # Tạo các bảng trong cơ sở dữ liệu
    metadata.create_all(engine)
    
    bus_schedule_df = get_csv_file("bus_schedule.csv")
    bus_timetable_df = get_csv_file("bus_timetable.csv")
    bus_trips_df = get_csv_file("bus_trips.csv")
    
    # Đổi tên các cột để khớp với tên cột trong cơ sở dữ liệu
    bus_schedule_df.columns = ['trip_id', 'stop_sequence', 'stop_name', 'stop_time']
    bus_timetable_df.columns = ['day_of_week', 'trip_id', 'date']
    bus_trips_df.columns = ['trip_id', 'route', 'departure_district', 'arrival', 'departure_time', 'arrival_time']
    
    # Đảm bảo các cột có định dạng đúng
    bus_schedule_df['stop_time'] = pd.to_datetime(bus_schedule_df['stop_time'], format="%H:%M")
    # bus_timetable_df['date'] = pd.to_datetime(bus_timetable_df['date'], format='%d/%m/%Y')
    bus_timetable_df['date'] = pd.to_datetime(bus_timetable_df['date'], format='%m/%d/%Y')
    bus_trips_df['departure_time'] = pd.to_datetime(bus_trips_df['departure_time'], format="%H:%M")
    bus_trips_df['arrival_time'] = pd.to_datetime(bus_trips_df['arrival_time'], format="%H:%M")
    
    # Tải dữ liệu lên PostgreSQL
    bus_trips_df.to_sql('bus_trips', engine, if_exists='append', index=False)
    bus_schedule_df.to_sql('bus_schedule', engine, if_exists='append', index=False)
    bus_timetable_df.to_sql('bus_timetable', engine, if_exists='append', index=False)

    print("Dữ liệu đã được tải lên PostgreSQL thành công!")


def get_xlsx_file(filename: str = "StartingTime.xlsx"):
    '''
    Load the bus schedule Excel file from Blob Storage
    '''
    # Create a BlobServiceClient object
    blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)

    # Get a client to interact with the container and the specific blob
    blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=filename)

    # Download the blob as a string
    # blob_data = blob_client.download_blob().readall()
    blob_data = blob_client.download_blob().read()

    # Read the Excel file into a pandas ExcelFile object
    excel_file = pd.ExcelFile(BytesIO(blob_data))
    
    blob_list = blob_service_client.get_container_client(CONTAINER_NAME).list_blobs()
    # Iterate through the blobs to find the xlsx file
    for blob in blob_list:
        if blob.name.endswith('.xlsx'):
            xlsx_file_name = blob.name
            break  # Stop the loop once the xlsx file is found
        
    return excel_file, xlsx_file_name


def get_csv_file(filename: str = "bus_trips.csv"):
    '''
    Get bus_trips.csv or bus_schedule.csv or StartingTime.csv file from Blob Storage
    '''
    
    # Create a BlobServiceClient object
    blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)

    # Get a client to interact with the container and the specific blob
    blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=filename)

    container_client = blob_service_client.get_container_client(CONTAINER_NAME)

    blob_client = container_client.get_blob_client(filename)

    df = pd.read_csv(blob_client.download_blob())
    return df


def generate_starting_time(bus_calender_file: pd.ExcelFile, 
                           export_path: str = "StartingTime.csv") -> None:
    # Get the sheet names
    # sheet_names = bus_calender_file.sheet_names
    
    sheet_names = ('Hai Ba Trung', 'Cau Giay', 'Tay Ho', 'Ha Dong', 'Ecopark')
    dfs = {}
    for sheet_name in sheet_names:
        dfs[sheet_name] = pd.read_excel(bus_calender_file, sheet_name=sheet_name)

    # Print the names of the loaded sheets
    print("Loaded sheets:", list(dfs.keys()))
    
    # Generate StartingTime.csv file
    df = pd.DataFrame(columns=['route_name', 
                               'pickup_point', 
                               'dropoff_point', 
                               'date', 
                               'slot1', 
                               'slot2', 
                               'slot3', 
                               'slot4', 
                               'slot5', 
                               'slot6'])

    # Iterate over the dictionary of dataframes
    for route_name, route_df in dfs.items():
        # Iterate over the rows of the dataframe
        for i in range(len(route_df)):
            if i < 3:
                continue
            
            pickup_point = route_df.iloc[i][0]
            for j in range(i + 1, len(route_df)):
                # Get the dropoff points
                dropoff_point = route_df.iloc[j][0]
                # Define the common columns
                new_row_data = {
                    'route_name': route_name.title(),
                    'pickup_point': pickup_point.title(),
                    'dropoff_point': dropoff_point.title(),
                    'date': None
                }
                
                # Iterate over the slots
                for k in range(1, 6):
                    # Get the slot value
                    slot = route_df.iloc[i][k].strftime("%H:%M")

                    # Add all slot columns to the dictionary
                    new_row_data[f'slot{k}'] = slot  # Assign slot value to the correct slot column

                # Create a new DataFrame with the new row
                new_row = pd.DataFrame([new_row_data])

                # Concatenate the new DataFrame with the existing DataFrame
                df = pd.concat([df, new_row], ignore_index=True)
        
        # Iterate over the rows of the dataframe
        for i in range(len(route_df)):
            if i < 3:
                continue
            
            pickup_point = route_df.iloc[i][7]
            for j in range(i + 1, len(route_df)):
                # Get the dropoff points
                dropoff_point = route_df.iloc[j][7]
                
                # Define the common columns
                new_row_data = {
                    'route_name': route_name.title(),
                    'pickup_point': pickup_point.title(),
                    'dropoff_point': dropoff_point.title(),
                    'date': None
                }
                
                # Iterate over the slots
                for k in range(1, 6):
                    # Get the slot value
                    slot = route_df.iloc[i][k + 7].strftime("%H:%M")

                    # Add all slot columns to the dictionary
                    new_row_data[f'slot{k}'] = slot  # Assign slot value to the correct slot column

                # Create a new DataFrame with the new row
                new_row = pd.DataFrame([new_row_data])

                # Concatenate the new DataFrame with the existing DataFrame
                df = pd.concat([df, new_row], ignore_index=True)
    
    
    df.to_csv(export_path, index=False)
    blob_block = ContainerClient.from_connection_string(
        conn_str=CONNECTION_STRING,
        container_name=CONTAINER_NAME
        )
    output = StringIO()
    output = df.to_csv(index=False, encoding='utf-8')
    blob_block.upload_blob(export_path, output, overwrite=True, encoding='utf-8')
    print(f"{export_path} file has been generated and uploaded to Blob Storage {CONTAINER_NAME}.")
    

def generate_bus_trips(bus_calender_file: pd.ExcelFile, 
                        export_path: str = "bus_trips.csv"):
    
    # sheet_names = bus_calender_file.sheet_names
    sheet_names = ('Hai Ba Trung', 'Cau Giay', 'Tay Ho', 'Ha Dong', 'Ecopark')
    
    dfs = {}
    for sheet_name in sheet_names:
        dfs[sheet_name] = pd.read_excel(bus_calender_file, sheet_name=sheet_name)
    print("Loaded sheets:", list(dfs.keys()))

    df = pd.DataFrame(columns=['trip_id', 'route', 'departure_district', 'arrival', 'departure_time', 'arrival_time'])

    new_row_data_list = []
    trip_id = 1
    for route_name, route_df in dfs.items():
        # Interate over the columns of route_df
        for col_idx in range(1, 6):
            formatted_route_name = route_name.title() + " to BUV Campus"
            departure_district = route_name.title()
            arrival = "BUV Campus"
            departure_time = route_df.iloc[3][col_idx].strftime("%H:%M")
            arrival_time = route_df.iloc[-1][col_idx].strftime("%H:%M")
            new_row_data = {
                            'trip_id': trip_id,
                            'route': formatted_route_name,
                            'departure_district': departure_district,
                            'arrival': arrival,
                            'departure_time': departure_time,
                            'arrival_time': arrival_time
                        }
            new_row_data_list.append(new_row_data)
            trip_id += 1
            
        new_data = pd.DataFrame(new_row_data_list)

    for route_name, route_df in dfs.items():
        # Interate over the columns of route_df
        for col_idx in range(1, 7):
            formatted_route_name = "BUV Campus to " + route_name.title()
            departure_district = "BUV Campus"
            arrival = route_name.title()
            departure_time = route_df.iloc[3][col_idx + 7].strftime("%H:%M")
            arrival_time = route_df.iloc[-1][col_idx + 7].strftime("%H:%M")
            new_row_data = {
                            'trip_id': trip_id,
                            'route': formatted_route_name,
                            'departure_district': departure_district,
                            'arrival': arrival,
                            'departure_time': departure_time,
                            'arrival_time': arrival_time
                        }
            new_row_data_list.append(new_row_data)
            trip_id += 1
            
        new_data = pd.DataFrame(new_row_data_list)
        
    df = pd.concat([df, new_data], ignore_index=True)

    # df.to_csv(export_path, index=False)
    blob_block = ContainerClient.from_connection_string(
        conn_str=CONNECTION_STRING,
        container_name=CONTAINER_NAME
        )
    output = StringIO()
    output = df.to_csv(index=False, encoding='utf-8')
    blob_block.upload_blob(export_path, output, overwrite=True, encoding='utf-8')
    print(f"{export_path} file has been generated and uploaded to Blob Storage {CONTAINER_NAME}.")


def generate_bus_schedule(bus_calender_file: pd.ExcelFile,
                          export_path: str = "bus_schedule.csv"):
    bus_trips = get_csv_file("bus_trips.csv")
    
    df = pd.DataFrame(columns=['trip_id', 'stop_sequence', 'stop_name', 'stop_time'])

    # Iterate over the trip_id
    for trip_id in bus_trips['trip_id']:
        stop_sequence = 1
        
        # Get the departure_district
        departure_district = bus_trips[bus_trips['trip_id'] == trip_id]['departure_district'].values[0]
        departure_time = bus_trips[bus_trips['trip_id'] == trip_id]['departure_time'].values[0]
        
        if departure_district != "BUV Campus":
            sheet_name = departure_district
            stops = pd.read_excel(bus_calender_file, sheet_name=sheet_name)        
            times_str = [time.strftime("%H:%M") for time in stops.iloc[3][1:6].values]
            
            new_row_data_list = []
            for i in range(len(stops)):
                if i < 3:
                    continue
                col_idx = times_str.index(departure_time) + 1
                stop_time = stops.iloc[i][col_idx].strftime("%H:%M")
                
                stop_name = stops.iloc[i][0]
                new_row_data = {
                                'trip_id': trip_id,
                                'stop_sequence': stop_sequence,
                                'stop_name': stop_name.title(),
                                'stop_time': stop_time,
                            }
                new_row_data_list.append(new_row_data)
                stop_sequence += 1
            new_data = pd.DataFrame(new_row_data_list)
        else:
            arrival = bus_trips[bus_trips['trip_id'] == trip_id]['arrival'].values[0]
            sheet_name = arrival
            stops = pd.read_excel(bus_calender_file, sheet_name=sheet_name)
            
            times_str = [time.strftime("%H:%M") for time in stops.iloc[3][8:14].values]
            new_row_data_list = []
            for i in range(len(stops)):
                if i < 3:
                    continue
                col_idx = times_str.index(departure_time) + 8
                stop_time = stops.iloc[i][col_idx].strftime("%H:%M")
                
                stop_name = stops.iloc[i][7]
                new_row_data = {
                                'trip_id': trip_id,
                                'stop_sequence': stop_sequence,
                                'stop_name': stop_name.title(),
                                'stop_time': stop_time,
                            }
                new_row_data_list.append(new_row_data)
                stop_sequence += 1
            new_data = pd.DataFrame(new_row_data_list)
        
        df = pd.concat([df, new_data], ignore_index=True)
        # Get the data from row 3 onwards, 

    # df.to_csv(export_path, index=False)
    blob_block = ContainerClient.from_connection_string(
        conn_str=CONNECTION_STRING,
        container_name=CONTAINER_NAME
        )
    output = StringIO()
    output = df.to_csv(index=False, encoding='utf-8')
    blob_block.upload_blob(export_path, output, overwrite=True, encoding='utf-8')
    print(f"{export_path} file has been generated and uploaded to Blob Storage {CONTAINER_NAME}.")
    
    
def generate_bus_timetable(bus_calender_file: pd.ExcelFile,
                           export_path: str = "bus_timetable.csv"):
    
    day_abbv_to_full = {
        'Mon': 'Monday',
        'Tue': 'Tuesday',
        'Wed': 'Wednesday',
        'Thu': 'Thursday',
        'Fri': 'Friday',
        'Sat': 'Saturday',
        'Sun': 'Sunday'
    }


    route_names = ('Hai Ba Trung', 'Cau Giay', 'Tay Ho', 'Ha Dong', 'Ecopark')

    calendar = pd.read_excel(bus_calender_file, sheet_name=bus_calender_file.sheet_names[-1])
    
    df = pd.DataFrame(columns=['day_of_week', 'trip_id', 'date'])


    trip_id = 1
    initial_trip_id = trip_id
    for i in range(len(calendar)):
        if i < 9:
            continue
        
        day_of_week = calendar.iloc[i][1]
        if i + 1 < len(calendar):
            next_day_of_week = calendar.iloc[i + 1][1]
        else:
            next_day_of_week = " "
            
        for j in range(3, 8):
            has_trip = int(calendar.iloc[i][j])
            formatted_date = pd.to_datetime(calendar.iloc[i][0]).strftime("%m/%d/%Y")
            # formatted_date = pd.to_datetime(calendar.iloc[i][0]).strftime("%d/%m/%Y")
            if has_trip == 1:
                new_row_data = {
                    'day_of_week': day_abbv_to_full[day_of_week],
                    'trip_id': trip_id,
                    'date': formatted_date,
                }
                new_row = pd.DataFrame([new_row_data])
                df = pd.concat([df, new_row], ignore_index=True)
            
            trip_id += 1
        
        if next_day_of_week != day_of_week:
            ecopark_departure_mask = calendar[(calendar.iloc[:, 1] == day_of_week) & (calendar.iloc[:, 2] == 'HBT')].iloc[:, 3:8].values[0]
            for j in range(3, 8):
                formatted_date = pd.to_datetime(calendar.iloc[i][0]).strftime("%m/%d/%Y")
                # formatted_date = pd.to_datetime(calendar.iloc[i][0]).strftime("%d/%m/%Y")
                if ecopark_departure_mask[j - 3] == 1:
                    new_row_data = {
                        'day_of_week': day_abbv_to_full[day_of_week],
                        'trip_id': trip_id,
                        'date': formatted_date,
                    }
                    new_row = pd.DataFrame([new_row_data])
                    df = pd.concat([df, new_row], ignore_index=True)
                    
                trip_id += 1
            
            trip_id = initial_trip_id

    trip_id = df['trip_id'].values[-1] + 1
    initial_trip_id = trip_id
    for i in range(len(calendar)):
        if i < 9:
            continue
        
        day_of_week = calendar.iloc[i][1]
        if i + 1 < len(calendar):
            next_day_of_week = calendar.iloc[i + 1][1]
        else:
            next_day_of_week = " "
            
        for j in range(8, 14):
            has_trip = int(calendar.iloc[i][j])
            formatted_date = pd.to_datetime(calendar.iloc[i][0]).strftime("%m/%d/%Y")
            # formatted_date = pd.to_datetime(calendar.iloc[i][0]).strftime("%d/%m/%Y")
            if has_trip == 1:
                new_row_data = {
                    'day_of_week': day_abbv_to_full[day_of_week],
                    'trip_id': trip_id,
                    'date': formatted_date,
                }
                new_row = pd.DataFrame([new_row_data])
                df = pd.concat([df, new_row], ignore_index=True)
            
            trip_id += 1
        
        if next_day_of_week != day_of_week:
            ecopark_arrival_mask = calendar[(calendar.iloc[:, 1] == day_of_week) & (calendar.iloc[:, 2] == 'HBT')].iloc[:, 8:14].values[0]
            for j in range(8, 14):
                formatted_date = pd.to_datetime(calendar.iloc[i][0]).strftime("%m/%d/%Y")
                # formatted_date = pd.to_datetime(calendar.iloc[i][0]).strftime("%d/%m/%Y")
                if ecopark_arrival_mask[j - 8] == 1:
                    new_row_data = {
                        'day_of_week': day_abbv_to_full[day_of_week],
                        'trip_id': trip_id,
                        'date': formatted_date,
                    }
                    new_row = pd.DataFrame([new_row_data])
                    df = pd.concat([df, new_row], ignore_index=True)
                    
                trip_id += 1
            # trip_id = 1
            trip_id = initial_trip_id
                
    # df.to_csv(export_path, index=False)
    blob_block = ContainerClient.from_connection_string(
        conn_str=CONNECTION_STRING,
        container_name=CONTAINER_NAME
        )
    output = StringIO()
    output = df.to_csv(index=False, encoding='utf-8')
    blob_block.upload_blob(export_path, output, overwrite=True, encoding='utf-8')
    print(f"{export_path} file has been generated and uploaded to Blob Storage {CONTAINER_NAME}.")



