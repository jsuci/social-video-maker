import sqlite3
import os
from dotenv import load_dotenv
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from io import BytesIO
from gspread_formatting import *


# Load environment variables
load_dotenv()
SHEET_ID = os.getenv('SHEET_ID')
FOLDER_MAINPHOTO_ID = os.getenv('FOLDER_MAINPHOTO_ID')



def find_file_in_drive(service, folder_id, file_name):
    """ Check if file exists in Google Drive and return its webViewLink if it does. """
    query = f"name = '{file_name}' and '{folder_id}' in parents and trashed = false"
    response = service.files().list(q=query, spaces='drive', fields='files(id, webViewLink)').execute()
    files = response.get('files', [])
    return files[0] if files else None


def upload_image_to_drive(service, folder_id, file_name, file_data):
    print(f"Checking if image for MLS {file_name} already exists in Google Drive...")
    existing_file = find_file_in_drive(service, folder_id, file_name)

    if existing_file:
        print(f"Image for MLS {file_name} already exists. Link: {existing_file['webViewLink']}")
        return existing_file['webViewLink']
    else:
        print(f"Uploading new image for MLS {file_name} to Google Drive...")
        file_metadata = {
            'name': file_name,
            'parents': [folder_id]
        }
        media = MediaIoBaseUpload(BytesIO(file_data), mimetype='image/jpeg')
        file = service.files().create(body=file_metadata, media_body=media, fields='id, webViewLink').execute()
        return file.get('webViewLink')


def execute_sql_query_and_upload_to_sheets(full_query):
    print("Starting the database and Google Sheets update process...")

    database_path = os.getenv('DATABASE_PATH')
    service_account_file = os.getenv('SERVICE_ACCOUNT_FILE')
    creds = Credentials.from_service_account_file(service_account_file, scopes=[
        'https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/spreadsheets'
    ])

    drive_service = build('drive', 'v3', credentials=creds)
    sheets_service = gspread.authorize(creds)

    connection = sqlite3.connect(database_path)
    cursor = connection.cursor()

    print("Executing SQL query...")
    try:
        cursor.execute(full_query)
        rows = cursor.fetchall()
        print(f"Retrieved {len(rows)} rows from the database.")

        # Retrieve column names from cursor.description
        columns = {description[0]: idx for idx, description in enumerate(cursor.description)}
        column_names = [description[0] for description in cursor.description]
        mls_idx = columns['mls']
        mainphoto_idx = columns['mainphoto']

        data = []
        folder_id = FOLDER_MAINPHOTO_ID  # Ensure this is set in your .env file

        for row in rows:
            mls, main_photo = row[mls_idx], row[mainphoto_idx]
            file_link = upload_image_to_drive(drive_service, folder_id, f"{mls}.jpg", main_photo)
            row_data = list(row)
            row_data[mainphoto_idx] = file_link  # Replace the BLOB with the Google Drive link
            data.append(row_data)

        sheet = sheets_service.open_by_key(SHEET_ID).sheet1

        # Get all current values to find where to start appending
        all_values = sheet.get_all_values()
        start_row = len(all_values) # Determine where to start appending

        print("Updating Google Sheets with data...")
        if start_row == 1:  # If the sheet is completely empty
            print("Adding column names to Google Sheets...")
            sheet.update([column_names], 'A1')
            # Calculate the column range dynamically based on the number of columns
            last_column_letter = gspread.utils.rowcol_to_a1(1, len(column_names))[0:-1]
            fmt = CellFormat(textFormat=TextFormat(bold=True))  # Define the format: bold
            format_cell_range(sheet, f'A1:{last_column_letter}1', fmt)  # Apply the format to the entire first row


        # Update the sheet with new data
        sheet.update(data, f'A{start_row + 1}')  # Update the entire data starting from the calculated start_row
        print("Data and images uploaded successfully to Google Sheets.")
    except Exception as e:
        print("An error occurred:", e)
    finally:
        connection.close()
        print("Database connection closed.")

# Example usage
query = "SELECT * FROM property_listings WHERE mls = '23-749'"
execute_sql_query_and_upload_to_sheets(query)

# Query to select all active properties
# status_query = "SELECT * FROM property_listings WHERE status = 'Active'"
# execute_sql_query_and_upload_to_sheets(status_query)
