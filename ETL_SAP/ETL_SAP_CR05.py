# Author: Omar Sabag
# Date Created: 2023-01-01
# Description: This script automates the extraction of data from SAP using the CR05 transaction code and concatenates the extracted data into a single Excel file. The extracted data is then uploaded to a SQL database table using a custom class for SQL operations.

import pandas as pd
import win32com.client
import subprocess
import time
from sabagclass import SqlConn, SqlUpload
import glob
import os

def sap():
    try:

        # Start the SAP GUI from the local file system
        subprocess.Popen(r"C:\Program Files (x86)\SAP\FrontEnd\SAPgui\saplogon.exe")
        # Wait for SAP GUI to launch
        time.sleep(2)

        # Connect to the SAP GUI automation object
        SapGuiAuto = win32com.client.GetObject("SAPGUI")
        if not type(SapGuiAuto) == win32com.client.CDispatch:
            return

        # Get the scripting engine of the SAP GUI
        application = SapGuiAuto.GetScriptingEngine
        if not type(application) == win32com.client.CDispatch:
            SapGuiAuto = None
            return

        # Open a connection to a pre-defined SAP server
        connection = application.OpenConnection("' PR2 - Prod. Planning/Quality - Automatic Logon", True)
        if not type(connection) == win32com.client.CDispatch:
            application = None
            SapGuiAuto = None
            return

        # Start a session from the established connection
        session = connection.Children(0)
        if not type(session) == win32com.client.CDispatch:
            connection = None
            application = None
            SapGuiAuto = None
            return

        # Handle multiple login screens if present
        if session.Children.Count > 1:
            session.findById("wnd[1]/usr/radMULTI_LOGON_OPT2").Select()
            session.findById("wnd[1]/tbar[0]/btn[0]").Press()
        else:
            # Execute the MB51 transaction code
            print('starting SAP')

        #######################################
        # EXAMPLE SCRIPT START PASTE YOUR OWN SAP SCRIPT HERE
        #######################################

        lista_plantas = ['PLANT1', 'PLANT2', 'PLANT3']
        print("starting SAP script")

        for planta in lista_plantas:
            session.findById("wnd[0]/tbar[0]/okcd").text = "/ncr05"
            session.findById("wnd[0]").sendVKey(0)
            session.findById("wnd[0]/usr/ctxtCR_WERKS-LOW").text = planta
            session.findById("wnd[0]").sendVKey(8)
            session.findById("wnd[0]/mbar/menu[0]/menu[1]/menu[2]").select()
            session.findById("wnd[1]/tbar[0]/btn[0]").press()
            session.findById("wnd[1]/usr/ctxtDY_PATH").text = r"YOUR_PATH"
            session.findById("wnd[1]/usr/ctxtDY_FILENAME").text = f"cr05_{planta}.txt"
            session.findById("wnd[1]/usr/ctxtDY_FILENAME").caretPosition = 8
            session.findById("wnd[1]/tbar[0]/btn[11]").press()

        # CLOSE SESION
        session.findById("wnd[0]").Close()
        session.findById("wnd[1]/usr/btnSPOP-OPTION1").press()

        #############################################
        # EXAMPLE SCRIPT END PASTE YOUR OWN SAP SCRIPT HERE
        ##############################################


    except Exception as e:
    # Handle any exceptions that occur during the extraction process
    # (Error handling code would be here)
    finally:


# Clean up the COM objects to ensure a proper closure
# (Resource deallocation code would be here)

if __name__ == "__main__":
    sap()


def concatenate_csv_files():
    """
    Concatenates all CSV files located in the directory 'cr05_files' into a single DataFrame.

    Reads each CSV file in the directory, strips the column names of leading and trailing spaces, and then concatenates them into one DataFrame. This assumes that all CSV files have the same column structure.

    Returns:
        DataFrame: A single DataFrame containing all the data from the CSV files.
    """

    # Get the current working directory
    path = os.getcwd()

    # Prepare the path for csv files in 'cr05_files' directory
    csv_files_path = glob.glob(os.path.join(path, 'cr05_files', "*.csv"))

    # Initialize an empty DataFrame
    df = pd.DataFrame()

    # Loop over the list of csv files and concatenate them
    for f in csv_files_path:
        df_temp = pd.read_csv(f)
        print(f)  # Optionally print the file path for each file processed

        # Strip all column names to avoid spaces between them
        df_temp = df_temp.rename(columns=lambda x: x.strip())

        # Concatenate the current dataframe to the main dataframe
        df = pd.concat([df, df_temp], axis=0, ignore_index=True)

    return df
# Call the function and store the result in a variable
csv_files = concatenate_csv_files()



def sqlupload():
    try:
        # Import custom classes for SQL operations
        from sabagclass import SqlUpload

        # Create an instance of the SqlUpload class
        sql_upload = SqlUpload()

        # Upload the DataFrame to the SQL table
        sql_upload.upload_dataframe(df,
                                    table_name='ALL_ZSD3TR008N',
                                    chunksize=5000,
                                    index=False,
                                    if_exists='append')

    except Exception as e:
        # Print the exception message
        print(e)
        print('Failed to upload to SQL')
        # Exit the script in case of an error
        exit()
# Execute the sqlupload function
sqlupload()