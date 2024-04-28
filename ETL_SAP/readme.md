# :file_folder: SAPtoSQL Scripts Documentation

## :mag: Overview
The `SAPtoSQL` folder contains a suite of Python scripts that form the backbone of our data extraction from SAP and loading pipeline. Each script is designed to perform a specific Extract, Transform, and Load (ETL) task, interacting with SAP systems to extract data, clean and transform it for enhanced information content, and finally load it into our SQL Server databases for further analysis and reporting.

### :gear: ETL Process Breakdown
The ETL process is broken down into three main functions:

1. **SAP Extract**: Automated scripts connect to SAP and extract data based on specific requirements.
2. **Clean (Transformation)**: Extracted data is then cleaned, validated, and transformed to fit the target schema in the SQL database.
3. **Upload to SQL Server**: Clean and transformed data is loaded into the SQL Server database, making it available for applications.

## :microscope: Sample Script Functionality: 
Below is a high-level description of a sample function within the `cr05` script, demonstrating the SAP extract phase:

```python
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
        #EXAMPLE SCRIPT START PASTE YOUR OWN SAP SCRIPT HERE
        #######################################
        
        lista_plantas = ['PLANT1','PLANT2','PLANT3']
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



        #CLOSE SESION
        session.findById("wnd[0]").Close()
        session.findById("wnd[1]/usr/btnSPOP-OPTION1").press()
   
        #############################################
        #EXAMPLE SCRIPT END PASTE YOUR OWN SAP SCRIPT HERE
        ##############################################
        
        
    except Exception as e:
        # Handle any exceptions that occur during the extraction process
        # (Error handling code would be here)
    finally:
        # Clean up the COM objects to ensure a proper closure
        # (Resource deallocation code would be here)

if __name__ == "__main__":
    sap()

```
## :broom: Data Cleaning and Consolidation Function

As part of the data transformation process, the cleanup function is tasked with merging multiple Excel files into a single cohesive DataFrame. This step is essential for creating a unified dataset from various sources, simplifying the subsequent analysis and loading phases.

### :page_with_curl: Function Overview

The cleanup function performs the following operations:

1. **Identification of Files**: It begins by identifying all Excel files within a specified directory, which were previously downloaded during the extract phase.
2. **Excel File Reading**: Each Excel file is read into a separate DataFrame. Pandas is used for this purpose because of its powerful data manipulation capabilities.
3. **Concatenation**: The individual DataFrames are concatenated into one larger DataFrame. Concatenation is done row-wise (`axis=0`), which means that data from each file is added as new rows to the cumulative DataFrame.
4. **Index Resetting**: The `ignore_index=True` parameter is set to ensure that the resulting DataFrame has a continuous index without any repetition of index values from the source files.

### :gear: Detailed Explanation of the Code Block

```python

def concatenate_excel_files(csv_files):
    """
    Concatenates multiple Excel files into a single DataFrame.

    This function iterates over a list of Excel file paths, reads each file into a temporary DataFrame, and then concatenates
    all of them into one main DataFrame. The concatenation is row-wise, which means that the data from each file is added
    below the previous file's data.

    Parameters:
    csv_files (list of str): A list of file paths for the Excel files to be concatenated.

    Returns:
    DataFrame: A DataFrame containing all data from the concatenated Excel files.
    """
    # Define an empty DataFrame to store the concatenated data
    df = pd.DataFrame()

    # Loop over each file path in the list of Excel files
    for f in csv_files:
        # Read the current Excel file into a temporary DataFrame
        df1 = pd.read_excel(f, sheet_name=0)
        print(f"Concatenating file: {f}")  # Optionally print the file path for confirmation

        # Concatenate the temporary DataFrame (`df1`) with the main DataFrame (`df`)
        df = pd.concat([df, df1], axis=0, ignore_index=True)
    
    return df
```


## :arrow_up: SQL Data Upload Function

The final stage in the ETL process involves uploading the transformed data into a SQL database for persistent storage and easy access for querying and reporting. The `sqlupload` function handles this task using custom classes designed to facilitate database connections and operations.

### :page_facing_up: Function Overview

This function accomplishes the following:

1. **Initiation of SQL Connection**: It employs a custom utility class, likely `SqlUpload`, which abstracts the database connection and upload functionalities.
2. **Dataframe Upload**: The function takes a pandas DataFrame (`df`), which contains the data to be uploaded, and inserts it into a specified table within the SQL database.
3. **Batch Processing**: The data is uploaded in chunks to optimize memory usage and improve performance, particularly important when dealing with large datasets.
4. **Handling of Duplicates**: The `if_exists` parameter is set to 'append', ensuring that new data is added to the existing table without overwriting current content.

### :wrench: Detailed Explanation of the Code Block

```python
def sqlupload():
    try:
        # Import custom classes for SQL operations
        from sabagclass import SqlUpload

        # Create an instance of the SqlUpload class
        sql_upload = SqlUpload()
        
        # Upload the DataFrame to the SQL table
        sql_upload.upload_dataframe(df,
                                    table_name='cr05_sap',
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