# :books: `sabag_class_port.py` Project Documentation

##  :mag_right: Overview

The `sabaglibraries` module is a collection of custom-built Python classes and functions designed to streamline database connections and data orchestration across various scripts. It simplifies connections to different data sources, orchestrates data flow monitoring the execution of scripts from start to finish.


## :books: Python Libraries Used

- `sqlalchemy`: Provides a full suite of well-known enterprise-level persistence patterns.
- `pyodbc`, `psycopg2`: Used for connecting to SQL databases.
- `pandas`: Offers data structures and operations for manipulating numerical tables and time series.
- `numpy`: Adds support for large, multi-dimensional arrays and matrices.
- `datetime`: Supplies classes for manipulating dates and times.
- `dotenv`: Loads environment variables from a `.env` file.

##  :link: SQL Connections

- SQL Server
- AWS Redshift

## :classical_building: CLASSES

>### :computer: `SqlConn`
>   - **Purpose**: Establishes a connection to Microsoft SQL Server databases.
>   - **Returns**: An instance of a SQL connection engine.

>### :arrow_up: `SqlUpload`
>   - **Purpose**: Manages data uploads to Microsoft SQL Server databases, with functionality for batch processing and error handling.
>   - **Returns**: Nothing directly, but uploads data to the specified SQL Server database table.

>### :cloud: `SqlUploadRedshift`
>   - **Purpose**: Facilitates direct data uploads to AWS Redshift using the psycopg2 library for efficient bulk inserts.
>   - **Returns**: Nothing directly, but performs data upload to the specified Redshift table.

>   ### :globe_with_meridians: `SqlUploadAWS`
>   - **Purpose**: Handles data uploads to AWS Redshift with methods for establishing connections using SQLAlchemy.
>   - **Returns**: Nothing directly, but facilitates data upload to AWS Redshift.

>### :link: `SqlConnEdge`
>   - **Purpose**: Creates connections specifically to Edge Redshift, utilizing ODBC connections.
>   - **Returns**: A database connection object.

> ### :heavy_check_mark: `CheckIfRunV2`
> - **Purpose**: Enhances the `CheckIfRun` functionality with additional validation for script dependencies within workflows.
> - **Returns**: A boolean indicating if the dependent script can proceed based on the successful execution of its prerequisite script(s).

> ### :bar_chart: `SqlUtils`
> - **Purpose**: for Data Orchestration, monitors and logs script execution details, handling start, finish, and error records for each script in production.
>- **Returns**: Logs script execution details to the designated SQL tables and provides utilities for error handling.


##  :memo: NOTES:

- Ensure all `.env` variables are set before running scripts for database connections.
- Securely store sensitive information and credentials.
- Handle database connections within context managers to ensure proper closure.

## :toolbox:  Dependencies

To install all dependencies required for `sabaglibraries`, run the following command:
```bash
pip install sqlalchemy pyodbc psycopg2-binary pandas numpy python-dotenv
```
## :electric_plug: Example How to Call It inside a Python Script

```python
from sabaglibraries import SqlConn, SqlUpload, CheckIfRunV2, SqlUtils

# Creating a new SQL Server connection
sql_conn = SqlConn()
conn = sql_conn.get_conn()



```

