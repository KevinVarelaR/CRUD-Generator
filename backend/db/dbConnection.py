import psycopg2
import pyodbc

def connectToDatabase(engine, host, user, password, database):
    """
    Establishes a connection to a PostgreSQL or MSSQL database based on the given engine.

    Parameters:
        engine (str): Database engine type ('PostgreSQL' or 'MSSQL')
        host (str): Host address of the database server
        user (str): Username for authentication
        password (str): Password for authentication
        database (str): Name of the database to connect to

    Returns:
        A database connection object

    Raises:
        ValueError: If the database engine is not supported
    """
    if engine == "PostgreSQL":
        return psycopg2.connect(
            dbname=database,
            user=user,
            password=password,
            host=host,
            port=5432
        )
    elif engine == "MSSQL":
        connectionString = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={host};DATABASE={database};UID={user};PWD={password}"
        )
        return pyodbc.connect(connectionString)
    else:
        raise ValueError("Unsupported database engine")
