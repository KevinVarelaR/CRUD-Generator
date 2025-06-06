import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from backend.db.dbConnection import connectToDatabase

def getSchemas(engine, host, user, password, database):
    """
    Retrieve a list of non-system schemas from the database.

    Parameters:
        engine (str): 'PostgreSQL' or 'MSSQL'
        host (str): Database host
        user (str): Username
        password (str): Password
        database (str): Database name

    Returns:
        List[str]: List of schema names
    """
    schemas = []
    try:
        conn = connectToDatabase(engine, host, user, password, database)
        with conn.cursor() as cur:
            if engine == "PostgreSQL":
                cur.execute("""
                    SELECT schema_name
                    FROM information_schema.schemata
                    WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                    ORDER BY schema_name;
                """)
            elif engine == "MSSQL":
                cur.execute("""
                    SELECT DISTINCT s.name
                    FROM sys.schemas s
                    INNER JOIN sys.tables t ON s.schema_id = t.schema_id
                    WHERE s.name NOT IN ('guest', 'INFORMATION_SCHEMA', 'sys', 'db_owner', 'db_accessadmin', 'db_securityadmin')
                    ORDER BY s.name;
                """)
            schemas = [row[0] for row in cur.fetchall()]
    except Exception as e:
        print("Error retrieving schemas:", e)
    return schemas


def getTables(engine, host, user, password, database, schema):
    """
    Retrieve a list of tables from the specified schema.

    Parameters:
        engine (str): Database engine
        host (str): Database host
        user (str): Username
        password (str): Password
        database (str): Database name
        schema (str): Target schema name

    Returns:
        List[str]: List of table names
    """
    tables = []
    try:
        conn = connectToDatabase(engine, host, user, password, database)
        with conn.cursor() as cur:
            if engine == "PostgreSQL":
                cur.execute(f"""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = '{schema}'
                    AND table_type = 'BASE TABLE';
                """)
            elif engine == "MSSQL":
                cur.execute(f"""
                    SELECT TABLE_NAME
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_TYPE = 'BASE TABLE'
                    AND TABLE_SCHEMA = '{schema}';
                """)
            tables = [row[0] for row in cur.fetchall()]
    except Exception as e:
        print("Error retrieving tables:", e)
    return tables


def getColumns(engine, host, user, password, database, schema, table):
    """
    Retrieve the columns and data types for a given table.

    Parameters:
        engine (str): Database engine
        host (str): Database host
        user (str): Username
        password (str): Password
        database (str): Database name
        schema (str): Schema name
        table (str): Table name

    Returns:
        List[Tuple[str, str]]: List of (column_name, data_type) tuples
    """
    columns = []
    try:
        conn = connectToDatabase(engine, host, user, password, database)
        with conn.cursor() as cur:
            if engine == "PostgreSQL":
                cur.execute(f"""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema = '{schema}'
                    AND table_name = '{table}';
                """)
            elif engine == "MSSQL":
                cur.execute(f"""
                    SELECT COLUMN_NAME, DATA_TYPE
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = '{table}'
                    AND TABLE_SCHEMA = '{schema}';
                """)
            columns = cur.fetchall()
    except Exception as e:
        print("Error retrieving columns:", e)
    return columns

def getPermissions(engine, host, user, password, dbname, schema, table):
    """
    Brief description:
        Retrieves column-level read and write permissions for a given user on a specified table,
        supporting both PostgreSQL and MSSQL engines.

    Parameters:
        engine (str): Database engine ("PostgreSQL" or "MSSQL").
        host (str): Hostname or IP address of the database server.
        user (str): Username to check permissions for.
        password (str): Password for the database connection.
        dbname (str): Name of the target database.
        schema (str): Schema where the table resides.
        table (str): Name of the target table.

    Returns:
        list[tuple]: A list of tuples containing column names and corresponding read/write flags.
                     Returns an empty list if an error occurs.
    """
    try:
        conn = connectToDatabase(engine, host, user, password, dbname)
        with conn.cursor() as cur:
            if engine == "PostgreSQL":
                cur.execute(f"""
                    SELECT column_name,
                        has_column_privilege('{user}', '{schema}.{table}', column_name, 'SELECT') AS can_read,
                        has_column_privilege('{user}', '{schema}.{table}', column_name, 'UPDATE') AS can_write
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s;
                """, (schema, table))
                return cur.fetchall()
            elif engine == "MSSQL":
                cur.execute(f"""
                    SELECT c.name AS column_name,
                        CASE WHEN perm.permission_name = 'SELECT' THEN 1 ELSE 0 END AS can_read,
                        CASE WHEN perm.permission_name = 'UPDATE' THEN 1 ELSE 0 END AS can_write
                    FROM sys.columns c
                    LEFT JOIN sys.database_permissions perm ON c.object_id = perm.major_id AND c.column_id = perm.minor_id
                    WHERE object_id = OBJECT_ID('{schema}.{table}');
                """)
                return cur.fetchall()
    except Exception as e:
        print(f"Error fetching permissions: {e}")
        return []
