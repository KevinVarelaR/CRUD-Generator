"""
This module contains functions to generate SQL stored procedures
for CRUD operations (Create, Read, Update, Delete) in PostgreSQL and MSSQL.
All names are generated in CamelCase with optional prefix.
"""

def camelCase(name):
    """
    Brief description:
        Converts a snake_case string into camelCase format.

    Parameters:
        name (str): The input string in snake_case.

    Returns:
        str: The converted string in camelCase format.
    """
    parts = name.split('_')
    return parts[0].lower() + ''.join(p.capitalize() for p in parts[1:])

def generateInsertPostgres(schema, table, columns, prefix=""):
    """
    Brief description:
        Generates a PostgreSQL INSERT function that inserts a new record into the specified table,
        excluding the 'id' column, and returns the generated ID.

    Parameters:
        schema (str): Schema name where the table resides.
        table (str): Target table name.
        columns (list[tuple]): List of (column_name, data_type) tuples.
        prefix (str, optional): Optional prefix for the function name. Defaults to "".

    Returns:
        str: The complete SQL code for creating the INSERT function in PostgreSQL.
    """
    colNames = [col[0] for col in columns if col[0] != 'id']
    params = ', '.join([f"p_{col} {dtype}" for col, dtype in columns if col != 'id'])
    insertCols = ', '.join(colNames)
    values = ', '.join([f"p_{col}" for col in colNames])
    funcName = f"{prefix}Insert{table.capitalize()}"
    fullName = f"{schema}.{funcName}"

    return f"""
    CREATE OR REPLACE FUNCTION {fullName}({params})
    RETURNS INT AS $$
    DECLARE
        v_id INT;
    BEGIN
        INSERT INTO {schema}.{table} ({insertCols})
        VALUES ({values})
        RETURNING id INTO v_id;

        RETURN v_id;
    END;
    $$ LANGUAGE plpgsql;
    """

def generateDeletePostgres(schema, table, prefix="", filterField="id"):
    """
    Brief description:
        Generates a PostgreSQL DELETE function that removes a record from the specified table
        based on a given filter field.

    Parameters:
        schema (str): Schema name where the table resides.
        table (str): Target table name.
        prefix (str, optional): Optional prefix for the function name. Defaults to "".
        filterField (str, optional): Column to use as the condition in the WHERE clause. Defaults to "id".

    Returns:
        str: The complete SQL code for creating the DELETE function in PostgreSQL.
    """
    funcName = f"{prefix}Delete{table.capitalize()}"
    fullName = f"{schema}.{funcName}"
    return f"""
    CREATE OR REPLACE FUNCTION {fullName}(p_{filterField} INT)
    RETURNS VOID AS $$
    BEGIN
        DELETE FROM {schema}.{table} WHERE {filterField} = p_{filterField};
    END;
    $$ LANGUAGE plpgsql;
    """

def generateUpdatePostgres(schema, table, columns, prefix="", filterField="id"):
    """
    Brief description:
        Generates a PostgreSQL UPDATE function that updates all columns in the given table
        except the filter field, which is used in the WHERE clause.

    Parameters:
        schema (str): Schema name where the table resides.
        table (str): Target table name.
        columns (list[tuple]): List of (column_name, data_type) tuples.
        prefix (str, optional): Optional prefix for the function name. Defaults to "".
        filterField (str, optional): Column to use as the condition in the WHERE clause. Defaults to "id".

    Returns:
        str: The complete SQL code for creating the UPDATE function in PostgreSQL.
    """   
    sets = ', '.join([f"{col} = p_{col}" for col, _ in columns if col != filterField])
    params = ', '.join([f"p_{col} {dtype}" for col, dtype in columns])
    funcName = f"{prefix}Update{table.capitalize()}"
    fullName = f"{schema}.{funcName}"

    return f"""
    CREATE OR REPLACE FUNCTION {fullName}({params})
    RETURNS VOID AS $$
    BEGIN
        UPDATE {schema}.{table}
        SET {sets}
        WHERE {filterField} = p_{filterField};
    END;
    $$ LANGUAGE plpgsql;
    """

def generateSelectPostgres(schema, table, columns, prefix="", filterFields=None):
    """
    Brief description:
        Generates a PostgreSQL SELECT function that returns all columns from a table,
        filtered by one or more fields.

    Parameters:
        schema (str): Schema name where the table resides.
        table (str): Target table name.
        columns (list[tuple]): List of (column_name, data_type) tuples.
        prefix (str, optional): Optional prefix for the function name. Defaults to "".
        filterFields (list[str], optional): Specific columns to include in the WHERE clause.
                                            If not provided, the first column is used.

    Returns:
        str: The complete SQL code for creating the SELECT function in PostgreSQL,
             or a comment if validation fails.
    """
    funcName = f"{prefix}Select{table.capitalize()}"
    fullName = f"{schema}.{funcName}"
    validColumns = [col for col in columns if isinstance(col, tuple) and len(col) == 2]
    if not validColumns:
        return f"-- No valid columns found for table {table}"

    colDefs = ', '.join([f"{col} {dtype}" for col, dtype in validColumns])

    if filterFields:
        filters = []
        for field in filterFields:
            dtype = next((dtype for col, dtype in validColumns if col == field), None)
            if not dtype:
                return f"-- Filter field '{field}' not found in table {table}"
            filters.append((field, dtype))
        paramDefs = ', '.join([f"p_{col} {dtype}" for col, dtype in filters])
        whereClause = ' AND '.join([f"t.{col} = p_{col}" for col, _ in filters])
    else:
        filterCol, filterType = validColumns[0]
        paramDefs = f"p_{filterCol} {filterType}"
        whereClause = f"t.{filterCol} = p_{filterCol}"

    return f"""
    CREATE OR REPLACE FUNCTION {fullName}({paramDefs})
    RETURNS TABLE({colDefs}) AS $$
    BEGIN
        RETURN QUERY
        SELECT t.* FROM {schema}.{table} t
        WHERE {whereClause};
    END;
    $$ LANGUAGE plpgsql;
    """
def normalize_dtype(dtype):
    """
    Brief description:
        Normalizes SQL data types by assigning default lengths to character types
        (e.g., 'varchar' → 'VARCHAR(100)') when unspecified.

    Parameters:
        dtype (str): The raw data type string from the database schema.

    Returns:
        str: A properly formatted SQL data type string with default length if needed.
    """
    dtype_lower = dtype.lower().strip()
    if dtype_lower in ['varchar', 'char']:
        return f"{dtype.upper()}(100)"
    if "varchar" in dtype_lower and "(" not in dtype_lower:
        return "VARCHAR(100)"
    if "char" in dtype_lower and "(" not in dtype_lower:
        return "CHAR(100)"
    return dtype

def generateInsertMSSQL(schema, table, columns, prefix=""):
    """
    Brief description:
        Generates a SQL Server INSERT stored procedure for the specified table,
        excluding the 'id' column from parameters and insert targets.

    Parameters:
        schema (str): Schema name where the table resides.
        table (str): Target table name.
        columns (list[tuple]): List of (column_name, data_type) tuples.
        prefix (str, optional): Optional prefix for the procedure name. Defaults to "".

    Returns:
        str: The complete SQL code for creating the INSERT stored procedure.
    """
    colNames = [col[0] for col in columns if col[0] != 'id']
    params = ', '.join([f"@p_{col} {normalize_dtype(dtype)}" for col, dtype in columns if col != 'id'])
    insertCols = ', '.join(colNames)
    values = ', '.join([f"@p_{col}" for col in colNames])
    name = f"{schema}.{prefix}Insert{table.capitalize()}"

    return f"""
    CREATE PROCEDURE {name}
    {params}
    AS
    BEGIN
        INSERT INTO {schema}.{table} ({insertCols})
        VALUES ({values});
    END
    """

def generateDeleteMSSQL(schema, table, prefix="", filterField="id"):
    """
    Brief description:
        Generates a SQL Server DELETE stored procedure for the specified table,
        using a single filter field (default is "id") in the WHERE clause.

    Parameters:
        schema (str): Schema name where the table resides.
        table (str): Target table name.
        prefix (str, optional): Optional prefix for the procedure name. Defaults to "".
        filterField (str, optional): Column to use in the WHERE clause. Defaults to "id".

    Returns:
        str: The complete SQL code for creating the DELETE stored procedure.
    """
    name = f"{schema}.{prefix}Delete{table.capitalize()}"
    return f"""
    CREATE PROCEDURE {name}
    @p_{filterField} INT
    AS
    BEGIN
        DELETE FROM {schema}.{table} WHERE {filterField} = @p_{filterField};
    END
    """

def generateUpdateMSSQL(schema, table, columns, prefix="", filterField="id"):
    """
    Brief description:
        Generates a SQL Server UPDATE stored procedure for the specified table,
        using all columns as parameters and filtering by a specific field.

    Parameters:
        schema (str): Schema name where the table resides.
        table (str): Target table name.
        columns (list[tuple]): List of (column_name, data_type) tuples.
        prefix (str, optional): Optional prefix for the procedure name. Defaults to "".
        filterField (str, optional): Column to use in the WHERE clause for filtering. Defaults to "id".

    Returns:
        str: The complete SQL code for creating the UPDATE stored procedure.
    """
    sets = ', '.join([f"{col} = @p_{col}" for col, _ in columns if col != filterField])
    params = ', '.join([f"@p_{col} {normalize_dtype(dtype)}" for col, dtype in columns])
    name = f"{schema}.{prefix}Update{table.capitalize()}"

    return f"""
    CREATE PROCEDURE {name}
    {params}
    AS
    BEGIN
        UPDATE {schema}.{table}
        SET {sets}
        WHERE {filterField} = @p_{filterField};
    END
    """

def generateSelectMSSQL(schema, table, columns, prefix="", filterFields=None):
    """
    Brief description:
        Generates a SQL Server SELECT stored procedure for the specified table,
        optionally filtering by provided columns.

    Parameters:
        schema (str): Schema name where the table resides.
        table (str): Target table name.
        columns (list[tuple]): List of (column_name, data_type) tuples.
        prefix (str, optional): Optional prefix for the procedure name. Defaults to "".
        filterFields (list[str], optional): Specific column names to use in the WHERE clause. 
                                            If not provided, the first column is used by default.

    Returns:
        str: The complete SQL code for creating the SELECT stored procedure, or
             a comment string if column validation fails.
    """
    validColumns = [ (row[0], row[1]) for row in columns if len(row) == 2 ]
   
    if not validColumns:
        return f"-- No valid columns found for table {table}"

    colDefs = ', '.join([f"{col} {dtype}" for col, dtype in validColumns])

    if filterFields:
        filters = []
        for field in filterFields:
            dtype = next((dtype for col, dtype in validColumns if col == field), None)
            if not dtype:
                return f"-- Filter field '{field}' not found in table {table}"
            filters.append((field, dtype))
        paramDefs = ', '.join([f"@p_{col} {normalize_dtype(dtype)}" for col, dtype in filters])
        whereClause = ' AND '.join([f"{col} = @p_{col}" for col, _ in filters])
    else:
        filterCol, filterType = validColumns[0]
        paramDefs = f"@p_{filterCol} {normalize_dtype(filterType)}"
        whereClause = f"{filterCol} = @p_{filterCol}"

    name = f"{schema}.{prefix}Select{table.capitalize()}"

    return f"""
    CREATE PROCEDURE {name}
    {paramDefs}
    AS
    BEGIN
        SELECT * FROM {schema}.{table}
        WHERE {whereClause};
    END
    """
