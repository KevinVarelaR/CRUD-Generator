import tkinter as tk
from tkinter import ttk, messagebox
import os
import sys
import psycopg2
import pyodbc

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from backend.db.metadata import getColumns, getSchemas, getTables
from backend.generators.crud import (
    generateInsertPostgres, generateUpdatePostgres, generateDeletePostgres, generateSelectPostgres,
    generateInsertMSSQL, generateUpdateMSSQL, generateDeleteMSSQL, generateSelectMSSQL
)

class CrudGenerator(tk.Tk):
    """
    Brief description:
        Main application window for generating and executing SQL CRUD procedures
        based on the selected database schema and tables.

    Attributes:
        engine (str): Database engine ("PostgreSQL" or "MSSQL").
        host (str): Hostname or IP address of the database server.
        user (str): Database username.
        password (str): Database password.
        dbname (str): Target database name.
        selectedTable (tk.StringVar): Currently selected table.
        selectedOption (tk.StringVar): Currently selected CRUD operation.
        executionMode (tk.StringVar): Execution mode ("Code Generation" or "Code Generation and Execution").
        selectedSchema (tk.StringVar): Currently selected database schema.
        prefixEntry (tk.Entry or None): Entry widget for optional procedure/function prefix.
        schemaOptions (list): List of available schemas in the database.
        tableVars (dict): Mapping of table names to selection variables.
        crudVars (dict): Mapping of CRUD actions to selection variables.

    Methods:
        - buildLayout(): Constructs the main layout.
        - buildLeftPanel(): Builds the selection UI for tables and CRUD options.
        - updateTableCheckboxes(): Updates table list dynamically.
        - displayRightPanel(sql): Displays generated SQL code in the right panel.
        - addConfirmButton(): Adds the confirm/generate button.
    """
    def __init__(self, engine, host, user, password, dbname):
        super().__init__()
        self.engine = engine
        self.host = host
        self.user = user
        self.password = password
        self.dbname = dbname

        self.title("CRUD Generator")
        self.state("zoomed")
        self.configure(bg="#f0f8ff")
        self.option_add("*Font", ("Segoe UI", 10))

        self.selectedTable = tk.StringVar()
        self.selectedOption = tk.StringVar()
        self.currentFields = []
        self.currentRecords = []
        self.executionMode = tk.StringVar(value="Code Generation")
        self.prefixEntry = None
        self.schemaOptions = []
        self.selectedSchema = tk.StringVar()

        self.tableVars = {}
        self.crudVars = {}

        self.buildLayout()
        self.buildLeftPanel()
        self.updateTableCheckboxes()
        self.displayRightPanel("")
        self.addConfirmButton()

    def buildLayout(self):
        """
        Brief description:
            Constructs the main graphical layout of the application, including top, bottom,
            left (with scrollable canvas), and right frames for interaction and display.

        Parameters:
            None (uses internal state to build and assign frame widgets).

        Returns:
            None
        """
        self.mainFrame = tk.Frame(self, bg="#f0f8ff")
        self.mainFrame.pack(fill=tk.BOTH, expand=True)

        self.topFrame = tk.Frame(self.mainFrame, bg="#f0f8ff")
        self.topFrame.pack(side=tk.TOP, fill=tk.BOTH, expand=True)

        leftContainer = tk.Frame(self.topFrame, bg="#f0f8ff")
        leftContainer.pack(side=tk.LEFT, fill=tk.Y, padx=20, pady=20)

        self.canvas = tk.Canvas(leftContainer, bg="#e6f2ff", highlightthickness=0)
        scrollbar = tk.Scrollbar(leftContainer, orient="vertical", command=self.canvas.yview)
        self.leftFrame = tk.Frame(self.canvas, bg="#e6f2ff")

        self.leftFrame.bind("<Configure>", lambda e: self.canvas.configure(scrollregion=self.canvas.bbox("all")))
        self.canvas.create_window((0, 0), window=self.leftFrame, anchor="nw")
        self.canvas.configure(yscrollcommand=scrollbar.set)

        self.canvas.pack(side="left", fill="y", expand=True)
        scrollbar.pack(side="right", fill="y")

        self.rightFrame = tk.Frame(self.topFrame, bg="#ffffff", bd=2, relief=tk.GROOVE, width=400)
        self.rightFrame.pack(side=tk.RIGHT, padx=20, pady=20, fill=tk.BOTH, expand=True)

        self.bottomFrame = tk.Frame(self.mainFrame, bg="#f0f8ff")
        self.bottomFrame.pack(side=tk.BOTTOM, pady=10)

    def executeSql(self, sql):
        """
        Brief description:
            Executes a given SQL statement on the connected database (PostgreSQL or MSSQL).
            Shows an error message if execution fails.

        Parameters:
            sql (str): The SQL statement to execute.

        Returns:
            None
        """
        try:
            if self.engine == "PostgreSQL":
                conn = psycopg2.connect(
                    dbname=self.dbname,
                    user=self.user,
                    password=self.password,
                    host=self.host,
                    port=5432
                )
            elif self.engine == "MSSQL":
                connStr = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={self.host};DATABASE={self.dbname};UID={self.user};PWD={self.password}"
                conn = pyodbc.connect(connStr)
            else:
                raise ValueError("Unsupported database engine")
            with conn.cursor() as cur:
                cur.execute(sql)
            conn.commit()
        except Exception as e:
            messagebox.showerror("Error", f"❌ Failed to execute procedure:{e}")

    def showSqlInPanel(self, sql):
        """
        Brief description:
            Displays the given SQL code in the right-side panel of the UI using a read-only text widget.

        Parameters:
            sql (str): The SQL code to display in the panel.

        Returns:
            None
        """
        for widget in self.rightFrame.winfo_children():
            widget.destroy()
        text = tk.Text(self.rightFrame, wrap="word", font=("Courier", 10), bg="#f9f9f9")
        text.insert("1.0", sql)
        text.pack(fill=tk.BOTH, expand=True)
        text.config(state="disabled")

    def generateCrudProcedures(self):
        """_ Breve descripción:
        Genera (y opcionalmente ejecuta) procedimientos SQL CRUD para las tablas seleccionadas,
        según las acciones elegidas y el motor de base de datos configurado (PostgreSQL o MSSQL).
        Para cada tabla activa y acción CRUD (Insert, Update, Delete, Filter), se invoca la función 
        de generación SQL correspondiente y, de ser el modo de ejecución el adecuado, se ejecuta 
        la instrucción generada.
        Parámetros:
        self: Objeto de la clase que contiene la configuración y los elementos de la interfaz de usuario. 
              Debe disponer de los siguientes atributos:
              - tableVars (dict): Diccionario que asocia nombres de tabla con variables de selección.
              - selectedSchema (tkinter variable): Elemento que almacena el esquema seleccionado.
              - executionMode (tkinter variable): Elemento que indica el modo de ejecución ("Code Generation and Execution" u otro).
              - prefixEntry (widget o similar): Entrada opcional para un prefijo a aplicar en la generación de SQL.
              - engine (str): Motor de la base de datos ("PostgreSQL" o "MSSQL").
              - host (str): Dirección del host de la base de datos.
              - user (str): Usuario de la base de datos.
              - password (str): Contraseña de la base de datos.
              - dbname (str): Nombre de la base de datos.
              Además, se espera que disponga de los métodos:
              - getSelectedCrudActions(): Devuelve las acciones CRUD seleccionadas.
              - executeSql(sql): Ejecuta la instrucción SQL proporcionada.
              - showSqlInPanel(sql): Muestra en un panel el SQL generado.
              Retorno:
        None
        (El método no retorna ningún valor; sin embargo, genera efectos secundarios:
            - Muestra en un panel el código SQL completo generado.
            - Ejecuta, si el modo de ejecución es "Code Generation and Execution", cada instrucción SQL.
            - Muestra un mensaje de éxito si se ejecutó al menos una instrucción SQL.)
        """
        
        tables = [table for table, var in self.tableVars.items() if var.get()]
        actions = self.getSelectedCrudActions()
        schema = self.selectedSchema.get()
        mode = self.executionMode.get()
        prefix = self.prefixEntry.get().strip() if self.prefixEntry else ""

        fullSql = ""
        somethingExecuted = False

        for table in tables:
            columns = getColumns(self.engine, self.host, self.user, self.password, self.dbname, schema, table)

            for action in actions:
                if self.engine == "PostgreSQL":
                    if action == "Insert":
                        sql = generateInsertPostgres(schema, table, columns, prefix)
                    elif action == "Update":
                        sql = generateUpdatePostgres(schema, table, columns, prefix)
                    elif action == "Delete":
                        sql = generateDeletePostgres(schema, table, prefix)
                    elif action == "Filter":
                        sql = generateSelectPostgres(schema, table, columns, prefix)
                    else:
                        continue
                elif self.engine == "MSSQL":
                    if action == "Insert":
                        sql = generateInsertMSSQL(schema, table, columns, prefix)
                    elif action == "Update":
                        sql = generateUpdateMSSQL(schema, table, columns, prefix)
                    elif action == "Delete":
                        sql = generateDeleteMSSQL(schema, table, prefix)
                    elif action == "Filter":
                        sql = generateSelectMSSQL(schema, table, columns, prefix)
                    else:
                        continue
                else:
                    continue

                fullSql += sql + "\n\n"
                if mode == "Code Generation and Execution":
                    self.executeSql(sql)
                    somethingExecuted = True

        self.showSqlInPanel(fullSql)
        if somethingExecuted:
            messagebox.showinfo("Success", "✅ All procedures were successfully executed.")

    def getSelectedCrudActions(self):
        """
        Returns a list of CRUD actions currently selected by the user.

        Iterates over the CRUD action checkboxes and includes those that 
        are checked (e.g., Insert, Delete, Update, Filter).
        """
        return [action for action, var in self.crudVars.items() if var.get()]

    def getAvailableTables(self):
        """
        Retrieves a list of available tables from the currently selected schema.

        Uses the getTables() utility to query the database and return all 
        base tables for the active schema selection.
        """
        return getTables(self.engine, self.host, self.user, self.password, self.dbname, self.selectedSchema.get())

    def updateTableCheckboxes(self, event=None):
        """
        Updates the list of table checkboxes based on the currently selected schema.

        Destroys any existing checkbox list, fetches available tables for the 
        selected schema, and renders a new group of checkboxes for user selection.
        """
        if hasattr(self, 'tableFrame'):
            self.tableFrame.destroy()

        self.tableVars = {}
        tables = self.getAvailableTables()
        self.tableFrame = tk.Frame(self.leftFrame, bg="#e6f2ff")
        self.tableFrame.pack(anchor="w", padx=10, pady=(5, 10))

        tk.Label(self.tableFrame, text="Select one or more tables", bg="#e6f2ff", font=("Segoe UI", 12, "bold")).pack(anchor="w", pady=(5, 0))

        for table in tables:
            var = tk.BooleanVar()
            chk = tk.Checkbutton(self.tableFrame, text=table, variable=var, bg="#e6f2ff")
            chk.pack(anchor="w", padx=10)
            self.tableVars[table] = var

    def buildLeftPanel(self):
        """
        Builds the left panel of the interface with input options for CRUD generation.

        This includes:
        - Schema selection (with PostgreSQL/MSSQL defaults)
        - Prefix input field
        - Execution mode selector
        - CRUD action checkboxes (Insert, Delete, Update, Filter)
        - Logout button at the bottom
        """
        self.schemaOptions = getSchemas(self.engine, self.host, self.user, self.password, self.dbname)
        tk.Label(self.leftFrame, text="Select schema", bg="#e6f2ff", font=("Segoe UI", 12, "bold")).pack(anchor="w", padx=10, pady=(10, 0))

        schemaCombo = ttk.Combobox(self.leftFrame, values=self.schemaOptions, textvariable=self.selectedSchema, state="readonly", width=30)
        if self.engine == "PostgreSQL" and "public" in self.schemaOptions:
            self.selectedSchema.set("public")
        elif self.engine == "MSSQL" and self.schemaOptions:
            self.selectedSchema.set(self.schemaOptions[0])
        schemaCombo.pack(anchor="w", padx=20, pady=2)
        schemaCombo.bind("<<ComboboxSelected>>", self.updateTableCheckboxes)

        options = [
            ("Enter prefix", "input"),
            ("Select execution mode", ["Code Generation", "Code Generation and Execution"])
        ]

        for labelText, values in options:
            tk.Label(self.leftFrame, text=labelText, bg="#e6f2ff", font=("Segoe UI", 12, "bold")).pack(anchor="w", padx=10, pady=(10, 0))
            if values == "input":
                self.prefixEntry = tk.Entry(self.leftFrame, width=30)
                self.prefixEntry.pack(anchor="w", padx=20, pady=2)
            else:
                combo = ttk.Combobox(self.leftFrame, values=values, state="readonly", textvariable=self.executionMode, width=30)
                combo.current(0)
                combo.pack(anchor="w", padx=20, pady=2)

        tk.Label(self.leftFrame, text="Select CRUD actions", bg="#e6f2ff", font=("Segoe UI", 12, "bold")).pack(anchor="w", padx=10, pady=(10, 0))
        crudOptions = ["Insert", "Delete", "Update", "Filter"]
        for option in crudOptions:
            var = tk.BooleanVar()
            chk = tk.Checkbutton(self.leftFrame, text=option, variable=var, bg="#e6f2ff")
            chk.pack(anchor="w", padx=20)
            self.crudVars[option] = var

        logoutBtn = tk.Button(self.leftFrame, text="Logout", command=self.logout, bg="#f44336", fg="white", font=("Segoe UI", 10, "bold"))
        logoutBtn.pack(side=tk.BOTTOM, fill=tk.X, pady=15, padx=5)


    def addConfirmButton(self):
        """
        Adds and positions action buttons at the bottom of the interface.

        This includes buttons for generating procedures and viewing permissions.
        Buttons are grouped in left and right frames within the bottom section
        of the main window.
        """
        self.bottomFrameLeft = tk.Frame(self.bottomFrame, bg="#f0f8ff")
        self.bottomFrameLeft.pack(side=tk.LEFT, padx=20)

        self.bottomFrameRight = tk.Frame(self.bottomFrame, bg="#f0f8ff")
        self.bottomFrameRight.pack(side=tk.RIGHT, pady=10)

        # Botones de acción (lado derecho)
        generateBtn = tk.Button(self.bottomFrameRight, text="Generate Procedures", command=self.generateCrudProcedures, bg="#4CAF50", fg="white")
        generateBtn.pack(side=tk.LEFT, padx=10)

        permissionsBtn = tk.Button(self.bottomFrameRight, text="View Permissions", command=self.viewPermissions, bg="#2196F3", fg="white")
        permissionsBtn.pack(side=tk.LEFT, padx=10)

    def displayRightPanel(self, option):
        """
        Placeholder for displaying content in the right panel based on user selection.
        """
        pass

    def logout(self):
        """
        Closes the current session and returns the user to the login window.

        This method destroys the current application window and reinitializes
        the login interface by launching a new instance of ConnectionApp.
        """
        self.destroy()
        from ui.mainWindow import ConnectionApp
        app = ConnectionApp()
        app.mainloop()

    def viewPermissions(self):
        """
        Retrieves and displays read/write permissions for the selected tables.

        This method calls the `getPermissions` function to obtain column-level 
        access information (read and write) for each selected table within the 
        current schema. The results are formatted and displayed in the right panel.
        """
        from backend.db.metadata import getPermissions

        schema = self.selectedSchema.get()
        tables = [t for t, v in self.tableVars.items() if v.get()]
        allPermissions = ""

        for table in tables:
            permissions = getPermissions(self.engine, self.host, self.user, self.password, self.dbname, schema, table)
            allPermissions += f"🔹 Table: {table}\n"
            for col, can_read, can_write in permissions:
                allPermissions += f"  • {col} → Read: {'✅' if can_read else '❌'} | Write: {'✅' if can_write else '❌'}\n"
            allPermissions += "\n"

        self.showSqlInPanel(allPermissions)
