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
        self.mainFrame = tk.Frame(self, bg="#f0f8ff")
        self.mainFrame.pack(fill=tk.BOTH, expand=True)

        self.topFrame = tk.Frame(self.mainFrame, bg="#f0f8ff")
        self.topFrame.pack(side=tk.TOP, fill=tk.BOTH, expand=True)

        self.leftFrame = tk.Frame(self.topFrame, bg="#e6f2ff", bd=2, relief=tk.GROOVE)
        self.leftFrame.pack(side=tk.LEFT, padx=20, pady=20, fill=tk.Y)

        self.rightFrame = tk.Frame(self.topFrame, bg="#ffffff", bd=2, relief=tk.GROOVE, width=400)
        self.rightFrame.pack(side=tk.RIGHT, padx=20, pady=20, fill=tk.BOTH, expand=True)

        self.bottomFrame = tk.Frame(self.mainFrame, bg="#f0f8ff")
        self.bottomFrame.pack(side=tk.BOTTOM, pady=10)

    def executeSql(self, sql):
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
        for widget in self.rightFrame.winfo_children():
            widget.destroy()
        text = tk.Text(self.rightFrame, wrap="word", font=("Courier", 10), bg="#f9f9f9")
        text.insert("1.0", sql)
        text.pack(fill=tk.BOTH, expand=True)
        text.config(state="disabled")

    def generateCrudProcedures(self):
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
        return [action for action, var in self.crudVars.items() if var.get()]

    def getAvailableTables(self):
        return getTables(self.engine, self.host, self.user, self.password, self.dbname, self.selectedSchema.get())

    def updateTableCheckboxes(self, event=None):
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

    def addConfirmButton(self):
        btn = tk.Button(self.bottomFrame, text="Generate Procedures", command=self.generateCrudProcedures, bg="#4CAF50", fg="white")
        btn.pack(pady=10)

    def displayRightPanel(self, option):
        pass