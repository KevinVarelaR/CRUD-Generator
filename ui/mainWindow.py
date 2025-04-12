import tkinter as tk
from tkinter import ttk, messagebox
import psycopg2
import pyodbc
import sys, os

# Add parent directory to the path for import resolution
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from ui.crudUI import CrudGenerator

class ConnectionApp(tk.Tk):
    """
    Brief description:
        GUI application window for collecting database connection parameters from the user.
        Initializes and centers the interface, and launches the main CRUD generator upon successful connection.

    Attributes:
        engine (tk.StringVar): Selected database engine ("PostgreSQL" or "MSSQL").
        host (tk.StringVar): Hostname or IP address.
        username (tk.StringVar): Database username.
        password (tk.StringVar): Database password.
        database (tk.StringVar): Name of the database to connect to.
    """
    def __init__(self):
        super().__init__()
        self.title("Database Connection")
        self.geometry("400x400")
        self.update_idletasks()
        
        # Center the window on screen
        width = 400
        height = 400
        x = (self.winfo_screenwidth() // 2) - (width // 2)
        y = (self.winfo_screenheight() // 2) - (height // 2)
        self.geometry(f"{width}x{height}+{x}+{y}")
        self.resizable(False, False)
        self.configure(bg="#f0f0f0")

        # Variables for form fields
        self.engine = tk.StringVar(value="PostgreSQL")
        self.host = tk.StringVar()
        self.username = tk.StringVar()
        self.password = tk.StringVar()
        self.database = tk.StringVar()

        self.buildUi()

    def buildUi(self):
        """
        Brief description:
            Builds the user interface for the database connection window, including input fields
            for engine, host, username, password, and database name, along with a connect button.

        Parameters:
            None (uses internal tkinter variables and widget states).

        Returns:
            None
        """
        padding = {'padx': 20, 'pady': 5}

        tk.Label(self, text="Database Engine", font=("Segoe UI", 10, "bold"), bg="#f0f0f0").pack(**padding)
        engineDropdown = ttk.Combobox(self, values=["PostgreSQL", "MSSQL"], state="readonly", textvariable=self.engine)
        engineDropdown.pack(**padding)

        tk.Label(self, text="Host (IP or localhost)", bg="#f0f0f0").pack(**padding)
        tk.Entry(self, textvariable=self.host).pack(**padding)

        tk.Label(self, text="Username", bg="#f0f0f0").pack(**padding)
        tk.Entry(self, textvariable=self.username).pack(**padding)

        tk.Label(self, text="Password", bg="#f0f0f0").pack(**padding)
        tk.Entry(self, textvariable=self.password, show="*").pack(**padding)

        tk.Label(self, text="Database Name", bg="#f0f0f0").pack(**padding)
        tk.Entry(self, textvariable=self.database).pack(**padding)

        tk.Button(
            self, text="Connect", command=self.connectToDatabase,
            bg="#4CAF50", fg="white", font=("Segoe UI", 10, "bold"), width=15
        ).pack(pady=20)

    def connectToDatabase(self):
        """
        Brief description:
            Attempts to establish a connection to the selected database engine using
            the provided credentials. If successful, launches the main CRUD interface.

        Parameters:
            None (retrieves connection data from internal tkinter variables).

        Returns:
            None
        """
        engine = self.engine.get()
        host = self.host.get()
        user = self.username.get()
        password = self.password.get()
        dbname = self.database.get()

        try:
            # Attempt to connect using appropriate library
            if engine == "PostgreSQL":
                conn = psycopg2.connect(
                    dbname=dbname,
                    user=user,
                    password=password,
                    host=host,
                    port=5432
                )
            elif engine == "MSSQL":
                connStr = (
                    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                    f"SERVER={host};DATABASE={dbname};UID={user};PWD={password}"
                )
                conn = pyodbc.connect(connStr)
            else:
                raise Exception("Unsupported database engine")

            # Connection succeeded
            conn.close()
            self.destroy()

            # Launch main application window
            app = CrudGenerator(engine, host, user, password, dbname)
            app.mainloop()

        except Exception as e:
            messagebox.showerror("Connection Error", str(e))


if __name__ == "__main__":
    app = ConnectionApp()
    app.mainloop()
