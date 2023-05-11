from my_database import MyDatabase
from database_manager import DatabaseManager

manager = DatabaseManager()

tables_names = ['circuits', 'constructor_results', 'constructors', 'constructor_standings', 'drivers', 'driver_standings', 'lap_times', '',
                'pit_stops', 'qualifying', 'races', 'results', 'seasons', 'sprint_results', 'status']
try:
    manager.is_database_exist()

    for table_name in tables_names:
        manager.is_table_exist(table_name)

    # Check if table is empty then run spark code and write data to table
except:
    pass
else:
    pass

# Get connection from db
# db = MyDatabase()
# db.get_connection()

# Create DB


# manager.create_database()
# manager.create_tables()
