class DBTable(object):
    """
    Object that contains the meta data of a database table and builds the queries
    to load all data from the source table to the target table.
    """

    def __init__(self, mtd_dict):
        self.schema_name = mtd_dict["schema_name"]
        self.table_name = mtd_dict["table_name"]
        self.column_names = mtd_dict["column_names"]
        self.window_column_name = mtd_dict.get("window_column_name")
        self.object_name = mtd_dict.get("object_name")

    def fetch_query(self):
        column_names = ", ".join(self.column_names)
        return f"SELECT {column_names} FROM {self.schema_name}.{self.table_name}"

    def windowed_fetch_query(self):
        column_names = ", ".join(self.column_names)
        return f"SELECT {column_names} FROM {self.schema_name}.{self.table_name} WHERE {self.window_column_name} > ?"

    def insert_query(self):
        column_names = ", ".join(self.column_names)
        values = ", ".join(["?"] * len(self.column_names))
        return f"INSERT INTO {self.schema_name}.{self.table_name}({column_names}) VALUES({values}) "

    def truncate_query(self):
        return f"TRUNCATE TABLE {self.schema_name}.{self.table_name}"

    def get_table_ref(self):
        return f"{self.schema_name}.{self.table_name}"
