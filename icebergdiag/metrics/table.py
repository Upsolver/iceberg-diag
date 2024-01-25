class Table:
    def __init__(self, database: str, table_name: str):
        self.database = database
        self.table_name = table_name

    def __eq__(self, other):
        return self.database == other.database and self.table_name == other.table_name

    def full_table_name(self) -> str:
        return f"{self.database}.{self.table_name}"
