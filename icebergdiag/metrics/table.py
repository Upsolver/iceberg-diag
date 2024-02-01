class Table:
    def __init__(self, database: str, table_name: str):
        self.database = database
        self.table_name = table_name

    @classmethod
    def from_full_name(cls, full_table_name: str):
        parts = full_table_name.strip().split('.', maxsplit=1)

        if len(parts) == 2:
            return cls(*parts)
        else:
            return cls('', full_table_name)

    def __eq__(self, other):
        return self.database == other.database and self.table_name == other.table_name

    def full_table_name(self) -> str:
        return f"{self.database}.{self.table_name}"

    def __str__(self):
        return self.full_table_name()
