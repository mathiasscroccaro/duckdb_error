import unittest
from datetime import datetime

import duckdb
import pandas as pd


class TestDuckDB(unittest.TestCase):
    def test_save_and_recover_df(self):

        data = {
            "timestamp": [
                datetime(2022, 1, 1, 0, 0, 0),
                datetime(2022, 1, 1, 0, 0, 1),
            ],
            "int": [1, 2],
            "float": [1.0, 2.0],
        }

        df = pd.DataFrame(data)
        df.set_index("timestamp", inplace=True)

        with duckdb.connect("file.db") as con:
            con.sql("CREATE TABLE IF NOT EXISTS example_table AS SELECT * FROM df")
            con.sql("DELETE FROM example_table")
            con.sql("INSERT INTO example_table SELECT * FROM df")

        with duckdb.connect("file.db") as con:
            expected_df = con.sql("SELECT * FROM example_table")

        self.assertFalse(df.equals(expected_df))


if __name__ == "__main__":
    unittest.main()
