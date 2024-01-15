import time
from datetime import datetime, timedelta

from database import Database


class Api:
    def __init__(self, database: Database):
        self._database = database
        self._start_timer = time.time()

    def __enter__(self):
        self.create_temp_table()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        if exception_type:
            print(exception_value)
        elapsed = (time.time() - self._start_timer)
        print(f"Duration: {str(timedelta(seconds=elapsed))}")
        self._database.close()

    @staticmethod
    def validate_row(row: tuple) -> bool:
        valid = True
        if len(row) != 4:
            valid = False

        if row[0] == "" or row[1] == "" or \
                row[2] == "" or row[3] == "":
            valid = False

        if not row[3].isnumeric():
            valid = False

        return valid

    def close(self):
        self._database.close()

    def start_load(self, filename: str) -> int:
        timestamp = datetime.now()
        sql = """
            INSERT INTO load(timestamp, filename)
            VALUES (%s, %s)
            RETURNING id
        """
        return int(self._database.insert(sql, (timestamp, filename), get_return_value=True))

    def create_temp_table(self) -> None:
        self._database.insert("""
            CREATE TEMP TABLE stock_temp(
                date date,
                stock int, 
                product_name text, 
                point_of_sale_name text
            )""")

    def insert_in_temp(self, chunk: list[tuple]) -> None:
        sql = """
            INSERT INTO stock_temp(point_of_sale_name, product_name, date, stock)
            VALUES %s
        """

        self._database.execute_batch(sql, chunk)

    def cross_check_and_dump(self, load_id: int) -> None:
        # 1: Insert into point of sale and product entries without id
        sql = """
            INSERT INTO point_of_sale(name)
            SELECT DISTINCT st.point_of_sale_name
                    FROM stock_temp st
                LEFT OUTER JOIN point_of_sale pos ON pos.name = st.point_of_sale_name
            WHERE pos.id IS NULL;
            
            INSERT INTO product(name)
            SELECT DISTINCT st.product_name
                    FROM stock_temp st
                LEFT OUTER JOIN product p ON p.name = st.product_name
            WHERE p.id IS NULL;
        """
        self._database.insert(sql)

        # 2: Query for conflicts and return load ids, timestamp and file name
        sql = """
            WITH new_entries AS (
                SELECT pos.id AS point_of_sale_id, p.id AS product_id, st.date AS date
                FROM stock_temp st
                LEFT JOIN point_of_sale pos ON pos.name = st.point_of_sale_name
                LEFT JOIN product p ON p.name = st.product_name
            )
            SELECT DISTINCT ON (s.load_id) filename, timestamp FROM stock s
                INNER JOIN new_entries ne ON ne.point_of_sale_id = s.point_of_sale_id AND
                                       ne.product_id = s.product_id AND
                                       ne.date = s.date
                LEFT JOIN load l ON l.id = s.load_id;
        """
        duplicates = self._database.query(sql)

        if len(duplicates) > 0:
            print("Error: Found duplicated data")
            for dup in duplicates:
                print(f"Duplicate from {dup[0]} on the {dup[1]}")

            return

        # 3: Insert into table
        sql = """
            INSERT INTO stock(point_of_sale_id, product_id, date, stock, load_id)
            SELECT pos.id, p.id, st.date, st.stock, %s FROM stock_temp st
                INNER JOIN point_of_sale pos ON pos.name = st.point_of_sale_name
                INNER JOIN product p ON p.name = st.product_name;
            
            TRUNCATE TABLE stock_temp;
        """
        self._database.insert(sql, (load_id,))
        self._database.commit()

    def list_loads(self) -> None:
        sql = """
            SELECT DISTINCT on (s.load_id ) s.load_id, l.filename, s.date FROM load l
                INNER JOIN stock s ON s.load_id = l.id
        """

        loads = self._database.query(sql)

        if len(loads) > 0:
            load_ids = []
            print("- Load Number - File name - Date -")
            for load in loads:
                print(f"-> {load[0]} - {load[1]} - {load[2]} -")
                load_ids.append(int(load[0]))

            delete_load = input("Do you wish to delete a load? (Y/N) ")

            if delete_load == 'Y':
                load_to_delete = input("Load number: ")

                if load_to_delete.isnumeric() and int(load_to_delete) in load_ids:
                    print(f"Deleting load {load_to_delete}")
                    self.delete_load(int(load_to_delete))
        else:
            print("No stock data found")

    def delete_load(self, load_id: int) -> None:
        sql = """
            DELETE FROM stock WHERE load_id=%s;
        """

        self._database.insert(sql, (load_id,))
        self._database.commit()
