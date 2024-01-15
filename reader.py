import csv
from api import Api


class Reader:
    def __init__(self, api: Api, filename: str):
        self._load_id = None
        self._filename = filename
        self._api = api

    def run(self):
        print(f"Loading file: {self._filename}")
        self._load_id = self._api.start_load(self._filename)

        chunk, chunksize = [], 1000000

        # Use context manager on open
        with open(self._filename, 'r') as file:
            reader = csv.reader(file, delimiter=';')
            for i, line in enumerate(reader):
                # Skip header
                if i == 0:
                    continue

                if i % chunksize == 0:
                    self.process_chunk(chunk)
                    chunk = []

                chunk.append(line)

            # Check leftover
            if len(chunk) > 0:
                self.process_chunk(chunk)

        self._api.cross_check_and_dump(self._load_id)

    def process_chunk(self, chunk: list) -> None:
        buffer = []

        for i, line in enumerate(chunk):
            if not self._api.validate_row(line):
                raise Exception(f"Invalid data in line {i}")
            buffer.append(line)

        self._api.insert_in_temp(buffer)
