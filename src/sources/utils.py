import io
import zipfile
from abc import ABC, abstractmethod
from dataclasses import dataclass

import pandas as pd
import requests
from sqlalchemy import create_engine


@dataclass
class ZippedCsvToDatabase(ABC):
    zip_url: str
    csv_name: str
    db_table: str
    skiprows: int = 0
    db_url: str = "postgresql://znckjdyz:1Z1UVKsGHICvNknjnOSrzw3mlNjnN1jn@trumpet.db.elephantsql.com/znckjdyz"

    def extract(self) -> pd.DataFrame:
        response = requests.get(self.zip_url)
        with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
            print(zip_file.namelist())
            with zip_file.open(self.csv_name) as csv_file:
                return pd.read_csv(csv_file, skiprows=self.skiprows)

    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        pass

    def load(self, df: pd.DataFrame) -> None:
        engine = create_engine(self.db_url)
        with engine.begin() as conn:
            df.to_sql(self.db_table, conn, if_exists="replace", index=False)

    def etl(self) -> None:
        self.load(self.transform(self.extract()))
