from dataclasses import dataclass

import pandas as pd

from sources.utils import ZippedCsvToDatabase


@dataclass
class WorldBankPopulation(ZippedCsvToDatabase):
    zip_url: str = "https://api.worldbank.org/v2/en/indicator/SP.POP.TOTL?downloadformat=csv"
    csv_name: str = "API_SP.POP.TOTL_DS2_en_csv_v2_5436324.csv"
    db_table: str = "worldbank_population"
    skiprows: int = 4

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.filter(regex='^(?!Unnamed)')
        df = df.drop(columns=["Indicator Name", "Indicator Code"])
        return df.melt(id_vars=["Country Code", "Country Name"], var_name="Year", value_name="Population")


@dataclass
class WorldBankGdpPerCapita(ZippedCsvToDatabase):
    zip_url: str = "https://api.worldbank.org/v2/en/indicator/NY.GDP.PCAP.CD?downloadformat=csv"
    csv_name: str = "API_NY.GDP.PCAP.CD_DS2_en_csv_v2_5358417.csv"
    db_table: str = "worldbank_gdp_per_capita"
    skiprows: int = 4

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.filter(regex='^(?!Unnamed)')
        df = df.drop(columns=["Indicator Name", "Indicator Code"])
        return df.melt(id_vars=["Country Code", "Country Name"], var_name="Year", value_name="GDP")
