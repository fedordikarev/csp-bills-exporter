import json
import pandas as pd
import os
from typing import List


BASE_PATH = "~/w/aws-billing"

def read_manifest(fname:str = None):
    if fname is None:
        fname = os.path.join(BASE_PATH, "daily-Manifest.json")
    with open(os.path.expanduser(fname), "r") as f:
        return json.load(f)


def read_bill(fname:str = None, manifest:List = []):
    dtypes = dict()
    types_mapping = {
        "DateTime": "datetime64",
        "String": "string",
        "BigDecimal": "float64",
    }
    for m in manifest:
        dtypes["/".join((m["category"], m["name"]))] = types_mapping.get(m["type"], m["type"])
    df = pd.read_csv(
        os.path.expanduser(fname),
        compression='infer',
        low_memory=True,
        # engine="pyarrow",
        # dtype=dtypes,
        usecols=["lineItem/BlendedCost", "lineItem/UsageEndDate"],
        )
    df.rename(columns=lambda s: "_".join(s.split("/")), inplace=True)
    return df


def parse_bill(df:pd.DataFrame):
    print(df.sort_values("lineItem_BlendedCost"))
    print(df.dtypes)
    print(df.info())
    # print(df[:5])


def write_out(df:pd.DataFrame, path:str) -> None:
    df.to_parquet(path=path, compression='gzip')
    df.to_csv(os.path.join(BASE_PATH, "out.csv"))


def main():
    df = read_bill(os.path.join(BASE_PATH, "daily-00001.csv.gz"))
    parse_bill(df)
    write_out(df, os.path.join(BASE_PATH, 'out_parquet.gz'))

    return True

if __name__ == "__main__":
    main()