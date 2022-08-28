import json
from re import L
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
    for m in manifest:
        dtypes["/".join((m["category"], m["name"]))] = m["type"]
    df = pd.read_csv(
        os.path.expanduser(fname),
        compression='infer',
        low_memory=True,
        # engine="pyarrow",
        dtype=dtypes,
        usecols=["lineItem/BlendedCost", "lineItem/UsageEndDate"],
        )
    df.rename(columns=lambda s: "_".join(s.split("/")), inplace=True)
    return df


def parse_bill(df:pd.DataFrame):
    print(df)
    # print(df[:5])


def main():
    df = read_bill("~/w/aws-billing/daily-00001.csv.gz")
    parse_bill(df)
    return True

if __name__ == "__main__":
    main()