#!/usr/bin/env python
# coding: utf-8

import sys
import pickle
import pandas as pd


def read_data(filename):
    df = pd.read_parquet(filename)

    return df


def prepare_data(df, categorical) -> pd.DataFrame:

    df["duration"] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df["duration"] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[categorical] = df[categorical].fillna(-1).astype("int").astype("str")

    return df


def main(year: int, month: int):

    year = int(sys.argv[1])
    month = int(sys.argv[2])

    input_file = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet"

    output_file = output_file = (
        f"taxi_type=yellow_year={year:04d}_month={month:02d}.parquet"
    )

    with open("model.bin", "rb") as f_in:
        dv, lr = pickle.load(f_in)

    categorical = ["PULocationID", "DOLocationID"]

    df = read_data(input_file)
    df["ride_id"] = f"{year:04d}/{month:02d}_" + df.index.astype("str")

    df = prepare_data(df, categorical)

    dicts = df[categorical].to_dict(orient="records")
    X_val = dv.transform(dicts)
    y_pred = lr.predict(X_val)

    print("predicted mean duration:", y_pred.mean())

    df_result = pd.DataFrame()
    df_result["ride_id"] = df["ride_id"]
    df_result["predicted_duration"] = y_pred

    df_result.to_parquet(output_file, engine="pyarrow", index=False)


if __name__ == "__main__":
    main(int(sys.argv[1]), int(sys.argv[2]))
