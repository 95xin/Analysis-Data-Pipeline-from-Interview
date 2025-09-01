import json 
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime



def read_trans(file_path):
    res=[]
    with open(file_path, "r") as f:
        for line in f:
            data = json.loads(line)
            hash = data["hash"]
            block_timestamp = data["block_timestamp"]
            fee = data["fee"]
            inputs = data.get("inputs", [])
            outputs = data.get("outputs", [])
            for i in inputs:
                res.append({
                    "hash": hash,
                    "block_timestamp": block_timestamp,
                    "fee": fee,
                    "input_address": i["addresses"][0],
                    "input_value": i["value"]
                })
            for i in outputs:
                res.append({
                    "hash": hash,
                    "block_timestamp": block_timestamp,
                    "fee": fee,
                    "output_address": i["addresses"][0],
                    "output_value": i["value"]
                })

    df = pd.DataFrame(res)
    df["block_timestamp"] = pd.to_datetime(df["block_timestamp"])
    
    return df

ALL_COLS = ["hash","block_timestamp","fee",
            "input_address","input_value",
            "output_address","output_value"]

def _ensure_cols(df: pd.DataFrame) -> pd.DataFrame:
    # 缺的列补上，顺序也固定
    for c in ALL_COLS:
        if c not in df.columns:
            df[c] = pd.NA
    df = df[ALL_COLS]
    # 类型/格式统一
    df["block_timestamp"] = pd.to_datetime(df["block_timestamp"])
    return df


def upload_to_db(table_name, file_path, read_chunk=5000, write_chunk=5000):
    user, password, host, port, database = "root", "12345678", "localhost", "3306", "interview"
    engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}")

    buffer = []
    total = 0
    with open(file_path, "r", encoding="utf-8") as f:
        for idex, line in enumerate(f, 1):
            data = json.loads(line)
            hash = data["hash"]
            block_timestamp = data["block_timestamp"]
            fee = data["fee"]
            inputs = data.get("inputs", [])
            outputs = data.get("outputs", [])
            for i in inputs:
                buffer.append({
                    "hash": hash,
                    "block_timestamp": block_timestamp,
                    "fee": fee,
                    "input_address": i.get("addresses",[""])[0],
                    "input_value": i.get("value",0),
                    "output_address": None,
                    "output_value": None,
                })
            for i in outputs:
                buffer.append({
                    "hash": hash,
                    "block_timestamp": block_timestamp,
                    "fee": fee,
                    "input_address": None,
                    "input_value": None,
                    "output_address": i.get("addresses",[""])[0],
                    "output_value": i.get("value",0)
                })

            if idex % read_chunk == 0:
                df = pd.DataFrame(buffer)
                df = _ensure_cols(df)
                df.to_sql(table_name, engine, if_exists="append",
                          index=False, method="multi", chunksize=write_chunk)
                total += len(df)
                buffer.clear()
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Uploaded {total} rows so far...")

    # 冲洗尾巴（放在 for 外！）
        if buffer:
            df = pd.DataFrame(buffer)
            df = _ensure_cols(df)
            df.to_sql(table_name, engine, if_exists="append",
                    index=False, method="multi", chunksize=write_chunk)
            total += len(df)
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Uploaded {total} rows (final batch)")

    print("upload done")
                

def upload_btcusd(df,table_name):
    user = "root"
    password = "12345678"
    host = "localhost"
    port = "3306"
    database = "interview"
    engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}")
    df.to_sql(table_name, engine, if_exists="append", index=False)
    print("btcusd upload done")



if __name__ == "__main__":
    file_path = "btc_transactions.json"
    # # data = read_trans(file_path)
    # # print(data.columns)
    upload_to_db("raw_transaction",file_path,read_chunk = 5000,write_chunk = 5000)
    # df = pd.read_csv("btcusd_data.csv")
    # df["Timestamp"] = pd.to_datetime(df["Timestamp"], unit="s", utc=True).dt.tz_convert(None)
    # upload_btcusd(df,"raw_rates")
    
    
            