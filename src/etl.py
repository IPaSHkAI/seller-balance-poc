import pandas as pd
from pathlib import Path

def validate(df: pd.DataFrame) -> None:
    assert df['event_id'].isnull().sum() == 0, "Null event_id"
    assert df['event_ts'].isnull().sum() == 0, "Null event_ts"
    assert df['seller_id'].isnull().sum() == 0, "Null seller_id"

def main():
    data_path = Path(__file__).resolve().parents[1] / "data" / "events.csv"
    df = pd.read_csv(data_path)
    validate(df)
    print("Rows:", len(df))
    print("Sellers:", df['seller_id'].nunique())
    print("Date range:", df['event_ts'].min(), "->", df['event_ts'].max())
    # You can add transformation/export logic here

if __name__ == "__main__":
    main()
