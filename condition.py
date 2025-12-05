import pandas as pd
import numpy as np
df1 = pd.read_csv(r"cards_details\yunusabad_cards_details.csv")   # title, url, price_raw, price_value, ...
df2 = pd.read_csv(r"district_listing_page_cleaned\yunusabad_cleaned.csv")   # card_id, area, number_rooms, furniture, ...

merged = pd.merge(df1, df2, on="card_id", how="inner")
merged = merged.drop(columns=["location_text", "posted_date_raw", "posted_date", "time_raw"])


merged["price_uzs"] = np.where(merged["price_currency"] == "сум", merged["price_value"], merged["price_value"] * 13933)

merged["area"] = (
    merged["area"]
    .astype(str)
    .str.extract(r"(\d+\.?\d*)")   # get only the number
    .astype(float)
)

merged["price_per_sq_meter"] = merged["price_uzs"] / merged["area"]
merged["condition"] = merged["condition"].fillna("Not Specified")
average_price_per_sq_meter_by_condition = merged.groupby("condition")["price_per_sq_meter"].mean()

print(average_price_per_sq_meter_by_condition.head())
