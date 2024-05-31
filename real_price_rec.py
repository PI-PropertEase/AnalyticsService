import pandas as pd
import numpy as np
import statistics

old_prices_df = pd.DataFrame()

def calculate_real_price_difference_by_location(real_prices_df, old_prices_df):
    
    old_prices = old_prices_df["price"].astype(np.float32, copy = False)
    prices = real_prices_df["price"].astype(np.float32, copy = False)
    old_mean = statistics.mean(old_prices)
    new_mean = statistics.mean(prices)
    general_porcentage_difference = (new_mean - old_mean) / old_mean

    recommended_prices = {}

    for _, property in real_prices_df.iterrows():
        old_price = old_prices_df[old_prices_df["id"] == property["id"]]["price"].iloc[0]
        price_change = property["price"] - old_price
        if price_change == 0 and abs(general_porcentage_difference) >= 0.10:
            recommended_prices[property["id"]] = property["price"] * (1 + general_porcentage_difference)
        else:
            recommended_prices[property["id"]] = property["price"]

    old_prices_df = real_prices_df
    return recommended_prices


def calculate_real_price_difference(real_prices_df):

    global old_prices_df
    if old_prices_df.empty:
        old_prices_df = real_prices_df
        return old_prices_df.set_index('id')['price'].to_dict()

    recommended_prices_real_price = {}
    locations = real_prices_df["location"].unique()

    for location in locations:
        real_prices_by_location_df = real_prices_df[real_prices_df["location"] == location]
        old_prices_by_location_df = old_prices_df[old_prices_df["location"] == location]
        recommended_prices_by_location = calculate_real_price_difference_by_location(real_prices_by_location_df, old_prices_by_location_df)
        recommended_prices_real_price = recommended_prices_real_price | recommended_prices_by_location

    return recommended_prices_real_price