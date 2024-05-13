import pandas as pd
import statistics
from joblib import load

features_comparison = {
    "bathrooms": 0.03,
    "bedrooms": 0.05,
    "beds": 0.08,
    "number_of_guests": 0.07,
    "num_amenities": 0.02
}

def calclate_features_difference(predictions_dict, properties_dict, median_id):
    recommended_prices = {} # id : recommended price

    for property_id in predictions_dict.keys():
        if property_id == median_id:
            recommended_prices[property_id] = predictions_dict[property_id]
        else:
            property = properties_dict[property_id]
            median_property = properties_dict[median_id]
            median_price = predictions_dict[median_id]
            total_difference = 0

            for feature in features_comparison.keys():
                feature_diff = property[feature] - median_property[feature]
                total_difference += feature_diff * features_comparison[feature]

            recommended_prices[property_id] = median_price * (1+total_difference)

    return recommended_prices


def calcualte_recommended_price_by_model(X):
    model = load("model.joblib")
    
    predictions_dict = {} # id: price predicted
    properties_dict = {} # id: property

    for _, property in X.iterrows():
        property_df = pd.DataFrame([property]).drop("id", axis=1)
        prediction = model.predict(property_df)
        predictions_dict[property["id"]] = prediction[0]
        properties_dict[property["id"]] = property

    median_price = statistics.median_high(predictions_dict.values())
    median_id = next(key for key, value in predictions_dict.items() if value == median_price)

    recommended_prices = calclate_features_difference(predictions_dict, properties_dict, median_id)
    
    return recommended_prices