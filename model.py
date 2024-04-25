import pandas as pd
import numpy as np
import pathlib as pl
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.ensemble import RandomForestRegressor

path = pl.Path (f"archive")
df_list = [] 

for file in path.iterdir():
    df_list.append(pd.read_csv(file))

dfraw = pd.concat(df_list, ignore_index=True)

dfraw.drop (dfraw.iloc [:, 0 : 29], axis = 1, inplace = True) # until latitude

dfraw.drop (dfraw.iloc [:, 11 :], axis = 1, inplace = True)
dfraw.drop (dfraw.iloc [:, 2 : 4], axis = 1, inplace = True) # just lat long
#dfraw.drop (dfraw.iloc [:, 0 : 3], axis = 1, inplace = True)  # just room_type
#dfraw.drop (dfraw.iloc [:, 0 : 4], axis = 1, inplace = True)  # no room_type and no latlong

dfraw['bathrooms'] = dfraw['bathrooms_text'].str.extract("(\d*\.?\d+)", expand=False)
dfraw['bathrooms'] = np.where(dfraw['bathrooms_text'].str.contains("half", case=False, na=False), 0.5, dfraw['bathrooms'])
dfraw = dfraw.dropna() # drop NaN values
dfraw["number_of_guests"] = dfraw["accommodates"]
df = dfraw.drop(["accommodates", "bathrooms_text"], axis=1)

df ["price"] = df ["price"].str.replace ("$", "", regex = False)
df ["price"] = df ["price"].str.replace (",", "", regex = False)
df ["price"] = df ["price"].astype (np.float32, copy = False)
df ["price"] = df ["price"].astype (np.int32, copy = False)
df ["number_of_guests"] = df ["number_of_guests"].astype (np.int32, copy = False)
df ["bedrooms"] = df ["bedrooms"].astype (np.int32, copy = False)
df ["beds"] = df ["beds"].astype (np.int32, copy = False)
df ["bathrooms"] = df ["bathrooms"].astype (np.float32, copy = False)
df ["latitude"] = df ["latitude"].astype (np.float32, copy = False)
df ["longitude"] = df ["longitude"].astype (np.float32, copy = False)

def outlier (col):
    q1 = col.quantile (0.25)
    q3 = col.quantile (0.75)
    iqr = q3 - q1
    lenght = 1.5
    return q1 - lenght * iqr, q3 + lenght * iqr

def remove (ds, col):
    before = ds.shape [0]
    low, up = outlier (ds [col])
    ds = ds.loc [(ds [col] >= low) & (ds [col] <= up), :]
    return ds, before - ds.shape [0]

df, removed = remove (df, "price")

df ["num_amenities"] = df ["amenities"].str.split (",").apply (len)
df ["num_amenities"] = df ["num_amenities"].astype (np.int32, copy = False)
dfenc = df.drop ("amenities", axis = 1)

def evaluate (ytest, prediction):
    rmse = np.sqrt (mean_squared_error (ytest, prediction))
    mae = mean_absolute_error (ytest, prediction)
    r2 = r2_score (ytest, prediction)
    return f"\n-----\nModel: Random Forest \nRMSE: {rmse:.2f}\nMAE: {mae:.2f}\nR2: {r2:.2%}\n-----\n"

y = dfenc ["price"]
x = dfenc.drop ("price", axis = 1)
xtrain, xtest, ytrain, ytest = train_test_split (x, y)

model = RandomForestRegressor()
print("Training model...")
model.fit (xtrain, ytrain)
print("Model training done!")

if __name__ == "__main__":
    prediction = model.predict (xtest)
    print (evaluate (ytest, prediction))