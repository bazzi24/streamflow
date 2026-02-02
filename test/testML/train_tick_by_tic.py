import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from sklearn.preprocessing import MinMaxScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
import tensorflow as tf
from keras.models import Sequential
from keras.layers import LSTM, Dense, Dropout
import os
from dotenv import load_dotenv

load_dotenv()

def create_sequences(data, sequence_length):
    X, y = [], []
    for i in range(len(data) - sequence_length):
        X.append(data[i:(i + sequence_length), :])
        y.append(data[i + sequence_length, 0]) # Predict 'last_price' of the next step
    return np.array(X), np.array(y)

def create_prediction_model_lstm():
    spark = SparkSession.builder \
        .appName("StockPricePredictionLSTM") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.2.23") \
        .config("spark.driver.memory", "6g") \
        .config("spark.executor.memory", "6g") \
        .config("spark.memory.fraction", "0.8") \
        .config("spark.memory.storageFraction", "0.3") \
        .config("spark.memory.storageFraction", "0.3") \
        .config("spark.sql.debug.maxToStringFields", "2000") \
        .getOrCreate()

    dw_db_url = os.getenv("DW_DB_URL")
    dw_db_properties = {
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "driver": os.getenv("DB_DRIVER")
    }

    # Load data from data warehouse
    fact_trade_df = spark.read.jdbc(url=dw_db_url, table="fact.stocktrade", properties=dw_db_properties)
    dim_symbol_df = spark.read.jdbc(url=dw_db_url, table="dim.symbol", properties=dw_db_properties)
    dim_date_df = spark.read.jdbc(url=dw_db_url, table="dim.date", properties=dw_db_properties)

    # Join data for prediction
    joined_df = fact_trade_df.alias("ft") \
        .join(dim_symbol_df.alias("ds"), col("ft.symbol_key") == col("ds.symbol_key"), "left") \
        .join(dim_date_df.alias("dd"), col("ft.tradingdate_key") == col("dd.tradingdate_key"), "left") \
        .select(
            col("ds.symbol"),
            col("dd.tradingdate"),
            col("ft.last_price"),
            col("ft.avg_price"),
            col("ft.ref_price"),
            col("ft.total_val"),
            col("ft.change"),
            col("ft.ratio_change"),
            col("ft.highest"),
            col("ft.lowest")
        )

    # Convert to Pandas DataFrame for scikit-learn and TensorFlow/Keras
    pandas_df = joined_df.toPandas()

    # Preprocessing for LSTM
    pandas_df['tradingdate'] = pd.to_datetime(pandas_df['tradingdate'])
    pandas_df = pandas_df.sort_values(by=['symbol', 'tradingdate'])

    # Features to use for prediction
    features = ['last_price', 'avg_price', 'ref_price', 'total_val', 'change', 'ratio_change', 'highest', 'lowest']
    
    all_predictions = pd.DataFrame()

    for symbol in pandas_df['symbol'].unique():
        symbol_df = pandas_df[pandas_df['symbol'] == symbol].copy()
        if len(symbol_df) < 2: # Need at least 2 data points for sequence creation
            continue

        data = symbol_df[features].values

        # Scale the data
        scaler = MinMaxScaler(feature_range=(0, 1))
        scaled_data = scaler.fit_transform(data)

        sequence_length = 10 # Number of past days to consider for prediction
        X, y = create_sequences(scaled_data, sequence_length)

        if len(X) == 0:
            continue

        # Split data into training and testing sets
        # Using time-series split for more realistic evaluation
        train_size = int(len(X) * 0.8)
        X_train, X_test = X[:train_size], X[train_size:]
        y_train, y_test = y[:train_size], y[train_size:]

        # Reshape data for LSTM [samples, time_steps, features]
        X_train = np.reshape(X_train, (X_train.shape[0], X_train.shape[1], X_train.shape[2]))
        X_test = np.reshape(X_test, (X_test.shape[0], X_test.shape[1], X_test.shape[2]))

        # Build the LSTM model
        model = Sequential()
        model.add(LSTM(units=50, return_sequences=True, input_shape=(X_train.shape[1], X_train.shape[2])))
        model.add(Dropout(0.2))
        model.add(LSTM(units=50, return_sequences=False))
        model.add(Dropout(0.2))
        model.add(Dense(units=1)) # Output layer for predicting one value (last_price)

        model.compile(optimizer='adam', loss='mean_squared_error')

        # Train the model
        model.fit(X_train, y_train, epochs=20, batch_size=32, verbose=0)

        # Make predictions
        predictions = model.predict(X_test)

        # Inverse transform the predictions to original scale
        # We need to create a dummy array with the same number of features as scaled_data
        # and replace the 'last_price' column with predictions
        predictions_full_scale = np.zeros((len(predictions), scaled_data.shape[1]))
        predictions_full_scale[:, 0] = predictions.flatten() # Put predictions into the first feature column
        predicted_prices = scaler.inverse_transform(predictions_full_scale)[:, 0]

        # Inverse transform actual values for comparison
        y_test_full_scale = np.zeros((len(y_test), scaled_data.shape[1]))
        y_test_full_scale[:, 0] = y_test.flatten()
        actual_prices = scaler.inverse_transform(y_test_full_scale)[:, 0]

        # Evaluate the model
        mse = mean_squared_error(actual_prices, predicted_prices)
        print(f"Symbol: {symbol}, Mean Squared Error: {mse}")

        # Predict the next day's price for the current symbol
        last_sequence = scaled_data[-sequence_length:]
        last_sequence = np.reshape(last_sequence, (1, sequence_length, scaled_data.shape[1]))
        
        next_day_prediction_scaled = model.predict(last_sequence)
        next_day_prediction_full_scale = np.zeros((1, scaled_data.shape[1]))
        next_day_prediction_full_scale[:, 0] = next_day_prediction_scaled.flatten()
        predicted_next_day_price = scaler.inverse_transform(next_day_prediction_full_scale)[:, 0][0]

        # Store the prediction
        latest_date = symbol_df['tradingdate'].iloc[-1]
        latest_price = symbol_df['last_price'].iloc[-1]
        
        all_predictions = pd.concat([all_predictions, pd.DataFrame([{
            'symbol': symbol,
            'tradingdate': latest_date,
            'last_price': latest_price,
            'predicted_next_day_price': predicted_next_day_price
        }])], ignore_index=True)

    # Save predictions to a CSV file
    output_path = "stock_predictions_lstm.csv"
    all_predictions.to_csv(output_path, index=False)
    print(f"Stock price predictions (LSTM) saved to {output_path}")

    spark.stop()

if __name__ == "__main__":
    create_prediction_model_lstm()
