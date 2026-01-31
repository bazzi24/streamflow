import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from keras.models import Sequential, load_model
from keras.layers import LSTM, Dense, Dropout
from keras import backend as K
import gc, os, datetime
from dotenv import load_dotenv

load_dotenv()


def create_sequences(data, seq_length):
    X, y = [], []
    for i in range(len(data) - seq_length):
        X.append(data[i:(i + seq_length), :])
        y.append(data[i + seq_length, 0])  
    return np.array(X), np.array(y)


def train_and_predict_incremental():
    spark = SparkSession.builder \
        .appName("LSTM_StockML_Incremental") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.2.23") \
        .config("spark.driver.memory", "6g") \
        .getOrCreate()

    db_url = os.getenv("DB_URL")
    db_props = {
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "driver": os.getenv("DB_DRIVER")
    }

    
    print("Reading data from ml_data.feature_data ...")
    feature_df = spark.read.jdbc(url=db_url, table="ml_data.feature_data", properties=db_props)
    pandas_df = feature_df.toPandas()
    pandas_df['tradingdate'] = pd.to_datetime(pandas_df['tradingdate'])
    pandas_df = pandas_df.sort_values(by=['symbol', 'tradingdate'])

    
    try:
        model_info_df = spark.read.jdbc(url=db_url, table="ml_data.model_info", properties=db_props).toPandas()
    except Exception:
        model_info_df = pd.DataFrame(columns=['symbol', 'last_train_date'])

    features = [
        'last_price', 'avg_price', 'ref_price', 'total_val',
        'change', 'ratio_change', 'highest', 'lowest'
    ]

    print(f"Quantity symbol: {pandas_df['symbol'].nunique()}")
    all_target, prediction_log, model_info_list = pd.DataFrame(), pd.DataFrame(), []

    
    for symbol in pandas_df['symbol'].unique():
        symbol_df = pandas_df[pandas_df['symbol'] == symbol].copy()
        print(f"\nHandle symbol: {symbol} ({len(symbol_df)} dòng)")

        if len(symbol_df) < 3:
            print(f"{symbol}: There's too little data, ignore it..")
            continue

        model_path = f"models/LSTM_{symbol}.h5"

        
        if symbol in model_info_df['symbol'].values:
            last_train_date = pd.to_datetime(model_info_df.loc[model_info_df['symbol'] == symbol, 'last_train_date'].values[0])
        else:
            last_train_date = None

        
        if last_train_date and os.path.exists(model_path):
            print(f"Loading the old model for {symbol} (already trained to {last_train_date.date()})")
            model = load_model(model_path)
            symbol_new_df = symbol_df[symbol_df['tradingdate'] > last_train_date]
        else:
            print(f"Tạo mới model cho {symbol}")
            model = Sequential([
                LSTM(32, input_shape=(2, len(features))),
                Dropout(0.1),
                Dense(1)
            ])
            model.compile(optimizer='adam', loss='mean_squared_error')
            symbol_new_df = symbol_df.copy()

        
        if symbol_new_df.empty:
            print(f"{symbol}: No new data, skip..")
            continue

       
        scaler = MinMaxScaler(feature_range=(0, 1))
        scaled_data = scaler.fit_transform(symbol_df[features].values)

        seq_len = 2
        X, y = create_sequences(scaled_data, seq_len)
        if len(X) == 0:
            continue

        if last_train_date:
            
            idx_start = len(symbol_df[symbol_df['tradingdate'] <= last_train_date])
            if idx_start < len(scaled_data) - seq_len:
                X_new, y_new = create_sequences(scaled_data[idx_start:], seq_len)
                print(f"Sequential training with the new {len(X_new)} sample.")
                model.fit(X_new, y_new, epochs=5, batch_size=1, verbose=0)
            else:
                print(f"Insufficient new data for training. {symbol}.")
        else:
            print(f"Training new models for {symbol}")
            model.fit(X, y, epochs=15, batch_size=1, verbose=0)

        
        last_seq = scaled_data[-seq_len:]
        next_scaled = model.predict(np.expand_dims(last_seq, axis=0), verbose=0)
        next_full = np.zeros((1, scaled_data.shape[1]))
        next_full[:, 0] = next_scaled.flatten()
        predicted_next_day_price = scaler.inverse_transform(next_full)[:, 0][0]

        latest_date = symbol_df['tradingdate'].iloc[-1]
        latest_price = symbol_df['last_price'].iloc[-1]

        all_target = pd.concat([all_target, pd.DataFrame([{
            'symbol': symbol,
            'tradingdate': latest_date,
            'target_next_day_price': round(predicted_next_day_price, 2)
        }])], ignore_index=True)

        prediction_log = pd.concat([prediction_log, pd.DataFrame([{
            'symbol': symbol,
            'tradingdate': latest_date,
            'last_price': round(latest_price, 2),
            'predicted_next_day_price': round(predicted_next_day_price, 6),
            'model_name': f"LSTM_{symbol}",
            'model_type': "LSTM"
        }])], ignore_index=True)

        # Cập nhật thông tin model
        model_info_list.append({
            'symbol': symbol,
            'model_name': f"LSTM_{symbol}",
            'model_type': "LSTM",
            'model_path': model_path,
            'last_train_date': latest_date,
            'mse': float('nan'),
            'mae': float('nan'),
            'r2': float('nan')
        })

        # Lưu lại model
        os.makedirs("models", exist_ok=True)
        model.save(model_path, overwrite=True)
        print(f"Model {symbol} has been saved at {model_path}")

        K.clear_session()
        gc.collect()

   
    print("\nPush data to PSQL...")

    if not all_target.empty:
        spark.createDataFrame(all_target).write.jdbc(
            url=db_url, table="ml_data.target_data", mode="append", properties=db_props
        )
    if not prediction_log.empty:
        spark.createDataFrame(prediction_log).write.jdbc(
            url=db_url, table="ml_data.prediction_log", mode="append", properties=db_props
        )
    if model_info_list:
        spark.createDataFrame(pd.DataFrame(model_info_list)).write.jdbc(
            url=db_url, table="ml_data.model_info", mode="overwrite", properties=db_props
        )

    print("The following tables have been updated: target_data, prediction_log, model_info")
    spark.stop()

if __name__ == "__main__":
    train_and_predict_incremental()
