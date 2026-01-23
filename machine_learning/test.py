import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_squared_error
from keras.models import Sequential
from keras.layers import LSTM, Dense, Dropout
from keras import backend as K
import tensorflow as tf
import gc, os

# -----------------------------
# 1️⃣ Tạo sequence cho LSTM
# -----------------------------
def create_sequences(data, seq_length):
    X, y = [], []
    for i in range(len(data) - seq_length):
        X.append(data[i:(i + seq_length), :])
        y.append(data[i + seq_length, 0])  # Dự đoán cột last_price
    return np.array(X), np.array(y)

# -----------------------------
# 2️⃣ Hàm chính
# -----------------------------
def daily_learning_prediction():
    # Kết nối Spark
    spark = SparkSession.builder \
        .appName("StockML_LSTM_Predict") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.2.23") \
        .config("spark.driver.memory", "6g") \
        .getOrCreate()

    db_url = "jdbc:postgresql://localhost:5432/stock_ml"
    db_props = {
        "user": "bazzi",
        "password": "bazzi123",
        "driver": "org.postgresql.Driver"
    }

    # -----------------------------
    # 3️⃣ Đọc dữ liệu feature từ DB
    # -----------------------------
    print("📥 Đang đọc dữ liệu feature từ DB stock_ml.feature ...")
    feature_df = spark.read.jdbc(url=db_url, table="ml_data.feature_data", properties=db_props)
    feature_df = feature_df.filter(col("tradingdate").isNotNull())
    pandas_df = feature_df.toPandas()

    pandas_df['tradingdate'] = pd.to_datetime(pandas_df['tradingdate'])
    pandas_df = pandas_df.sort_values(by=['symbol', 'tradingdate']).reset_index(drop=True)

    features = [
        'last_price', 'avg_price', 'ref_price', 'total_val',
        'change', 'ratio_change', 'highest', 'lowest'
    ]

    print(f"🔹 Số lượng symbol: {pandas_df['symbol'].nunique()}")
    all_predictions = pd.DataFrame()

    # -----------------------------
    # 4️⃣ Huấn luyện và dự đoán từng mã
    # -----------------------------
    for symbol in pandas_df['symbol'].unique():
        symbol_df = pandas_df[pandas_df['symbol'] == symbol].copy()
        print(f"\n🚀 Symbol: {symbol} ({len(symbol_df)} bản ghi)")

        if len(symbol_df) < 5:
            print(f"⚠️ {symbol}: Dữ liệu quá ít, bỏ qua.")
            continue

        # Chuẩn hóa dữ liệu
        scaler = MinMaxScaler(feature_range=(0, 1))
        scaled_data = scaler.fit_transform(symbol_df[features].values)

        seq_length = 3
        X, y = create_sequences(scaled_data, seq_length)

        if len(X) == 0:
            print(f"⚠️ {symbol}: Không đủ dữ liệu để tạo sequence.")
            continue

        # Huấn luyện theo từng ngày (incremental)
        train_size = int(len(X) * 0.8)
        X_train, X_test = X[:train_size], X[train_size:]
        y_train, y_test = y[:train_size], y[train_size:]

        model = Sequential([
            LSTM(32, input_shape=(X_train.shape[1], X_train.shape[2])),
            Dropout(0.1),
            Dense(1)
        ])
        model.compile(optimizer='adam', loss='mean_squared_error')
        model.fit(X_train, y_train, epochs=10, batch_size=1, verbose=0)

        # Dự đoán test set
        preds = model.predict(X_test)
        preds_full = np.zeros((len(preds), scaled_data.shape[1]))
        preds_full[:, 0] = preds.flatten()
        pred_prices = scaler.inverse_transform(preds_full)[:, 0]

        y_test_full = np.zeros((len(y_test), scaled_data.shape[1]))
        y_test_full[:, 0] = y_test.flatten()
        true_prices = scaler.inverse_transform(y_test_full)[:, 0]

        mse = mean_squared_error(true_prices, pred_prices)
        print(f"✅ {symbol}: MSE = {mse:.6f}")

        # Dự đoán giá ngày tiếp theo (next-day prediction)
        last_seq = scaled_data[-seq_length:]
        next_scaled = model.predict(np.expand_dims(last_seq, axis=0))
        next_full = np.zeros((1, scaled_data.shape[1]))
        next_full[:, 0] = next_scaled.flatten()
        predicted_next_day_price = scaler.inverse_transform(next_full)[:, 0][0]

        latest_date = symbol_df['tradingdate'].iloc[-1]
        latest_price = symbol_df['last_price'].iloc[-1]

        all_predictions = pd.concat([all_predictions, pd.DataFrame([{
            'symbol': symbol,
            'tradingdate': latest_date.strftime("%Y-%m-%d"),
            'last_price': round(latest_price, 2),
            'predicted_next_day_price': round(predicted_next_day_price, 2)
        }])], ignore_index=True)

        # Giải phóng bộ nhớ
        K.clear_session()
        gc.collect()

    # -----------------------------
    # 5️⃣ Lưu kết quả vào DB target
    # -----------------------------
    print("\n💾 Lưu kết quả dự đoán vào bảng stock_ml.target ...")

    spark_target_df = spark.createDataFrame(all_predictions)
    spark_target_df.write.jdbc(
        url=db_url,
        table="ml_data.target_data",
        mode="append",   # Ghi thêm từng batch kết quả
        properties=db_props
    )

    print("✅ Đã lưu thành công vào stock_ml.target")
    spark.stop()


# -----------------------------
# 6️⃣ Chạy chính
# -----------------------------
if __name__ == "__main__":
    daily_learning_prediction()
