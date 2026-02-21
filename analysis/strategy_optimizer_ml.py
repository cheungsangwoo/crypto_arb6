import pandas as pd
import numpy as np
import os
import joblib
from sqlalchemy import create_engine
from sklearn.ensemble import RandomForestRegressor  # [CHANGED] Classifier -> Regressor
from sklearn.model_selection import TimeSeriesSplit
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_absolute_error, r2_score
from dotenv import load_dotenv

load_dotenv(override=True)

# --- CONFIGURATION ---
LOOKAHEAD_WINDOW = 240  # 4 Hours
FEE_RATE = 0.0015  # 0.15% round-trip
MIN_PROFIT_FILTER = 0.001  # Only take trades with >0.1% expected net profit


def get_db_engine():
    user = os.getenv("DB_USER", "root")
    password = os.getenv("DB_PASSWORD", "")
    host = os.getenv("DB_HOST", "127.0.0.1")
    port = os.getenv("DB_PORT", "3306")
    db_name = os.getenv("DB_NAME", "crypto_arb3")
    DATABASE_URL = f"mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}"
    return create_engine(DATABASE_URL)


def engineer_advanced_features(df):
    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values(["symbol", "timestamp"])

    # 1. Base Features
    df["ba_spread"] = df["spot_ask_price"] - df["spot_bid_price"]
    market_avg = df.groupby("timestamp")["kimchi_premium_pct"].transform("mean")
    df["relative_premium"] = df["kimchi_premium_pct"] - market_avg

    # 2. Stateful Features (Z-Scores & Momentum)
    for window in [5, 30, 60]:
        roll = df.groupby("symbol")["kimchi_premium_pct"].rolling(window)
        df[f"prem_z_{window}"] = (df["kimchi_premium_pct"] - roll.mean().values) / (
            roll.std().values + 1e-9
        )

    df["premium_momentum"] = df.groupby("symbol")["kimchi_premium_pct"].diff()

    # 3. [CRITICAL] Profit-Based Target Labeling
    # Calculate the max exit premium available in the next 4 hours
    future_max_exit = (
        df.groupby("symbol")["exit_premium_pct"]
        .shift(-1)
        .rolling(window=LOOKAHEAD_WINDOW, min_periods=1)
        .max()
    )

    # Target = (Best Possible Exit - Current Entry) - Fees
    # This is a decimal value (e.g., 0.005 = 0.5% net profit)
    df["target_net_profit"] = (
        (future_max_exit - df["entry_premium_pct"]) / 100
    ) - FEE_RATE

    return df.dropna()


def run_advanced_optimization():
    print("⏳ Loading data for Profit-Regressor Training...")
    # Training on [72h - 24h] to avoid datamining the test set
    engine = get_db_engine()
    query = """
        SELECT * FROM strategy_collector 
        WHERE timestamp BETWEEN '2026-02-08 06:59:04' AND '2026-02-10 06:59:04'
        ORDER BY timestamp ASC
    """
    df = pd.read_sql(query, engine)
    df_feat = engineer_advanced_features(df)

    features = [
        "entry_premium_pct",
        "funding_rate",
        "annualized_funding_pct",
        "ba_spread",
        "premium_momentum",
        "relative_premium",
        "prem_z_5",
        "prem_z_30",
        "prem_z_60",
    ]

    X = df_feat[features]
    y = df_feat["target_net_profit"]

    # Time-Series Validation
    tscv = TimeSeriesSplit(n_splits=3)
    scaler = StandardScaler()
    model = RandomForestRegressor(
        n_estimators=100, max_depth=12, random_state=42, n_jobs=-1
    )

    print(f"🔎 Validating Profit Potential on {len(df_feat)} rows...")

    for fold, (train_idx, test_idx) in enumerate(tscv.split(X)):
        X_train, X_test = X.iloc[train_idx], X.iloc[test_idx]
        y_train, y_test = y.iloc[train_idx], y.iloc[test_idx]

        X_t_scaled = scaler.fit_transform(X_train)
        model.fit(X_t_scaled, y_train)

        X_v_scaled = scaler.transform(X_test)
        preds = model.predict(X_v_scaled)

        mae = mean_absolute_error(y_test, preds)
        print(f"   Fold {fold} | Mean Error: {mae:.4%} profit")

    # Save final production model
    print("💾 Saving final Profit-Regressor...")
    X_final_scaled = scaler.fit_transform(X)
    model.fit(X_final_scaled, y)

    joblib.dump(model, "analysis/arb_model.pkl")
    joblib.dump(scaler, "analysis/scaler.pkl")
    print("✅ Advanced Model & Scaler Saved.")


if __name__ == "__main__":
    run_advanced_optimization()
