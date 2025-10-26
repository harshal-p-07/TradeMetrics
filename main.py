import psycopg2
import pandas as pd
from fastapi import FastAPI
from contextlib import asynccontextmanager
import time
from pydantic import BaseModel


class StockData(BaseModel):
    datetime: str
    open: float
    high: float
    low: float
    close: float
    volume: int

def get_db_connection():
    try:
        conn = psycopg2.connect(
            host="db",
            database="stockdata_db",
            user="postgres",
            password="pdsql@2025",
            port="5432"
        )
        return conn
    except psycopg2.OperationalError:
        return None

def load_initial_data():
    retries = 5
    conn = None

    while retries > 0:
        conn = get_db_connection()
        if conn is not None:
            print("Database connected successfully.")
            break
        print(f"Database not ready, retrying in 3 sec... ({retries} left)")
        time.sleep(3)
        retries -= 1

    if conn is None:
        print(" Could not connect to database after multiple tries.")
        return

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM stock_data;")
            count = cur.fetchone()[0]

            if count == 0:
                print("No data found in DB. Loading from CSV...")

                df = pd.read_csv("/app/HINDALCO_1D.xlsx - HINDALCO.csv")

                if 'instrument' in df.columns:
                    df = df.drop(columns=['instrument'])
                else:
                    df = df.iloc[:, :6]

                df.columns = ["datetime", "close", "high", "low", "open", "volume"]
                for _, row in df.iterrows():
                    cur.execute(
                        """
                        INSERT INTO stock_data (datetime, open, high, low, close, volume)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (datetime) DO NOTHING;
                        """,
                        (
                            row["datetime"],
                            row["open"],
                            row["high"],
                            row["low"],
                            row["close"],
                            int(row["volume"])
                        )
                    )
                conn.commit()
                print("Data successfully inserted into DB!")
            else:
                print("Database already has data, skipping load.")
    except Exception as e:
        print(f" Error while loading data: {e}")

    finally:
        if conn:
            conn.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Starting application and loading initial data...")
    load_initial_data()
    yield
    print("Shutting down application...")

app = FastAPI(lifespan=lifespan)


@app.get("/")
def home():
    return {"message": "Welcome to TradeMetrics API"}


@app.get("/data")
def get_data():
    conn = get_db_connection()
    if conn is None:
        return {"error": "Database connection failed"}

    with conn.cursor() as cur:
        cur.execute("SELECT * FROM stock_data;")
        rows = cur.fetchall()
    conn.close()
    return {"data": rows}


@app.post("/data")
def insert_data(data: StockData):
    conn = get_db_connection()
    if conn is None:
        return {"error": "Database connection failed"}
    cur = conn.cursor()
    query = """
        INSERT INTO stock_data (datetime, open, high, low, close, volume)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    cur.execute(query, (data.datetime, data.open, data.high, data.low, data.close, data.volume))
    conn.commit()
    cur.close()
    conn.close()
    return {"message": "Record inserted successfully!"}


@app.get("/strategy/performance")
def strategy_performance(short_window: int = 5, long_window: int = 20):
    conn = get_db_connection()
    if conn is None:
        return {"error": "Database connection failed"}

    query = "SELECT datetime, close FROM stock_data ORDER BY datetime ASC;"
    df = pd.read_sql_query(query, conn)
    conn.close()

    if df.empty:
        return {"error": "No data available"}

   
    df["SMA_short"] = df["close"].rolling(window=short_window).mean()
    df["SMA_long"] = df["close"].rolling(window=long_window).mean()

   
    df["Signal"] = 0
    df.loc[df["SMA_short"] > df["SMA_long"], "Signal"] = 1
    df.loc[df["SMA_short"] < df["SMA_long"], "Signal"] = -1

    
    df["Return"] = df["close"].pct_change()
    df["Strategy_Return"] = df["Signal"].shift(1) * df["Return"]
    total_return = (df["Strategy_Return"] + 1).prod() - 1

    return {
        "short_window": short_window,
        "long_window": long_window,
        "total_return_percent": round(total_return * 100, 2),
        "data_points": len(df)
    }
