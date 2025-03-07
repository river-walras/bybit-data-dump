# Bybit Data Dumper

A Python tool for downloading historical trading data from Bybit's public data repository.

## Features

- Downloads historical trade data and kline (candlestick) data from Bybit
- Supports both spot and contract markets
- Automatically handles data pagination and retries
- Converts downloaded CSV files to Parquet format for efficient storage
- Supports filtering by quote currency (e.g., USDT, USDC, etc.)
- Configurable date ranges for data collection

## Supported Data Types

- **Trades**: Individual trade records
  - Spot market fields: id, timestamp, price, volume, side
  - Contract market fields: timestamp, symbol, side, size, price, tickDirection, trdMatchID, grossValue, homeNotional, foreignNotional

- **Klines**: Candlestick data with the following frequencies:
  - 1 minute (1m)
  - 5 minutes (5m)
  - 15 minutes (15m)
  - 30 minutes (30m)
  - 60 minutes (60m)

## Installation

```bash
# Install required dependencies
pip install pandas aiohttp tenacity tqdm
```

## Usage

```python
import datetime
from bybit_dump.dump import DataDumper

# Initialize the dumper
dumper = DataDumper(
    asset_type="contract",  # "spot" or "contract"
    quote_currency="USDT",  # Filter symbols by quote currency
    symbols=["BTCUSDT", "ETHUSDT"],  # Specific symbols to download (optional)
    start_date=datetime.date(2024, 1, 1),  # Start date (optional)
    end_date=datetime.date(2024, 2, 1),    # End date (optional)
    save_dir="./data",  # Directory to save data (optional)
)

# Download trade data
dumper.dump_symbols(data_type="trades")

# Download kline data
dumper.dump_symbols(data_type="klines", freq="1m")
```

## Data Storage

- Data is saved in Parquet format for efficient storage and querying
- Directory structure:
  ```
  data/
  ├── spot/
  │   ├── trades/
  │   │   └── YYYY-MM-DD/
  │   └── klines/
  │       └── YYYY-MM/
  └── contract/
      ├── trades/
      │   └── YYYY-MM-DD/
      └── klines/
          └── YYYY-MM/
  ```

## Limitations

- Historical data availability starts from December 18, 2020
- Data is available up to the previous day
- Rate limits and retry mechanisms are implemented to handle API restrictions

## Error Handling

- Implements automatic retries for common HTTP errors
- Logs errors and warnings to `bybit_dump.log`
- Skips unavailable data periods gracefully

## Dependencies

- pandas: Data manipulation and Parquet file handling
- aiohttp: Asynchronous HTTP requests
- tenacity: Retry mechanism
- tqdm: Progress bars
