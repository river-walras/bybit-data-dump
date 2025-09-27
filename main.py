from bybit_dump import DataDumper
import datetime
import logging

# Configure logging for the application
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler("bybit_dump.log"),
        logging.StreamHandler()
    ]
)

dumper = DataDumper(
    asset_type="contract",
    quote_currency="USDT",
)

dumper.dump_symbols(
    data_type="klines",
    freq="1d",
    start_date=datetime.datetime(2024, 1, 1),
)
