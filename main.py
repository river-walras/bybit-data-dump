from bybit_dump import DataDumper
import datetime

dumper = DataDumper(
    asset_type="contract",
    quote_currency="USDT",
)

dumper.dump_symbols(
    data_type="klines",
    freq="1m",
    start_date=datetime.datetime(2024, 1, 1),
)
