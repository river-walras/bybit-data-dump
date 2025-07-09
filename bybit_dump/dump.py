from typing import Literal, Optional
import datetime
import tenacity
import pandas as pd
from tqdm.asyncio import tqdm
import os
import asyncio
import aiohttp
import aiohttp.client_exceptions
import aiohttp.web_exceptions
import aiohttp.http_exceptions
import logging
from urllib.parse import urlparse
from fake_headers import Headers

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    filename="bybit_dump.log",
    filemode="a",
)


class DataDumper:
    _info = {"spot": {}, "contract": {}}
    _available_quote_currencies = {
        "spot": [
            "EUR",
            "BRL",
            "PLN",
            "TRY",
            "SOL",
            "BTC",
            "ETH",
            "DAI",
            "BRZ",
            "USDT",
            "USDC",
            "USDE",
        ],
        "contract": ["USD", "USDT", "USDC", "FUTURE"],
    }

    @staticmethod
    def _get_date_range(
        start_date: datetime.date,
        end_date: datetime.date,
        circle: Literal["daily", "monthly"] = "daily",
    ):
        if start_date > end_date:
            raise ValueError("Start date must be before end date")

        dates = []
        current = start_date

        if circle == "monthly":
            while current <= end_date:
                # Get the first day of the current month
                first_day = datetime.date(current.year, current.month, 1)
                dates.append(first_day)

                # Move to the first day of next month
                if current.month == 12:
                    current = datetime.date(current.year + 1, 1, 1)
                else:
                    current = datetime.date(current.year, current.month + 1, 1)
        elif circle == "daily":
            while current <= end_date:
                dates.append(current)
                current = current + datetime.timedelta(days=1)

        return dates

    def __init__(
        self,
        asset_type: Literal["spot", "contract"],
        symbols: list[str] | None = None,
        start_date: datetime.date | None = None,
        end_date: datetime.date | None = None,
        save_dir: str | None = None,
        quote_currency: str | None = None,
        chunk_size: int = 1024 * 16,
        proxy: str | None = None,
    ):
        self._log = logging.getLogger("bybit_dump")
        self._loop = asyncio.get_event_loop()
        self._chunk_size = chunk_size
        self._proxy = proxy
        self._headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
            "cache-control": "no-cache",
            "pragma": "no-cache",
            "priority": "u=0, i",
            "sec-ch-ua": '"Google Chrome";v="137", "Chromium";v="137", "Not/A)Brand";v="24"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"macOS"',
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "none",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36",
        }
        # haader = Headers(
        #     browser="chrome",
        #     os="mac",
        #     headers=True,
        # )
        # self._headers = haader.generate()

        now = (
            datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1)
        ).date()

        if start_date is None:
            start_date = datetime.datetime(
                2020, 12, 18, 0, 0, 0, tzinfo=datetime.timezone.utc
            ).date()
        if end_date is None or end_date > now:
            end_date = now
        if start_date > end_date:
            raise ValueError("Start date must be before end date")

        self.asset_type = asset_type
        self._info[asset_type] = self.get_exchange_info(
            asset_type=asset_type, quote_currency=quote_currency
        )

        if symbols is None:
            self.symbols = list(self._info[asset_type].keys())
        else:
            self.symbols = symbols

        self.start_date = start_date
        self.end_date = end_date
        if save_dir is None:
            self.save_dir = os.path.join("./data", asset_type)
        else:
            self.save_dir = os.path.join(save_dir, asset_type)
        os.makedirs(self.save_dir, exist_ok=True)

    async def _get_exchange_info(
        self,
        asset_type: Literal["spot", "contract"],
    ):
        exchange_map = {"spot": "bybit-spot", "contract": "bybit"}
        async with aiohttp.ClientSession(trust_env=True, proxy=self._proxy) as session:
            async with session.get(
                f"https://api.tardis.dev/v1/exchanges/{exchange_map[asset_type]}"
            ) as response:
                return await response.json()

    def get_exchange_info(
        self,
        asset_type: Literal["spot", "contract"],
        quote_currency: str | None = None,
    ):
        if quote_currency is not None:
            available_quote_currencies = self._available_quote_currencies[asset_type]
            if quote_currency not in available_quote_currencies:
                raise ValueError(
                    f"quote_currency {quote_currency} not in {available_quote_currencies}, must be one of {', '.join(available_quote_currencies)}"
                )

        if self._info[asset_type]:
            return self._info[asset_type]

        start = datetime.datetime(
            2020, 12, 18, 0, 0, 0, tzinfo=datetime.timezone.utc
        ).date()
        end = (
            datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1)
        ).date()

        data = self._loop.run_until_complete(
            self._get_exchange_info(asset_type=asset_type)
        )

        info = {}

        start_idx = 1 if asset_type == "spot" else 2
        for symbol in data["datasets"]["symbols"][start_idx:]:
            symbol_info = {}
            id = symbol["id"]
            symbol_info["id"] = id
            _type = symbol["type"]

            # 将ISO格式的日期字符串转换为datetime对象
            available_since = (
                datetime.datetime.strptime(
                    symbol["availableSince"].split(".")[0], "%Y-%m-%dT%H:%M:%S"
                )
                .replace(tzinfo=datetime.timezone.utc)
                .date()
            )

            available_to = (
                datetime.datetime.strptime(
                    symbol["availableTo"].split(".")[0], "%Y-%m-%dT%H:%M:%S"
                )
                .replace(tzinfo=datetime.timezone.utc)
                .date()
            )

            if available_since < start:
                available_since = start
            if available_to > end:
                available_to = end

            symbol_info["start_date"] = available_since
            symbol_info["end_date"] = available_to
            if asset_type == "spot":
                base, quote = self._parse_symbol(id, _type)
            elif asset_type == "contract":
                base, quote = self._parse_symbol(id, _type)
            symbol_info["base"] = base
            symbol_info["quote"] = quote
            if quote_currency is None or quote_currency == symbol_info["quote"]:
                info[symbol_info["id"]] = symbol_info
        self._info[asset_type] = info
        return info

    def _parse_symbol(
        self, id: str, asset_type: Literal["spot", "perpetual", "future"]
    ):
        if asset_type == "future":
            return None, "FUTURE"

        # 定义可能的后缀及其长度
        quote_suffixes = {
            "spot": {
                "EUR": 3,
                "BRL": 3,
                "PLN": 3,
                "TRY": 3,
                "SOL": 3,
                "BTC": 3,
                "ETH": 3,
                "DAI": 3,
                "BRZ": 3,
                "USDT": 4,
                "USDC": 4,
                "USDE": 4,
            },
            "perpetual": {
                "USD": 3,
                "USDT": 4,
                "PERP": 4,  # PERP实际对应的是USDC
            },
        }

        # 检查对应资产类型的后缀
        for quote, length in quote_suffixes.get(asset_type, {}).items():
            if id.endswith(quote):
                base = id[:-length]
                # 特殊情况处理
                if asset_type == "perpetual" and quote == "PERP":
                    quote = "USDC"
                return base, quote

        return None, None

    def _generate_kline_for_metatrader4_url(
        self,
        symbol: str,
        date: datetime.date,
        freq: Literal["1m", "5m", "15m", "30m", "60m"],
    ):
        if freq not in ["1m", "5m", "15m", "30m", "60m"]:
            raise ValueError(
                f"freq {freq} must be one of ['1m', '5m', '15m', '30m', '60m']"
            )

        freq_map = {
            "1m": "1",
            "5m": "5",
            "15m": "15",
            "30m": "30",
            "60m": "60",
        }

        start_of_month = datetime.date(date.year, date.month, 1)

        if date.month == 12:
            end_of_month = datetime.date(date.year + 1, 1, 1) - datetime.timedelta(
                days=1
            )
        else:
            end_of_month = datetime.date(
                date.year, date.month + 1, 1
            ) - datetime.timedelta(days=1)

        base_url = (
            f"https://public.bybit.com/kline_for_metatrader4/{symbol}/{date.year}"
        )

        file_name = f"{symbol}_{freq_map[freq]}_{start_of_month.strftime('%Y-%m-%d')}_{end_of_month.strftime('%Y-%m-%d')}.csv.gz"
        url = f"{base_url}/{file_name}"
        return {"url": url, "file_name": file_name, "date": date.strftime("%Y-%m")}

    def _generate_url_for_public_trading_history(
        self, symbol: str, date: datetime.date, asset_type: Literal["spot", "contract"]
    ):
        if asset_type == "spot":
            base_url = f"https://public.bybit.com/spot/{symbol}"
        elif asset_type == "contract":
            base_url = f"https://public.bybit.com/trading/{symbol}"

        file_name = f"{symbol}{date.strftime('%Y-%m-%d')}.csv.gz"
        url = f"{base_url}/{file_name}"
        return {"url": url, "file_name": file_name, "date": date.strftime("%Y-%m-%d")}

    def generate_url(
        self,
        symbol: str,
        data_type: Literal["klines", "trades"],
        date: datetime.date,
        asset_type: Literal["spot", "contract"] | None = None,
        freq: Literal["1m", "5m", "15m", "30m", "60m"] | None = None,
    ):
        """
        https://public.bybit.com/spot/1INCHUSDT/1INCHUSDT_2025-02-25.csv.gz
        https://public.bybit.com/trading/1000000CHEEMSUSDT/1000000CHEEMSUSDT2025-02-23.csv.gz
        https://public.bybit.com/trading/BTCUSD/BTCUSD2025-03-02.csv.gz


        https://public.bybit.com/kline_for_metatrader4/ADAUSDT/2025/ADAUSDT_1_2025-01-01_2025-01-31.csv.gz
        https://public.bybit.com/kline_for_metatrader4/ADAUSDT/2025/ADAUSDT_5_2025-02-01_2025-02-28.csv.gz
        https://public.bybit.com/kline_for_metatrader4/ADAUSDT/2025/ADAUSDT_60_2025-02-01_2025-02-28.csv.gz
        """
        if data_type == "klines":
            if freq is None:
                raise ValueError("`freq` must be provided for klines")
            if asset_type == "spot":
                raise ValueError(
                    "`asset_type` must is `contract` for `metatrader4` klines"
                )
            return self._generate_kline_for_metatrader4_url(
                symbol=symbol, date=date, freq=freq
            )
        elif data_type == "trades":
            if asset_type is None:
                raise ValueError("`asset_type` must be provided for trades")
            return self._generate_url_for_public_trading_history(
                symbol=symbol, date=date, asset_type=asset_type
            )

    # @tenacity.retry(
    #     stop=tenacity.stop_after_attempt(5),
    #     wait=tenacity.wait_exponential(exp_base=2, multiplier=4, max=64),
    # )
    async def _async_download_symbol_data(
        self,
        symbol: str,
        data_type: Literal["klines", "trades", "fundingrate"],
        date: datetime.date,
        freq: Literal["1m", "5m", "15m", "30m", "60m"] | None = None,
    ):
        asset_type = self.asset_type

        res = self.generate_url(
            symbol=symbol,
            data_type=data_type,
            date=date,
            asset_type=asset_type,
            freq=freq,
        )
        zip_path = os.path.join(self.save_dir, data_type, res["date"], res["file_name"])
        parquet_path = zip_path.replace(".csv.gz", ".parquet")

        if os.path.exists(parquet_path):
            self._log.debug(f"symbol {symbol} {data_type} {date} already exists")
            return parquet_path

        async with aiohttp.ClientSession(trust_env=True, proxy=self._proxy) as session:
            async with session.get(res["url"]) as response:
                try:
                    response.raise_for_status()
                except aiohttp.client_exceptions.ClientResponseError as e:
                    if e.status == 404:
                        self._log.warning(
                            f"symbol {symbol} {data_type} {date} not found"
                        )
                        return None
                    elif e.status in [500, 502, 503, 504, 429, 408]:
                        raise tenacity.TryAgain
                    else:
                        raise e

                os.makedirs(os.path.dirname(zip_path), exist_ok=True)
                if data_type in ["trades"]:
                    with open(zip_path, "wb") as f:
                        async for chunk in response.content.iter_chunked(
                            self._chunk_size
                        ):
                            f.write(chunk)
                else:
                    content = await response.read()
                    with open(zip_path, "wb") as f:
                        f.write(content)
                if data_type == "trades":
                    if self.asset_type == "spot":
                        names = ["id", "timestamp", "price", "volume", "side"]
                    elif self.asset_type == "contract":
                        names = [
                            "timestamp",
                            "symbol",
                            "side",
                            "size",
                            "price",
                            "tickDirection",
                            "trdMatchID",
                            "grossValue",
                            "homeNotional",
                            "foreignNotional",
                        ]
                    df = pd.read_csv(
                        zip_path,
                        names=names,
                        header=0,
                    )
                    df.to_parquet(parquet_path, index=False)
                elif data_type == "klines":
                    df = pd.read_csv(
                        zip_path,
                        names=["timestamp", "open", "high", "low", "close", "volume"],
                        header=None,
                    )
                    df["timestamp"] = pd.to_datetime(
                        df["timestamp"],
                        format="%Y.%m.%d %H:%M",  # Parse the time format
                        utc=False,  # Don't assume UTC
                    )
                    df["timestamp"] = (
                        df["timestamp"].dt.tz_localize("Etc/GMT-3").dt.tz_convert("UTC")
                    )
                    df.to_parquet(parquet_path, index=False)
                os.remove(zip_path)
        return parquet_path

    async def _get_download_url(self, symbol: str) -> str:
        """
        https://www.bybit.com/x-api/contract/v5/support/funding-rate-list-export?symbol=ETHUSDT
        https://www.bybit.com/x-api/contract/v5/support/funding-rate-list-export?symbol=BTCUSDT
        """
        params = {
            "symbol": symbol,
        }

        async with aiohttp.ClientSession(trust_env=True, proxy=self._proxy) as session:
            async with session.get(
                "https://www.bybit.com/x-api/contract/v5/support/funding-rate-list-export",
                params=params,
                headers=self._headers,
            ) as response:
                response.raise_for_status()
                data = await response.json()

                if not data.get("ret_code") == 0:
                    raise ValueError(f"Error fetching data for {symbol}: {data.get('ret_msg')}")
                
                return data["result"]["downloadUrl"]

    async def _download_from_s3_url(self, s3_url: str) -> str:
        """
        Download a file from S3 using a pre-signed URL with streaming download
        """
        # Extract filename from S3 URL if not provided
        parsed_url = urlparse(s3_url)
        local_filename = os.path.basename(parsed_url.path)
        
        async with aiohttp.ClientSession(trust_env=True, proxy=self._proxy) as session:
            async with session.get(s3_url, headers=self._headers) as response:
                response.raise_for_status()
                
                with open(local_filename, 'wb') as f:
                    async for chunk in response.content.iter_chunked(self._chunk_size):
                        f.write(chunk)
        
        self._log.debug(f"Downloaded {local_filename}")

        # Read the Excel file and convert to parquet
        df = pd.read_excel(local_filename, header=0, names=['timestamp', 'symbol', 'fundingrate'])
        
        # Set proper column types
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
        df['symbol'] = df['symbol'].astype(str)
        df['fundingrate'] = df['fundingrate'].astype(float)
        
        # Save as parquet
        parquet_filename = local_filename.replace('.xlsx', '.parquet')


        parquet_filepath = os.path.join(self.save_dir, "funding_rates", parquet_filename)
        os.makedirs(os.path.dirname(parquet_filepath), exist_ok=True)
        df.to_parquet(parquet_filepath, index=False)

        # Delete the xlsx file
        os.remove(local_filename)
        
        self._log.debug(f"Converted to parquet: {parquet_filename}")
        return parquet_filename

    # async def _aggregate_symbol_kline(self, symbol, date: datetime.date):
    #     parquet_path = await self._async_download_symbol_data(
    #         symbol=symbol, data_type="aggtrades", date=date
    #     )  # we use aggtrades to generate kline
    #     if parquet_path is None:
    #         self._log.warning(
    #             f"symbol {symbol} {date} aggtrades not found -> no kline generated"
    #         )
    #         return
    #     save_path = parquet_path.replace("aggtrades", "klines")
    #     os.makedirs(os.path.dirname(save_path), exist_ok=True)
    #     if os.path.exists(save_path):
    #         return
    #     df = pd.read_parquet(parquet_path)
    #     ohlcv = (
    #         df.set_index("timestamp")
    #         .resample("1min")
    #         .agg(
    #             {
    #                 "price": ["first", "max", "min", "last"],  # OHLC
    #                 "size": "sum",  # volume
    #             }
    #         )
    #     )
    #     ohlcv.columns = ["open", "high", "low", "close", "volume"]
    #     # For rows with no trading volume, fill ohlc with the previous row's close, i.e., ohlcv are all the previous row's close
    #     no_trade_mask = (ohlcv["volume"] == 0) | (ohlcv["volume"].isna())
    #     ohlcv.loc[no_trade_mask, ["open", "high", "low", "close"]] = ohlcv[
    #         "close"
    #     ].shift(1)
    #     ohlcv["volume"] = ohlcv["volume"].fillna(0)
    #     ohlcv.reset_index(inplace=True)
    #     ohlcv.to_parquet(save_path, index=False)
    #     return save_path

    async def _async_download_symbol_fundingrate(
        self,
        symbol: str,
    ):
        """
        Download funding rate data for a specific symbol.
        """
        url = await self._get_download_url(symbol)
        await self._download_from_s3_url(url)

        
    def _dump_symbol_data(
        self,
        symbol: str,
        data_type: Literal["trades", "klines"],
        start_date: datetime.date | None = None,
        end_date: datetime.date | None = None,
        freq: Literal["1m", "5m", "15m", "30m", "60m"] | None = None,
    ):
        info = self._info[self.asset_type]
        if symbol not in info:
            raise ValueError(f"symbol {symbol} not found in {self.asset_type}")

        symbol_info = info[symbol]
        if start_date is None:
            start_date = symbol_info["start_date"]
        if end_date is None:
            end_date = symbol_info["end_date"]

        if self.start_date:
            start_date = max(start_date, self.start_date)
        if self.end_date:
            end_date = min(end_date, self.end_date)

        if start_date > end_date:
            self._log.debug(
                f"start_date {start_date} is greater than end_date {end_date} for symbol {symbol}, skip"
            )
            return

        if start_date < symbol_info["start_date"]:
            start_date = symbol_info["start_date"]
        if end_date > symbol_info["end_date"]:
            end_date = symbol_info["end_date"]

        if data_type == "klines":
            date_list = self._get_date_range(
                start_date=start_date, end_date=end_date, circle="monthly"
            )
        elif data_type == "trades":
            date_list = self._get_date_range(
                start_date=start_date, end_date=end_date, circle="daily"
            )

        if data_type == "klines":
            func = self._async_download_symbol_data
            params = [(symbol, data_type, date, freq) for date in date_list]
        elif data_type == "trades":
            func = self._async_download_symbol_data
            params = [(symbol, data_type, date) for date in date_list]
        elif data_type == "fundingrate":
            func = self._async_download_symbol_fundingrate
            params = [(symbol,)]
            

        self._loop.run_until_complete(
            tqdm.gather(
                *[func(*param) for param in params],
                leave=False,
                desc=f"Dumping {symbol} {data_type}",
            )
        )

    def dump_symbols(
        self,
        data_type: Literal["trades", "klines", "fundingrate"],
        start_date: datetime.date | None = None,
        end_date: datetime.date | None = None,
        freq: Literal["1m", "5m", "15m", "30m", "60m"] | None = None,
    ):
        for symbol in tqdm(self.symbols, desc="Dumping symbols", leave=False):
            # try:
            self._dump_symbol_data(
                symbol=symbol,
                data_type=data_type,
                start_date=start_date,
                end_date=end_date,
                freq=freq,
            )
        # except Exception as e:
        #     self._log.error(f"Error dumping {symbol} {data_type}: {e}")


def main():
    dumper = DataDumper(
        asset_type="contract",
        quote_currency="USDT",
        symbols=["ADAUSDT", "BTCUSDT", "ETHUSDT", "SOLUSDT"],
        start_date=datetime.date(2025, 1, 1),
    )
    dumper.dump_symbols(data_type="trades")


if __name__ == "__main__":
    main()
