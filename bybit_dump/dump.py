from typing import Literal
from datetime import datetime, timedelta, timezone
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
from throttled import Throttled, rate_limiter
from urllib.parse import urlparse
from curl_cffi import requests as cfreq

FREQ_TYPE = Literal[
    "1m",
    "5m",
    "15m",
    "30m",
    "60m",
    "2h",
    "4h",
    "6h",
    "12h",
    "1d",
    "1w",
    "1M",
]


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
    _throttle = {
        "public": Throttled(
            quota=rate_limiter.per_duration(timedelta(seconds=5), limit=600), timeout=5
        ),
    }
    _freq_map = {
        "1m": "1",
        "5m": "5",
        "15m": "15",
        "30m": "30",
        "60m": "60",
        "2h": "120",
        "4h": "240",
        "6h": "360",
        "12h": "720",
        "1d": "D",
        "1w": "W",
        "1M": "M",
    }

    @staticmethod
    def safe_dt(dt: datetime) -> datetime:
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    @staticmethod
    def _get_date_range(
        start_date: datetime,
        end_date: datetime,
        circle: Literal["daily", "monthly"] = "daily",
    ):
        start_date = DataDumper.safe_dt(start_date)
        end_date = DataDumper.safe_dt(end_date)

        if start_date > end_date:
            raise ValueError("Start date must be before end date")

        dates = []
        current = start_date

        if circle == "monthly":
            while current <= end_date:
                # Get the first day of the current month
                first_day = datetime(
                    current.year, current.month, 1, tzinfo=timezone.utc
                )
                dates.append(first_day)

                # Move to the first day of next month
                if current.month == 12:
                    current = datetime(current.year + 1, 1, 1, tzinfo=timezone.utc)
                else:
                    current = datetime(
                        current.year, current.month + 1, 1, tzinfo=timezone.utc
                    )
        elif circle == "daily":
            while current <= end_date:
                dates.append(current)
                current = current + timedelta(days=1)

        return dates

    def __init__(
        self,
        asset_type: Literal["spot", "contract"],
        symbols: list[str] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
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
            "authority": "www.bybit.com",
            "method": "GET",
            "scheme": "https",
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
            "accept-encoding": "gzip, deflate, br, zstd",
            "priority": "u=0, i",
            "cookie": '_by_l_g_d=aa0e7125-7e34-64e3-ab46-564d6c1bcfc7; deviceId=10d8c616-6fd0-ac1b-c2b3-979cadd2e845; cookies_uuid_report=11679560-f1b9-4aee-b9a4-3ef021093777; first_collect=true; _gcl_au=1.1.126852506.1758727699; _by_l_g_d=aa0e7125-7e34-64e3-ab46-564d6c1bcfc7; by_token_print=4b8fda7e03m0z3l7n4mszhe2d7d1d8047; deviceCodeExpire=1758734661352; secure-token=eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX2lkIjo0NjM3OTQzOTgsImIiOjAsInAiOjMsInVhIjoiIiwiZ2VuX3RzIjoxNzU4NzM0NjgyLCJleHAiOjE3NTg5OTM4ODIsIm5zIjoiIiwiZXh0Ijp7Im1jdCI6IjE3NDY3ODg2NDAiLCJwaWQiOiI0NjM3MzI0MzYiLCJzaWQiOiJCWUJJVCIsInNpdGUtaWQiOiJCWUJJVCIsInR5cCI6Ik1FTUJFUl9SRUxBVElPTl9UWVBFX09XTiJ9LCJkIjpmYWxzZSwic2lkIjoiQllCSVQifQ.GnFLqF6KvxwDpRdyK6_nX4wQPHkKDK1t8ky7W9AQjsJeb3Y3ld6H1UER2U0Da94AuWvBNcw4rJnAkRNULJJ_Jg; sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%22463794398%22%2C%22first_id%22%3A%221997b082d251178-096121ec31b9a08-1f525631-3686400-1997b082d26349a%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E7%9B%B4%E6%8E%A5%E6%B5%81%E9%87%8F%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC_%E7%9B%B4%E6%8E%A5%E6%89%93%E5%BC%80%22%2C%22%24latest_referrer%22%3A%22%22%2C%22_a_u_v%22%3A%220.0.6%22%7D%2C%22identities%22%3A%22eyIkaWRlbnRpdHlfY29va2llX2lkIjoiMTk5N2IwODJkMjUxMTc4LTA5NjEyMWVjMzFiOWEwOC0xZjUyNTYzMS0zNjg2NDAwLTE5OTdiMDgyZDI2MzQ5YSIsIiRpZGVudGl0eV9sb2dpbl9pZCI6IjQ2Mzc5NDM5OCJ9%22%2C%22history_login_id%22%3A%7B%22name%22%3A%22%24identity_login_id%22%2C%22value%22%3A%22463794398%22%7D%7D; BYBIT_REG_REF_prod={"lang":"en-US","g":"aa0e7125-7e34-64e3-ab46-564d6c1bcfc7","referrer":"www.bybit.com/","source":"bybit.com","medium":"other","url":"https://www.bybit.com/en/","last_refresh_time":"Fri, 26 Sep 2025 09:20:24 GMT","ext_json":{"dtpid":null}}; tx_token_current=BNE; tx_token_time=1758878428394; trace_id_time=1758878428418; _abck=9B7A266F0CC6190038BADB4F231DACD9~0~YAAQxnw2F+soN1OZAQAAqhEXiQ67pxXOCCVEZdYJchN5RUpKOd3EOLNk0S7jeqGsd9pQc5C7d3HyG4odjLFkCpqihpnId8JCWg8Nd6YlN0Lyy402LQKH418ENazLcyUyX8dlrDAF0CyZhiGHH2iXBVFnWIy19clKz+/Ge7cap9p3HXysGS5eTgJV5abR1/y0Z5Xlj67UD75VzZ0hTMO8qeI77OS/sok/uT7Ch1t3OX80LhzChb0aEmC3OTbubbyeVZs5x1h0Q06D4L/MSAZ2fT/4tRtkS9AFbDhfIRkyBeoJ/Tv4fbD5hrIRUu1c/NZTO3n5HjE6KgT3+SZyqIwurgco9Xe/4LzctR56b8GJH19Kz2uhTEoIF4xOVApt7YqPC6iiAkiIKhbJDhl+Fm+IKC1oWAsrc6UP1w1awZvXJ8GXHXVzUofTM4YzehCf4AaBolq7OlOpaFN8vwa6yva2pSggl3+3tI7+nulcw37p2yG+U4mQG3vlEj5Q9a4VNIZuioCA42Q+JqpFseq4b7v+bx8dK01GX8QldB4B3rpayaDNDafYZp+VanOUKY8dv/DqJuR5nHwvb65t/AjHgELCy8eAOxE0SkVkDxjcd8PWBOAWWlNsocpTsuHUAV1aLe+6Lbo=~-1~-1~-1~AAQAAAAE%2f%2f%2f%2f%2f43iDZhB2UJo6cYyqLeYPskziP+VZ86ZBZEyYMHvjlIG2dhWNEpVFbBJrMgC6GMn383TLia7HVuSnbLCmE5Eb3jbzHiRVmVD7hPw~-1; ak_bmsc=9F3561885BCF25AF16B785E63C837746~000000000000000000000000000000~YAAQxnw2F+woN1OZAQAAqhEXiR3lX3nQzVVDvrmxBJe+4CrLj4b9WTTo9qohqRoKNg68B//e82Rlh81MoJ9/kFJ4px0Yuf0grZWZYcrTNZrrKxbOYdwPwSe2zx5pSTyptwg+svXib6SYsMl7fhJUCm2Wjk9m6ww56yqhcdUEDC5hKCOTByUCUSoH2c5Fz60jR6YqyZO8JJ0lQC4isqRWcdVWH+DUo4tbNMQxRyXvCDQbdhH8OMZ3hwQ4ht5y2CyOpCJyNXmrrEV9PWxXHHnk4cLT6aOpRKyqXool6YKFx4ax3OAJBSS39ESRFeimQxqEK6ZuWx9nQo/jIOUqouhCVigaZD40tDnUs/D6DJd78EPgnJMdWBUBu0Ej//KM9q1mXVmNsPNa4CzpoVF6; bm_sz=C6F01C6FE9F54F11ACD061D84FF026FD~YAAQxnw2F+e4OVOZAQAA1UooiR3djRRgmiQFN617hGhjIHxuj3U/c8o4TSOueV4MIuKhlTthIqiX2r/ii6Rd4TC/KaRMmE584CKmJ5rPNrYy2CuxGgLw3h/MgE+k77oura+US2f8buMM2oklx4Y6Jblc+1vPCTOvmUx5+1aGSfSFUvCxgSS66kAeuDCjtpVgdPNx5HSVb0rDgxIwgQZZMR/V2VOJJmXJwPRb8AysX2tLi0+arcYMXH5vd9uVUM+dLa93Tv3mPV9AnV+MsrLFvmXRxi2MOClu33s93EhN2oOM1AnWih1iBhvOUyXwk2AMVLt+sZ5xZkvMR9iH7uACpUNnYjbnIoQiylEhq7oPjY/iWKo9xWB5qJ3x/DuUrUsv/fmOz2hLMeXTGKl6TInAFtxZ346W81B7sc2mhw3oPM5cV6PLqA==~3747906~4404279; bm_sv=95B6895C03FD7E8504F7359D8D4BBE2B~YAAQxnw2F3LAOVOZAQAAd30oiR1PkNkYx02IgXJLvyNSxoMcjLhf85TozF3sVeUxP3Oo6hRk0huMB004aplN3CvetJSGQesbXTvPQ1t4YGIkJuJc7Su9CfLUFTHXApuK/qcLmlhN2mI6wJ7rB9hYhbVOAgiNIwrcz1kcEZwAKM+azwBCqogltf3gtulfIprnGsintg28ssIfhtPoJYemeBoype6wULX4raSeiMCa/Acxud8P3Ga+aGTA5h5ICGY=~1',
            "sec-ch-ua": '"Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"macOS"',
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "none",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
        }
        # haader = Headers(
        #     browser="chrome",
        #     os="mac",
        #     headers=True,
        # )
        # self._headers = haader.generate()
        start_date = self.safe_dt(start_date) if start_date else None
        end_date = self.safe_dt(end_date) if end_date else None

        now = datetime.now(timezone.utc) - timedelta(days=1)

        if start_date is None:
            start_date = datetime(2020, 12, 18, 0, 0, 0, tzinfo=timezone.utc)
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

    def _get_exchange_info(
        self,
        asset_type: Literal["spot", "contract"],
    ):
        exchange_map = {"spot": "bybit-spot", "contract": "bybit"}
        with cfreq.Session(trust_env=True, proxy=self._proxy) as session:
            response = session.get(
                f"https://api.tardis.dev/v1/exchanges/{exchange_map[asset_type]}"
            )
            return response.json()

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

        start = datetime(2020, 12, 18, 0, 0, 0, tzinfo=timezone.utc)
        end = datetime.now(timezone.utc) - timedelta(days=1)

        data = self._get_exchange_info(asset_type=asset_type)

        info = {}

        start_idx = 1 if asset_type == "spot" else 2
        for symbol in data["datasets"]["symbols"][start_idx:]:
            symbol_info = {}
            id = symbol["id"]
            symbol_info["id"] = id
            _type = symbol["type"]

            # 将ISO格式的日期字符串转换为datetime对象
            available_since = datetime.strptime(
                symbol["availableSince"].split(".")[0], "%Y-%m-%dT%H:%M:%S"
            ).replace(tzinfo=timezone.utc)

            available_to = datetime.strptime(
                symbol["availableTo"].split(".")[0], "%Y-%m-%dT%H:%M:%S"
            ).replace(tzinfo=timezone.utc)

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
        date: datetime,
        freq: FREQ_TYPE,
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

        start_of_month = datetime(date.year, date.month, 1)

        if date.month == 12:
            end_of_month = datetime(date.year + 1, 1, 1) - timedelta(days=1)
        else:
            end_of_month = datetime(date.year, date.month + 1, 1) - timedelta(days=1)

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
        date: datetime,
        asset_type: Literal["spot", "contract"] | None = None,
        freq: FREQ_TYPE | None = None,
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
    async def _get_v5_market_kline(
        self,
        category: str,
        symbol: str,
        interval: str,
        start: int | None = None,
        end: int | None = None,
        limit: int = 1000,
    ):
        url = "https://api.bybit.com/v5/market/kline"
        payload = {
            "category": category,
            "symbol": symbol,
            "interval": interval,
            "start": start,
            "end": end,
            "limit": limit,
        }
        payload = {k: v for k, v in payload.items() if v is not None}
        async with aiohttp.ClientSession(trust_env=True, proxy=self._proxy) as session:
            self._throttle["public"].limit(key="v5/market/kline")
            async with session.get(
                url, params=payload, headers=self._headers
            ) as response:
                try:
                    response.raise_for_status()
                except aiohttp.client_exceptions.ClientResponseError as e:
                    if e.status in [500, 502, 503, 504, 429, 408]:
                        raise tenacity.TryAgain
                    else:
                        raise e
                data = await response.json()
                return data

    def _get_category(self, symbol: str) -> str:
        if self.asset_type == "spot":
            return "spot"

        if symbol.endswith("USDT") or symbol.endswith("USDC"):
            return "linear"
        elif symbol.endswith("USD"):
            return "inverse"
        else:
            raise ValueError(f"Symbol {symbol} formated wrongly, or not supported")

    async def _request_klines(
        self,
        symbol: str,
        freq: FREQ_TYPE,
        start_time: datetime,
        end_time: datetime,
    ):
        start_ms = int(start_time.timestamp() * 1000)
        end_ms = int(end_time.timestamp() * 1000)
        category = self._get_category(symbol)
        interval = self._freq_map[freq]

        seen: set[int] = set()
        records: list[list] = []
        cursor = start_ms

        while cursor < end_ms:
            resp = await self._get_v5_market_kline(
                category=category,
                symbol=symbol,
                interval=interval,
                limit=1000,
                start=cursor,
            )
            lst = resp.get("result", {}).get("list") or []
            if not lst:
                break

            # 排序（升序），去重 + 过滤超出时间范围
            lst = sorted(lst, key=lambda k: int(k[0]))
            new_count = 0
            for k in lst:
                ts = int(k[0])
                if ts in seen:
                    continue
                if ts < start_ms or ts >= end_ms:
                    continue
                seen.add(ts)
                records.append(k)
                new_count += 1

            # 防止死循环：如果没有新增 或 最后一条时间戳未前进则退出
            last_ts = int(lst[-1][0])
            if new_count == 0 or last_ts < cursor:
                break

            cursor = last_ts + 1  # 继续下一个毫秒

        if not records:
            return pd.DataFrame(
                columns=[
                    "symbol",
                    "timestamp",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "turnover",
                ]
            )

        df = pd.DataFrame(
            records,
            columns=["startTime", "open", "high", "low", "close", "volume", "turnover"],
        )
        df["symbol"] = symbol
        df["startTime"] = df["startTime"].astype(int)
        df["open"] = df["open"].astype(float)
        df["high"] = df["high"].astype(float)
        df["low"] = df["low"].astype(float)
        df["close"] = df["close"].astype(float)
        df["volume"] = df["volume"].astype(float)
        df["turnover"] = df["turnover"].astype(float)
        df["timestamp"] = pd.to_datetime(df["startTime"], unit="ms", utc=True)

        return df[
            [
                "symbol",
                "timestamp",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "turnover",
            ]
        ].sort_values("timestamp")

    async def _async_download_symbol_kline_data(
        self,
        symbol: str,
        freq: FREQ_TYPE,
        date: datetime,
    ):
        """Download (or refresh current month) kline data for a symbol/frequency/month.

        Spec:
        - Directory: <save_dir>/klines/<freq>/
        - Filename: {symbol}_kline_YYYY-MM.parquet
        - If file does not exist OR the target month has not finished yet (i.e. it's the current ongoing month),
          fetch fresh data via `_request_klines` for [month_start, next_month_start) and overwrite.
        - Otherwise (file exists and month is complete), skip download.

        Assumptions:
        - User spec mentioned `self.download_dir`; repo uses `self.save_dir`, so we use `self.save_dir`.
        - `date` can be any datetime within the target month (naive or tz-aware). We normalize to UTC month boundaries.
        """
        if freq is None:
            raise ValueError("`freq` must be provided for klines")
        
        if freq not in self._freq_map:
            raise ValueError(f"freq {freq} not supported, must be one of {list(self._freq_map.keys())}")

        # Normalize to UTC month start
        date = self.safe_dt(date)

        month_start = datetime(date.year, date.month, 1, tzinfo=timezone.utc)
        # Compute first day of next month
        if date.month == 12:
            next_month_start = datetime(date.year + 1, 1, 1, tzinfo=timezone.utc)
        else:
            next_month_start = datetime(
                date.year, date.month + 1, 1, tzinfo=timezone.utc
            )

        # Build storage paths
        dir_path = os.path.join(self.save_dir, "klines", freq)
        os.makedirs(dir_path, exist_ok=True)
        file_name = f"{symbol}_kline_{month_start.strftime('%Y-%m')}.parquet"
        file_path = os.path.join(dir_path, file_name)

        # Determine whether month is finished (i.e., we are past next month's start)
        now_utc = datetime.now(timezone.utc)
        month_finished = now_utc >= next_month_start

        # If file exists and month already finished, we assume it's complete and skip re-download
        if os.path.exists(file_path) and month_finished:
            self._log.debug(
                f"Skip kline download: {file_name} exists and month completed"
            )
            return file_path

        # Fetch klines for the whole month range
        try:
            df = await self._request_klines(
                symbol=symbol,
                freq=freq,
                start_time=month_start,
                end_time=next_month_start,
            )
        except Exception as e:
            self._log.error(
                f"Failed to fetch klines for {symbol} {freq} {month_start.strftime('%Y-%m')}: {e}"
            )
            return None

        df.to_parquet(file_path, index=False)
        self._log.debug(
            f"Saved kline data {symbol} {freq} {month_start.strftime('%Y-%m')} -> {file_path} (rows={len(df)})"
        )

        return file_path

    # @tenacity.retry(
    #     stop=tenacity.stop_after_attempt(5),
    #     wait=tenacity.wait_exponential(exp_base=2, multiplier=4, max=64),
    # )
    async def _async_download_symbol_data(
        self,
        symbol: str,
        data_type: Literal["klines", "trades", "fundingrate"],
        date: datetime,
        freq: FREQ_TYPE | None = None,
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

    def _get_download_url(self, symbol: str) -> str:
        """
        https://www.bybit.com/x-api/contract/v5/support/funding-rate-list-export?symbol=ETHUSDT
        https://www.bybit.com/x-api/contract/v5/support/funding-rate-list-export?symbol=BTCUSDT
        """
        response = cfreq.get(
            "https://www.bybit.com/x-api/contract/v5/support/funding-rate-list-export",
            params={"symbol": symbol},
            impersonate="chrome",
        )

        response.raise_for_status()
        data = response.json()

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

                with open(local_filename, "wb") as f:
                    async for chunk in response.content.iter_chunked(self._chunk_size):
                        f.write(chunk)

        self._log.debug(f"Downloaded {local_filename}")

        # Read the Excel file and convert to parquet
        df = pd.read_excel(
            local_filename, header=0, names=["timestamp", "symbol", "fundingrate"]
        )

        # Set proper column types
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        df["symbol"] = df["symbol"].astype(str)
        df["fundingrate"] = df["fundingrate"].astype(float)

        # Save as parquet
        parquet_filename = local_filename.replace(".xlsx", ".parquet")

        parquet_filepath = os.path.join(
            self.save_dir, "funding_rates", parquet_filename
        )
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
        url = self._get_download_url(symbol)
        await self._download_from_s3_url(url)

    def _dump_symbol_data(
        self,
        symbol: str,
        data_type: Literal["trades", "klines"],
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        freq: FREQ_TYPE | None = None,
    ):
        start_date = self.safe_dt(start_date) if start_date else None
        end_date = self.safe_dt(end_date) if end_date else None

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
            func = self._async_download_symbol_kline_data
            params = [(symbol, freq, date) for date in date_list]
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
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        freq: FREQ_TYPE | None = None,
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


async def main():
    dumper = DataDumper(
        asset_type="contract",
        quote_currency="USDT",
        symbols=["ADAUSDT", "BTCUSDT", "ETHUSDT", "SOLUSDT"],
        start_date=datetime(2025, 1, 1),
    )

    df = await dumper._request_klines(
        symbol="ADAUSDT",
        freq="1m",
        start_time=datetime(2025, 1, 1),
        end_time=datetime(2025, 1, 11),
    )

    print(df)


if __name__ == "__main__":
    asyncio.run(main())
