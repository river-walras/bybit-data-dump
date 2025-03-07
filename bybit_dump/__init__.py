from .dump import DataDumper
from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("bybit_dump")  
except PackageNotFoundError:
    __version__ = "unknown"  



__all__ = ["DataDumper"]
