"""sparqix - convert pandas / Python / SQL into Apache Spark code."""
from .core import Report, convert, detect_language

__all__ = ["convert", "detect_language", "Report"]
__version__ = "0.1.0.dev1"
