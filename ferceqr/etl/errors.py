"""
Some data-ingestion and formatting-related errors
"""
from __future__ import annotations



class FercEqrError(Exception):
    """
    Base class for FERC EQR related errors
    """
    pass



class MissingEocdError(FercEqrError):
    """
    Raised when a FERC-provided zipfile is missing its EOCD
    """
    def __init__(self, zip_name: str):
        self.zip_name = zip_name
        super().__init__(f"ZIP file is missing EOCD: {zip_name}")



class MissingRecordTypeError(FercEqrError):
    """
    Raised when an entity's zipped file is missing, e.g. its transactions
    or contracts datasets
    """
    def __init__(self, zip_name: str, rtype: str = ""):
        self.zip_name = zip_name
        self.rtype = rtype
        if rtype:
            super().__init__(f"No {rtype} file found in '{zip_name}'")
        else:
            super().__init__(f"Desired record type file not found in '{zip_name}'")
