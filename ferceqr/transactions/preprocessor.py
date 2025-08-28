"""
Tools for reading in FERC EQR transactions data

This module reads FERC EQR “transactions” datasets that are distributed as a
“zip of zips.” It locates the inner ``*_transactions.csv`` file(s), loads them
into Polars, applies light canonicalization (upper-casing selected categorical
columns and trimming time-zone values), and optionally filters rows.

Main entry point: :class:`TransactionsPreProcessor`.

Notes
-----
- The raw CSVs **cannot** be scanned lazily because some inner files are not
  UTF-8 encoded. Reading logic retries a few common encodings and falls back
  to decoding + reparsing when needed.
"""
from __future__ import annotations

import io
from ferceqr.etl.preprocessor import EqrPreProcessor
import ferceqr.transactions.enums as en
from functools import reduce
import operator
import pathlib
import polars as pl
import re
import zipfile



TRANSACTIONS_PATTERN = re.compile("^.+_transactions.csv$", re.IGNORECASE)

TRANSACTIONS_INPUT_SCHEMA = {
    "transaction_unique_id": pl.String,
    "seller_company_name": pl.String,
    "customer_company_name": pl.String,
    "ferc_tariff_reference": pl.String,
    "contract_service_agreement": pl.String,
    "transaction_unique_identifier": pl.String,
    "transaction_begin_date": pl.Int64,
    "transaction_end_date": pl.Int64,
    "trade_date": pl.Int32,
    "exchange_brokerage_service": pl.String, # needs to be canonicalized
    "type_of_rate": pl.String, # needs to be canonicalized
    "time_zone": pl.String, # needs to be canonicalized
    "point_of_delivery_balancing_authority": pl.String,
    "point_of_delivery_specific_location": pl.String,
    "class_name": en.TransactionClassName,
    "term_name": en.TransactionTermName,
    "increment_name": pl.String, # needs to be canonicalized
    "increment_peaking_name": pl.String, # needs to be canonicalized
    "product_name": pl.String, # needs to be canonicalized
    "transaction_quantity": pl.Float64,
    "price": pl.Float64,
    "rate_units": pl.String, # needs to be canonicalized
    "standardized_quantity": pl.Float64,
    "standardized_price": pl.Float64,
    "total_transmission_charge": pl.Float64,
    "total_transaction_charge": pl.Float64,
}



class TransactionsPreProcessor(EqrPreProcessor):
    """
    Transformer for EQR transactions implementing EqrPreProcessor hooks.

    This subclass provides transactions-specific behavior for reading raw CSV
    payloads and normalizing schemas so that downstream code can operate on
    consistent types (including Polars categorical enums).

    Implements :meth:`read_into_polars` to read CSV content, overrides
    :meth:`align_schema` to coerce column types and standardize a few string
    columns, and implements :meth:`unzip_by_rtype` to extract the inner
    ``*_transactions.csv`` bytes from a seller ZIP.
    """

    def align_schema(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Standardize the transactions schema and canonicalize select fields.

        Casts columns to the target transactions schema (enums, integers, floats)
        and applies element-wise fixes observed in the wild:

        1. Convert certain categorical columns to uppercase when providers supply
           title case.
        2. Force ``type_of_rate`` values to uppercase for consistency with the
           rest of the categorical fields.
        3. Trim time-zone strings to a two-character code when values include an
           extra trailing character (e.g., ``"EST"`` → ``"ES"``).

        Parameters
        ----------
        df : polars.DataFrame
            Raw transactions frame parsed from an inner CSV.

        Returns
        -------
        polars.DataFrame
            Schema-aligned frame with standardized categorical/time-zone fields.
        """

        to_uppercase = [
            "type_of_rate", "product_name", "rate_units",
            "exchange_brokerage_service",
            "increment_name",
            "increment_peaking_name",
        ]
        to_shorten = ["time_zone"]
        to_datetime = ["transaction_begin_date", "transaction_end_date"]
        to_date = ["trade_date"]

        upper_mods = [
            pl.col(c)
            .str.to_uppercase()
            .cast(en.TRANSACTION_MAPPINGS[c])
            for c in to_uppercase
        ]
        shorten_mods = [
            pl.col(c)
            .str.slice(0, length=2)
            .cast(en.TRANSACTION_MAPPINGS[c])
            for c in to_shorten
        ]
        datetime_mods = [
            pl.col(c)
            .cast(pl.String)
            .str.strptime(pl.Datetime, "%Y%m%d%H%M")
            for c in to_datetime
        ]
        date_mods = [
            pl.col(c)
            .cast(pl.String)
            .str.strptime(pl.Date, "%Y%m%d")
            for c in to_date
        ]
        mods = upper_mods + shorten_mods + datetime_mods + date_mods

        return (
            df
            .with_columns(*mods)
            .with_columns([
                pl.col(c).cast(enum)
                for c, enum in en.TRANSACTION_MAPPINGS.items()
            ])
        )

    def read_into_polars(
        self,
        source: str | pathlib.Path | io.BytesIO | io.StringIO | bytes,
    ) -> pl.DataFrame:
        """
        Parse a transactions CSV payload into a :class:`polars.DataFrame`.

        Delegates to :func:`polars.read_csv` using the module’s
        ``TRANSACTIONS_INPUT_SCHEMA`` so the resulting frame has stable dtypes
        across source files.

        Parameters
        ----------
        source : str or pathlib.Path or io.BytesIO or io.StringIO or bytes
            Path or in-memory buffer for a single ``*_transactions.csv`` payload.

        Returns
        -------
        polars.DataFrame
            Parsed transactions frame with input (pre-alignment) dtypes.
        """
        return pl.read_csv(source, schema=TRANSACTIONS_INPUT_SCHEMA)

    def unzip_by_rtype(
        self,
        outer: zipfile.ZipFile,
        inner_name: str,
    ) -> bytes:
        """
        Extract raw bytes for the inner ``*_transactions.csv`` from a seller ZIP.

        Searches the provided inner archive for exactly one file matching
        ``TRANSACTIONS_PATTERN`` and returns its bytes. If none or multiple
        matches are found, an error is raised.

        Parameters
        ----------
        outer : zipfile.ZipFile
            Open outer quarterly ZIP.
        inner_name : str
            Name of a seller member within the outer ZIP to inspect.

        Returns
        -------
        bytes
            Raw bytes of the inner transactions CSV.

        Raises
        ------
        FileNotFoundError
            If the inner archive contains no transactions CSV.
        RuntimeError
            If multiple transactions CSVs are found in the inner archive.
        """
        inner_bytes = outer.read(inner_name)
        inner_buffer = io.BytesIO(inner_bytes)

        with zipfile.ZipFile(inner_buffer) as inner:
            tfile = list(filter(
                lambda n: TRANSACTIONS_PATTERN.match(n),
                inner.namelist()
            ))

            if len(tfile) == 0:
                raise FileNotFoundError(
                    f"No transactions file found in '{inner_name}'"
                )

            if len(tfile) > 1:
                raise RuntimeError(
                    f"File '{inner_name}' has multiple transactions datasets"
                )

            trans_name = tfile[0]
            trans_bytes = inner.read(trans_name)
            return trans_bytes
