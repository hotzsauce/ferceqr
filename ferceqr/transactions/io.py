"""
Tools for reading in FERC EQR transactions data

This module reads FERC EQR "transactions" datasets that are distributed as a
"zip of zips". It locates the inner `*_transactions.csv` file(s), loads them
into Polars, applies light canonicalization (upper-casing a few categorical
columns and trimming time zone strings), and optionally filters rows.

Main entry point: `QuarterlyTransactionsTransformer`.

Notes
-----
- The raw CSVs **cannot** be scanned lazily because some inner files are not
  UTF-8 encoded. The reader retries a few common encodings and falls back to
  decoding+reparsing when needed.
"""
from __future__ import annotations

import io
import ferceqr.transactions.enums as en
from functools import reduce
import operator
import pathlib
import polars as pl
import re
import zipfile



TRANSACTIONS_PATTERN = re.compile("^.+_transactions.csv$", re.IGNORECASE)
TRANSACTIONS_SCHEMA = {
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
    "increment_name": en.TransactionIncrementName,
    # "increment_peaking_name": en.TransactionIncrementPeakingName,
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





class QuarterlyTransactionsTransformer(object):
    """
    Read and normalize quarterly FERC EQR transactions from a "zip of zips".

    This class opens the outer quarterly ZIP, finds inner seller ZIPs, extracts
    the single `*_transactions.csv` per seller, loads each into Polars with a
    fixed schema, applies standardization to a few string columns, optionally
    filters rows, concatenates all frames vertically, and returns a single
    `pl.DataFrame`.

    Parameters
    ----------
    zip_path : str or pathlib.Path
        Path to the outer quarterly ZIP file (e.g., 'csv_2025_q1.zip').

    Attributes
    ----------
    zip_path : pathlib.Path
        Absolute, resolved path to the outer ZIP.

    Notes
    -----
    The inner CSV files are not consistently UTF-8 encoded. Reading is eager
    (not lazy) to allow decode retries and reliable schema application.

    Examples
    --------
    >>> reader = QuarterlyTransactionsTransformer("path/to/csv_2025_q1.zip")
    >>> df = reader.read(filters={"point_of_delivery_balancing_authority": "PJM"})
    >>> df.shape
    (n_rows, n_cols)
    """

    def __init__(self, zip_path: str | pathlib.Path):
        self.zip_path = pathlib.Path(str(zip_path)).resolve()

    def read(
        self,
        filters: dict[str, object] | None = None,
    ) -> pl.DataFrame:
        """
        Read, standardize, optionally filter, and concatenate all transactions.

        Parameters
        ----------
        filters : dict[str, object], optional
            Column -> value constraints for row filtering. Values can be:
            - Scalar: equality predicate (e.g., {"ba": "PJM"}).
            - Iterable of scalars (list/tuple/set): membership predicate
              (e.g., {"ba": {"PJM", "MISO"}}).
            - 2-tuple operator form: (op, rhs), where `op` is one of
              ``"gt" (>)``, ``"ge" (>=)``, ``"lt" (<)``, ``"le" (<=)``,
              ``"ne" (!=)``, or ``"between"`` with `(low, high[, inclusive])`.
              Example: `{"trade_date": ("between", (20240101, 20240331, True))}`.

        Returns
        -------
        pl.DataFrame
            Concatenated transactions frame after normalization (and filtering
            if requested).

        Raises
        ------
        FileNotFoundError
            If a seller ZIP exists without a matching transactions CSV.
        RuntimeError
            If a seller ZIP contains more than one transactions CSV.
        """
        predicate = _filters_to_predicate(filters) if filters else pl.lit(True)

        # sometimes we run into some uncleaned columns. Three observed problems:
        # 1. Title case is used when it should be uppercase
        # 2. 'type_of_rate' **specification** is title case, but some entries are
        #   all uppercase. To make consistent with all the other columns, we
        #   force these to be uppercase.
        # 3. Time zone entries sometimes have a "T"; i.e. "EST" instead of "ES"
        to_uppercase = [
            "type_of_rate", "product_name", "rate_units",
            "exchange_brokerage_service",
            "increment_peaking_name",
        ]
        to_shorten = ["time_zone"]

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
        mods = upper_mods + shorten_mods
        with zipfile.ZipFile(self.zip_path) as zf:
            names = zf.namelist()

            frames = []
            for i, name in enumerate(names):
                try:
                    df = (
                        self._unzip_and_read(name, zf, TRANSACTIONS_PATTERN)
                        .filter(predicate)
                        .with_columns(*mods)
                    )
                    frames.append(df)
                except FileNotFoundError:
                    pass

            df = pl.concat(frames, how="vertical")
            return df

    def _unzip_and_read(
        self,
        inner_name: str,
        outer: zipfile.ZipFile,
        pat: re.Pattern[str],
    ) -> pl.DataFrame:
        """
        Extract and read the transactions CSV from a single seller ZIP.

        Parameters
        ----------
        inner_name : str
            Name of the inner seller ZIP inside the outer quarterly archive.
        outer : zipfile.ZipFile
            Opened ZipFile handle for the outer archive containing `inner_name`.
        pat : re.Pattern[str]
            Compiled regex used to identify the transactions CSV within the inner
            archive (typically :data:`TRANSACTIONS_PATTERN`).

        Returns
        -------
        pl.DataFrame
            The loaded transactions CSV parsed with :data:`TRANSACTIONS_SCHEMA`.

        Notes
        -----
        - Tries UTF-8 first, then retries a few common encodings ('cp1252',
            'latin-1') by decoding bytes and reparsing if necessary.
        - EQR files are distributed as zips-of-zips; this unzips one of those
            inner zips. It is expected that this inner zip has four csv files:
                - YYYYQQ_{seller name}_contracts.CSV
                - YYYYQQ_{seller name}_transactions.CSV
                - YYYYQQ_{seller name}_indexPub.CSV
                - YYYYQQ_{seller name}_ident.CSV

        Raises
        ------
        FileNotFoundError
            If no transactions CSV matches the pattern within the inner ZIP.
        RuntimeError
            If more than one transactions CSV matches the pattern.
        """
        inner_bytes = outer.read(inner_name)
        inner_buffer = io.BytesIO(inner_bytes)

        with zipfile.ZipFile(inner_buffer) as inner:
            tfile = list(filter(lambda n: pat.match(n), inner.namelist()))
            if tfile:
                if len(tfile) > 1:
                    raise RuntimeError(
                        f"File '{inner_name}' has multiple transactions "
                        "datasets"
                    )

                file_name = tfile[0]
                file_bytes = inner.read(file_name)
                file_buffer = io.BytesIO(file_bytes)

                try:
                    return pl.read_csv(file_buffer, schema=TRANSACTIONS_SCHEMA)
                except pl.exceptions.ComputeError:
                    # the possibility of this error is why we can't scan CSVs
                    possible_encodings = ["cp1252", "latin-1"]
                    for encoding in possible_encodings:
                        txt = file_bytes.decode(encoding, errors="strict")

                        try:
                            return pl.read_csv(
                                io.StringIO(txt),
                                schema=TRANSACTIONS_SCHEMA,
                            )
                        except UnicodeDecodeError:
                            pass

                    raise RuntimeError(f"Cannot decode file '{inner_name}'")
            else:
                raise FileNotFoundError(
                    f"No transactions file found in '{inner_name}'"
                )




def _filters_to_predicate(filters: dict[str, object]) -> pl.Expr:
    """
    Build a Polars boolean expression from a simple filter mapping.

    Supports three styles of constraints:

    1. Equality
       A scalar RHS produces `col == value`.

    2. Membership
       A list/tuple/set RHS produces `col.is_in(values)`.

    3. Operator tuple
       A 2-tuple `(op, rhs)` produces the corresponding comparison:
       ``"gt" (>)``, ``"ge" (>=)``, ``"lt" (<)``, ``"le" (<=)``, ``"ne" (!=)``,
       or ``"between"`` with `rhs = (low, high[, inclusive])`.

    Parameters
    ----------
    filters : dict[str, object]
        Mapping from column name to constraint.

    Returns
    -------
    pl.Expr
        Combined conjunction (`&`) of all predicates. If `filters` is empty,
        the identity predicate `pl.lit(True)` is returned.

    Examples
    --------
    >>> expr = _filters_to_predicate({
    ...     "point_of_delivery_balancing_authority": {"PJM", "MISO"},
    ...     "trade_date": ("between", (20240101, 20240331, True)),
    ... })
    >>> lf.filter(expr)  # doctest: +SKIP
    """
    exprs: list[pl.Expr] = []
    for col, val in filters.items():
        c = pl.col(col)
        if (
            isinstance(val, (list, tuple, set))
            and not (len(val) == 2 and isinstance(val[0], str))
        ):
            exprs.append(c.is_in(list(val)))
        elif isinstance(val, tuple) and len(val) == 2 and isinstance(val[0], str):
            op, rhs = val
            if op in {"gt", ">"}: exprs.append(c > rhs)
            elif op in {"ge", ">="}: exprs.append(c >= rhs)
            elif op in {"lt", "<"}: exprs.append(c < rhs)
            elif op in {"le", "<="}: exprs.append(c <= rhs)
            elif op in {"ne", "!="}: exprs.append(c != rhs)
            elif op == "between":
                lo, hi, *rest = rhs if isinstance(rhs, (list, tuple)) else (rhs,)
                inclusive = True
                if rest:
                    inclusive = bool(rest[0])
                if inclusive:
                    exprs.append((c >= lo) & (c <= hi))
                else:
                    exprs.append((c > lo) & (c < hi))
            else:
                raise ValueError(f"Unsupported operator: {op!r} for column {col!r}")
        else:
            exprs.append(c == val)
    return reduce(operator.and_, exprs, pl.lit(True))
