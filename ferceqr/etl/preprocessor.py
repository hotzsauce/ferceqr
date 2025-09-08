"""
Overview
--------
Tools for pre-processing the **doubly-zipped** CSVs from the FERC EQR program
into consistent, chunked Parquet files using **polars**. The module provides a
base class, :class:`EqrPreProcessor`, that knows how to iterate through the
outer ZIP, unzip the inner archives by record type, standardize schemas, apply
row filters, and write results as numbered Parquet chunks.

The abstract hooks :meth:`EqrPreProcessor.read_into_polars` and
:meth:`EqrPreProcessor.unzip_by_rtype` must be implemented by subclasses for a
specific EQR release/shape. The default :meth:`EqrPreProcessor.align_schema`
passes through the data unchanged, but can be overridden to coerce dtypes and
rename columns.

Notes
-----
- Chunking is controlled by ``chunk_size`` (rows) and written to
  ``out_dir/chunk_0000.parquet``, ``chunk_0001.parquet``, etc.
- Filters are converted to a polars predicate via
  :func:`ferceqr.utils.polars.filters_to_predicate` and applied before chunking.
"""
from __future__ import annotations

from ferceqr.etl.errors import (
    MissingEocdError,
    MissingRecordTypeError,
)
from ferceqr.utils.polars import filters_to_predicate
import io
import logging
import pathlib
import polars as pl
import zipfile



class EqrPreProcessor(object):
    """
    Base pre-processor for FERC EQR "double-zipped" CSVs.

    This class orchestrates reading an outer ZIP of inner ZIPs/CSVs, routing
    each record type to the appropriate reader, aligning the schema, optionally
    filtering rows, and writing chunked Parquet outputs.

    Subclasses are expected to implement :meth:`read_into_polars` (how to parse a
    single CSV-like payload for a given record type) and :meth:`unzip_by_rtype`
    (how to locate and extract the raw bytes for a requested record type from the
    outer archive). Override :meth:`align_schema` to rename/coerce columns.

    Parameters
    ----------
    in_zip : str or pathlib.Path
        Path to the outer ZIP file containing inner archives/CSVs.
    out_dir : str or pathlib.Path
        Directory where chunked Parquet files will be written.
    chunk_size : int, default 1_000_000
        Minimum number of rows per output chunk.

    Attributes
    ----------
    in_zip : pathlib.Path
        Resolved path to the input ZIP.
    out_dir : pathlib.Path
        Resolved output directory; created if missing.
    chunk_size : int
        Maximum rows per chunk.
    log_name : str, default ""
        The name of the logging file. Defaults to 'eqr_preprocessor_[timestamp].log"
    strict : bool, default True
        If `True`, throw an error and immediately terminate the processing.
        If `False`, log the error message and continue with the processing.
    """

    def __init__(
        self,
        in_zip: str | pathlib.Path,
        out_dir: str | pathlib.Path,
        chunk_size: int = 1_000_000,
        log_name: str = "",
        strict: bool = True,
    ):
        """
        Initialize the processor.

        Parameters
        ----------
        in_zip : str or pathlib.Path
            Path to the outer ZIP file containing inner archives/CSVs.
        out_dir : str or pathlib.Path
            Directory where chunked Parquet files will be written.
        chunk_size : int, default 1_000_000
            Maximum number of rows per output chunk.
        log_name : str, default ""
            The name of the logging file. Defaults to
            'eqr_preprocessor_[timestamp].log"
        strict : bool, default True
            If `True`, throw an error and immediately terminate the processing.
            If `False`, log the error message and continue with the processing.

        Notes
        -----
        Creates ``out_dir`` if it does not already exist and resets
        ``chunk_count`` to zero.
        """
        self.in_zip = pathlib.Path(str(in_zip)).expanduser().resolve()
        self.out_dir = pathlib.Path(str(out_dir)).expanduser().resolve()
        self.out_dir.mkdir(parents=True, exist_ok=True)

        self.chunk_size = chunk_size
        self.chunk_count = 0 # used to number chunks in out_dir

        # assign a `self.logger` tracker, and its first handler to
        # `self.logger_handler`
        self.log_name = log_name
        self._init_logger(log_name)

        self.strict = strict

    def align_schema(self, df: pl.DataFrame, *args, **kwargs) -> pl.DataFrame:
        """
        Align/standardize the schema for a record type.

        This default implementation returns ``df`` unchanged. Subclasses can
        override to rename columns, set dtypes, or add/remove fields.

        Parameters
        ----------
        df : polars.DataFrame
            Input DataFrame parsed from a raw record-type payload.
        *args
            Additional positional arguments for subclass-specific needs.
        **kwargs
            Additional keyword arguments for subclass-specific needs.

        Returns
        -------
        polars.DataFrame
            A schema-aligned DataFrame.
        """
        return df

    def read(
        self,
        filters: dict[str, object] | None = None,
    ):
        """
        Read, standardize, optionally filter, and write chunked Parquet outputs.

        The method iterates the outer ZIP, extracts each inner file for the desired
        record type(s) via :meth:`unzip_by_rtype`, parses it with
        :meth:`read_rtype_bytes` / :meth:`read_into_polars`, applies an optional
        predicate built from ``filters``, aligns the schema via
        :meth:`align_schema`, and accumulates rows into Parquet chunks of size
        ``chunk_size`` using :meth:`write_chunk`. Any remaining rows are flushed as a
        final chunk.

        Parameters
        ----------
        filters : dict[str, object], optional
            Mapping from column name to value(s) used to build a polars predicate
            with :func:`ferceqr.utils.polars.filters_to_predicate`. If ``None``, all
            rows are retained.

        Returns
        -------
        None
            Results are written to ``out_dir`` as ``chunk_####.parquet`` files.

        Notes
        -----
        This method resets ``chunk_count`` at the start and again after completion.
        """
        predicate = filters_to_predicate(filters) if filters else pl.lit(True)
        if filters:
            self.logger_handler.stream.write(
                "\n=== Filters ===\n" +
                "\n".join(f"    {k}: {v}" for k, v in filters.items()) +
                "\n\n"
            )
            self.logger_handler.stream.flush()

        chunk_frames = []
        row_count = 0
        self.chunk_count = 0 # reset in case read is called more than once

        with zipfile.ZipFile(self.in_zip) as zf:
            names = zf.namelist()
            for inner in names:
                try:
                    rbytes = self.unzip_by_rtype(zf, inner)
                except (
                    FileNotFoundError,
                    MissingEocdError,
                    MissingRecordTypeError,
                    RuntimeError,
                    zipfile.BadZipFile,
                ) as exc:
                    # all these error messages include the inner filename
                    self.logger.error(f"Error while processing: {exc}")
                except Exception as exc:
                    if self.strict:
                        raise exc
                    else:
                        self.logger.error(f"{exc}")

                raw_df = self.read_rtype_bytes(rbytes, inner).filter(predicate)
                n_rows = raw_df.shape[0]

                if n_rows > 0:
                    df = self.align_schema(raw_df)
                    chunk_frames.append(df)
                    row_count += n_rows

                    if row_count >= self.chunk_size:
                        chunk = pl.concat(chunk_frames, how="vertical")
                        self.write_chunk(chunk)

                        chunk_frames.clear()
                        row_count = 0

        if chunk_frames:
            chunk = pl.concat(chunk_frames, how="vertical")
            self.write_chunk(chunk)

        self.chunk_count = 0 # maybe a bit too careful
        return None

    def read_into_polars(self, *args, **kwargs):
        """
        Parse a raw CSV-like payload into a :class:`polars.DataFrame`.

        This is a required subclass hook. Implementations should read the
        provided buffer/bytes for a single record type (e.g., transactions,
        contracts, or identification) and return a DataFrame with the *raw*
        schema for that type. Any schema normalization should be done in
        :meth:`align_schema`.

        Parameters
        ----------
        *args
            Implementation-defined.
        **kwargs
            Implementation-defined.

        Returns
        -------
        polars.DataFrame
            The parsed raw DataFrame.

        Raises
        ------
        NotImplementedError
            If not implemented by a subclass.
        """
        raise NotImplementedError(
            "'read_into_polars' must be implemented by subclasses of "
            "'EqrPreProcessor'."
        )

    def read_rtype_bytes(self, rbytes: bytes, source_name: str = ""):
        """
        Decode and parse raw bytes for a single record type.

        Attempts to parse the bytes directly by passing a :class:`io.BytesIO` to
        :meth:`read_into_polars`. If a :class:`polars.exceptions.ComputeError`
        is raised (commonly due to encoding issues), the method retries by
        decoding with a small set of fallback encodings (currently ``cp1252``
        and ``latin-1``) and passing a :class:`io.StringIO` to
        :meth:`read_into_polars`.

        Parameters
        ----------
        rbytes : bytes
            Raw bytes of a CSV-like payload for a record type.
        source_name : str, default ''
            Optional source label used only for debugging/logging by callers.

        Returns
        -------
        polars.DataFrame
            Parsed DataFrame for the *raw* record type.
        """
        try:
            return self.read_into_polars(io.BytesIO(rbytes))
        except pl.exceptions.ComputeError:
            # the possibility of this error is why we can't scan CSVs
            possible_encodings = ["cp1252", "latin-1"]
            for encoding in possible_encodings:
                txt = rbytes.decode(encoding, errors="strict")

                try:
                    return self.read_into_polars(io.StringIO(txt))
                except UnicodeDecodeError:
                    pass

            if source_name:
                raise UnicodeDecodeError(f"Cannot decode file '{source_name}'")
            else:
                raise UnicodeDecodeError("Cannot decode file")

    def unzip_by_rtype(self, *args, **kwargs):
        """
        Locate and return raw bytes for the requested record type inside the outer ZIP.

        This is a required subclass hook. Given the open outer ZIP and a path/name
        for a member, the implementation should extract and return the bytes for the
        desired record type (e.g., transactions, contracts, identification).

        Parameters
        ----------
        *args
            Implementation-defined (e.g., an open :class:`zipfile.ZipFile` and an
            inner member name).
        **kwargs
            Implementation-defined.

        Returns
        -------
        bytes
            Raw bytes for the requested record type.

        Raises
        ------
        NotImplementedError
            If not implemented by a subclass.
        """
        raise NotImplementedError(
            "'unzip_by_dtype' must be implemented by subclasses of "
            "'EqrPreProcessor'."
        )

    def write_chunk(self, df: pl.DataFrame):
        """
        Write a chunk to Parquet and increment the chunk counter.

        The file is named ``chunk_####.parquet`` (zero-padded to 4 digits) and
        written to ``out_dir``.

        Parameters
        ----------
        df : polars.DataFrame
            The chunk to persist.

        Returns
        -------
        None
        """
        chunk_str = str(self.chunk_count).zfill(4)
        out_path = self.out_dir / f"chunk_{chunk_str}.parquet"
        df.write_parquet(out_path)
        self.chunk_count += 1

    def _init_logger(self, log_id: str):
        if not log_id:
            import datetime
            now = datetime.datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
            log_id = f"eqr_preprocessing_{now}.log"

        self.logger = logging.getLogger(log_id)
        self.logger.setLevel(logging.INFO)
        self.logger.propagate = False # don't send to root

        # avoid duplicating handlers if this gets called twice
        if not (
            any(
                isinstance(h, logging.FileHandler)
                and getattr(h, "baseFilename", None) == str(log_id)
                for h in self.logger.handlers
            )
        ):
            fh = logging.FileHandler(log_id, mode="a", encoding="utf-8")
            fh.setLevel(logging.INFO)
            fh.setFormatter(logging.Formatter(
                "%(asctime)s %(levelname)s: %(message)s"
            ))

            self.logger.addHandler(fh)
            self.logger_handler = fh
        else:
            self.logger_handler = self.logger.handlers[0]

        # write an informative header that doesn't have to abide by the
        # format set above
        self.logger_handler.stream.write(
            f"Source: {self.in_zip}\n"
            f"Target: {self.out_dir}\n"
        )
        self.logger_handler.stream.flush()
