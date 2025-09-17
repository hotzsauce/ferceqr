"""
Tools for reading in FERC EQR contracts data
"""
from __future__ import annotations

import io
from ferceqr.etl.errors import (
    MissingEocdError,
    MissingRecordTypeError,
)
from ferceqr.etl.preprocessor import EqrPreProcessor
import pathlib
import polars as pl
import re
import zipfile



CONTRACTS_PATTERN = re.compile("^.+_contracts.csv$", re.IGNORECASE)

CONTRACTS_INPUT_SCHEMA = {
    "contract_unique_id": pl.String,
    "seller_company_name": pl.String,
    "seller_history_name": pl.String,
    "customer_company_name": pl.String,
    "contract_affiliate": pl.String,
    "ferc_tariff_reference": pl.String,
    "contract_service_agreement_id": pl.String,
    "contract_execution_date": pl.Int64,
    "commencement_date_of_contract_term": pl.Int64,
    "contract_termination_date": pl.Int64,
    "actual_termination_date": pl.Int64,
    "extension_provision_description": pl.String,
    "class_name": pl.String, # needs to be canonicalized
    "term_name": pl.String, # needs to be canonicalized
    "increment_name": pl.String, # needs to be canonicalized
    "increment_peaking_name": pl.String, # needs to be canonicalized
    "product_type_name": pl.String, # needs to be canonicalized
    "product_name": pl.String, # needs to be canonicalized
    "quantity": pl.Float64,
    "units": pl.String, # needs to be canonicalized
    "rate": pl.Float64,
    "rate_minimum": pl.Float64,
    "rate_maximum": pl.Float64,
    "rate_description": pl.String,
    "rate_units": pl.String, # needs to be canonicalized
    "point_of_receipt_balancing_authority": pl.String,
    "point_of_receipt_specific_location": pl.String,
    "point_of_delivery_balancing_authority": pl.String,
    "point_of_delivery_specific_location": pl.String,
    "begin_date": pl.Int64,
    "end_date": pl.Int64,
}




class ContractPreProcessor(EqrPreProcessor):
    """
    Transformer for EQR contracts implementing EqrPreProcessor hooks
    """

    def __init__(
        self,
        in_zip: str | pathlib.Path,
        out_dir: str | pathlib.Path,
        chunk_size: int = 1_000_000,
        log_name: str = "",
        strict: bool = True,
    ):
        if not log_name:
            import datetime
            now = datetime.datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
            log_name = f"eqr_contracts_{now}.log"
        super().__init__(in_zip, out_dir, chunk_size, log_name, strict)

    def read_into_polars(
        self,
        source: str | pathlib.Path | io.BytesIO | io.StringIO | bytes,
    ) -> pl.DataFrame:
        """
        Parse a contracts CSV payload into a :class:`polars.DataFrame`.
        """
        return pl.read_csv(source, schema=CONTRACTS_INPUT_SCHEMA)
        try:
            return pl.read_csv(source, schema=CONTRACTS_INPUT_SCHEMA)
        except pl.ComputeError:
            return pl.read_csv(
                source,
                schema=CONTRACTS_INPUT_SCHEMA,
                encoding="utf8-lossy",
            )

    def unzip_by_rtype(
        self,
        outer: zipfile.ZipFile,
        inner_name: str,
    ) -> bytes:
        """
        Extract raw bytes for the inner ``*_contracts.csv`` from a seller ZIP
        """
        inner_bytes = outer.read(inner_name)
        inner_buffer = io.BytesIO(inner_bytes)

        zi = outer.getinfo(inner_name)
        if zi.is_dir():
            raise FileNotFoundError(
                f"'{inner_name}' is a directory, not a ZIP file"
            )

        if len(inner_bytes) != zi.file_size:
            raise zipfile.BadZipFile(
                f"Truncated inner ZIP for '{inner_name}': "
                f"expected {zi.file_size} bytes; got {len(inner_bytes)} bytes"
            )

        # EOCD presence check in last 66 KiB (ZIP spec)
        tail = inner_bytes[-66560:] if len(inner_bytes) >= 66560 else inner_bytes
        if b"PK\x05\x06" not in tail:
            raise MissingEocdError(inner_name)

        with zipfile.ZipFile(inner_buffer) as inner:
            tfile = list(filter(
                lambda n: CONTRACTS_PATTERN.match(n),
                inner.namelist()
            ))

            if len(tfile) == 0:
                raise MissingRecordTypeError("contracts", inner_name)

            if len(tfile) > 1:
                raise RuntimeError(
                    f"File '{inner_name}' has multiple contracts datasets"
                )

            contract_name = tfile[0]
            contract_bytes = inner.read(contract_name)
            return contract_bytes



if __name__ == "__main__":
    in_zip = "eqr_data/csv_2025_q1.zip"
    out_dir = "caiso/contracts"

    proc = ContractPreProcessor(
        in_zip,
        out_dir,
        log_name="caiso/contracts/log.log",
    )
    df = proc.read({"point_of_delivery_balancing_authority": "CISO"})
