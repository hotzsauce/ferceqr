"""
An example script showing how to use the `ferceqr` package
"""
from ferceqr.contracts import ContractPreProcessor
from ferceqr.transactions import TransactionsPreProcessor
from ferceqr.viewer import ReportViewer

import ferceqr
import pathlib



def pull_data_from_ferc(quarter: str, target_dir: str):
    """
    Grab the quarterly data and write it locally
    """
    report = ferceqr.viewer.ReportViewer()
    report.download(quarter, target_dir=target_dir)

def filter_pjm_transactions(source: str, target: str):
    """
    From the raw data, grab transactions data associated with PJM
    """
    log_name = pathlib.Path(target).expanduser().resolve().with_suffix(".log")
    pipeline = TransactionsPreProcessor(
        source,
        target,
        log_name=log_name
    )
    pipeline.read({"point_of_delivery_balancing_authority": "PJM"})

def filter_pjm_contracts(source: str, target: str):
    """
    From the raw data, grab contracts data associated with PJM
    """
    log_name = pathlib.Path(target).expanduser().resolve().with_suffix(".log")
    pipeline = ContractPreProcessor(
        source,
        target,
        log_name=log_name
    )
    pipeline.read({"point_of_delivery_balancing_authority": "PJM"})


def main():
    quarter = "2025 q2"
    raw_target = "example_ferc_data/raw"
    pull_data_from_ferc(quarter, raw_target) # produces `csv_2025_q2.zip` file

    transaction_source = raw_target + f"/csv_{quarter.replace(" ", "_")}.zip"
    transaction_target = "example_ferc_data/processed/transactions"
    filter_pjm_transactions(transaction_source, transaction_target)

    contract_source = transaction_source
    contract_target = "example_ferc_data/processed/contracts"
    filter_pjm_contracts(contract_source, contract_target)



if __name__ == "__main__":
    main()
