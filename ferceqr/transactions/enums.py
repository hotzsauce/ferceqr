"""
Column enums
"""
from __future__ import annotations

import polars as pl



__all__ = [
    "TransactionClassName",
    "TransactionExchangeBrokerage",
    "TransactionIncrementName",
    "TransactionIncrementPeakingName",
    "TransactionProductName",
    "TransactionRate",
    "TransactionRateUnits",
    "TransactionTermName",
    "TransactionTimeZone",
]



TransactionClassName = pl.Enum([
    "F", # Firm
    "NF", # Non-firm
    "UP", # Unit power sale
    "BA", # Billing adjustment
    "N/A", # Not applicable
])

TransactionExchangeBrokerage = pl.Enum([
    "BROKER",
    "ICE",
    "NODAL",
    "NYMEX",
])

TransactionIncrementName = pl.Enum([
    "5", # Five-minute
    "15", # Fifteen-minute
    "H", # Hourly
    "D", # Daily
    "W", # Weekly
    "M", # Monthly
    "Y", # Yearly
    "N/A", # Not applicable
])

TransactionIncrementPeakingName = pl.Enum([
    "FP", # Full-period
    "OP", # Off-peak
    "P", # Peak
    "N/A", # Not applicable
])

TransactionProductName = pl.Enum([
    "BLACK START SERVICE",
    "BOOKED OUT POWER",
    "CAPACITY",
    "CUSTOMER CHARGE",
    "DIRECT ASSIGNMENT FACILITIES CHARGE",
    "EMERGENCY ENERGY",
    "ENERGY",
    "ENERGY IMBALANCE",
    "EXCHANGE",
    "FUEL CHARGE",
    "GENERATOR IMBALANCE",
    "GRANDFATHERED BUNDLED",
    "INTERCONNECTION AGREEMENT",
    "MEMBERSHIP AGREEMENT",
    "MUST RUN AGREEMENT",
    "NEGOTIATED-RATE TRANSMISSION",
    "NETWORK",
    "NETWORK OPERATING AGREEMENT",
    "OTHER",
    "POINT-TO-POINT AGREEMENT",
    "PRIMARY FREQUENCY RESPONSE",
    "REACTIVE SUPPLY & VOLTAGE CONTROL",
    "REAL POWER TRANSMISSION LOSS",
    "REASSIGNMENT AGREEMENT",
    "REGULATION & FREQUENCY RESPONSE",
    "REQUIREMENTS SERVICE",
    "SCHEDULE SYSTEM CONTROL & DISPATCH",
    "SPINNING RESERVE",
    "SUPPLEMENTAL RESERVE",
    "SYSTEM OPERATING AGREEMENTS",
    "TOLLING ENERGY",
    "TRANSMISSION OWNERS AGREEMENT",
    "UPLIFT",
])

TransactionRate = pl.Enum([
    # The specified entries are title case, but to make consistent with other
    # columns we'll make these uppercase
    "FIXED",
    "FORMULA",
    "ELECTRIC INDEX",
    "RTO/ISO",
])

TransactionRateUnits = pl.Enum([
    "$/KV", #dollars per kilovolt
    "$/KVA", #dollars per kilovolt amperes
    "$/KVR", #dollars per kilovar
    "$/KW", #dollars per kilowatt
    "$/KWH", #dollars per kilowatt hour
    "$/KW-DAY", #dollars per kilowatt day
    "$/KW-MO", #dollars per kilowatt month
    "$/KW-WK", #dollars per kilowatt week
    "$/KW-YR", #dollars per kilowatt year
    "$/MW", #dollars per megawatt
    "$/MWH", #dollars per megawatt hour
    "$/MW-DAY", #dollars per megawatt day
    "$/MW-MO", #dollars per megawatt month
    "$/MW-WK", #dollars per megawatt week
    "$/MW-YR", #dollars per megawatt year
    "$/MVAR-YR", #dollars per megavar year
    "$/RKVA", #dollars per reactive kilovar amperes
    "CENTS", #cents
    "CENTS/KVR", #cents per kilovolt amperes
    "CENTS/KWH", #cents per kilowatt hour
    "FLAT RATE", # rate not specified in any other units
])

TransactionTermName = pl.Enum([
    "LT", # Long-term
    "ST", # Short-term
    "N/A", # Not applicable
])

TransactionTimeZone = pl.Enum([
    "AD", # atlantic daylight
    "AP", # atlantic prevailing
    "AS", # atlantic standard
    "CD", # central daylight
    "CP", # central prevailing
    "CS", # central standard
    "ED", # eastern daylight
    "EP", # eastern prevailing
    "ES", # eastern standard
    "MD", # mountain daylight
    "MP", # mountain prevailing
    "MS", # mountain standard
    "PD", # pacific daylight
    "PP", # pacific prevailing
    "PS", # pacific standard
])

TRANSACTION_MAPPINGS = {
    # col name -> enum type
    "exchange_brokerage_service": TransactionExchangeBrokerage,
    "type_of_rate": TransactionRate,
    "time_zone": TransactionTimeZone,
    "class_name": TransactionClassName,
    "term_name": TransactionTermName,
    "increment_name": TransactionIncrementName,
    "increment_peaking_name": TransactionIncrementPeakingName,
    "product_name": TransactionProductName,
    "rate_units": TransactionRateUnits,
}
