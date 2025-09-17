"""
Column enums
"""
from __future__ import annotations

import polars as pl



__all__ = [
    "ContractClassName",
    "ContractExchangeBrokerage",
    "ContractIncrementName",
    "ContractIncrementPeakingName",
    "ContractProductName",
    "ContractRate",
    "ContractRateUnits",
    "ContractTermName",
    "ContractTimeZone",
]



ContractClassName = pl.Enum([
    "F", # Firm
    "NF", # Non-firm
    "UP", # Unit power sale
    "BA", # Billing adjustment
    "N/A", # Not applicable
])

ContractExchangeBrokerage = pl.Enum([
    "BROKER",
    "ICE",
    "NODAL",
    "NYMEX",
])

ContractIncrementName = pl.Enum([
    "5", # Five-minute
    "15", # Fifteen-minute
    "H", # Hourly
    "D", # Daily
    "W", # Weekly
    "M", # Monthly
    "Y", # Yearly
    "N/A", # Not applicable
])

ContractIncrementPeakingName = pl.Enum([
    "FP", # Full-period
    "OP", # Off-peak
    "P", # Peak
    "N/A", # Not applicable
])

ContractProductName = pl.Enum([
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

ContractProductTypeName = pl.Enum([
    "CB - Cost Based",
    "CR - Capacity Reassignment",
    "MB - Market Based",
    "T - Transmission",
    "NPU - Non-Public Utility",
    "Other",
])

ContractRate = pl.Enum([
    # The specified entries are title case, but to make consistent with other
    # columns we'll make these uppercase
    "FIXED",
    "FORMULA",
    "ELECTRIC INDEX",
    "RTO/ISO",
])

ContractRateUnits = pl.Enum([
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

ContractTermName = pl.Enum([
    "LT", # Long-term
    "ST", # Short-term
    "N/A", # Not applicable
])

ContractTimeZone = pl.Enum([
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

CONTRACT_MAPPINGS = {
    # col name -> enum type
    "exchange_brokerage_service": ContractExchangeBrokerage,
    "type_of_rate": ContractRate,
    "time_zone": ContractTimeZone,
    "class_name": ContractClassName,
    "term_name": ContractTermName,
    "increment_name": ContractIncrementName,
    "increment_peaking_name": ContractIncrementPeakingName,
    "product_name": ContractProductName,
    "product_type_name": ContractProductTypeName,
    "rate_units": ContractRateUnits,
}
