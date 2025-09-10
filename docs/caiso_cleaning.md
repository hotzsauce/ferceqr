# Cleaning FERC Data that covers CAISO Assets

In order to align FERC data up with CAISO's generation resource data, we need
to modify the raw data a little bit. This file documents those steps.

1. Clean the seller names. This entails
    1. Trimming leading and trailing whitespace
    2. Removing internal whitespace to single spaces
    3. Removing any commas or periods
    4. Sending to lowercase
