
""" Here we save the sql table templates.
    Other functions can call it for creating new table.

    NOTE: The first column would be set as key column.
"""

HISTROCIAL_PRICE = {
    "date": "INT",
    "open": "FLOAT",
    "high": "FLOAT",
    "low": "FLOAT",
    "close": "FLOAT",
    "adjclose": "FLOAT",
    "volume": "BIGINT"
}

STOCK_SPLIT = {
    "date": "INT",
    "priceMultipleFactor": "INT",
    "priceDivideFactor": "INT"
}

DIVIDEND = {
    "date": "INT",
    "dividend": "FLOAT"
}