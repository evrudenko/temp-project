import os

import pandas as pd
import sqlalchemy

from settings import CONNECTION_STRING


def get_df(filename):
    return pd.read_csv(filename, sep=';', index_col=None)


def upload_product():
    engine = sqlalchemy.create_engine(CONNECTION_STRING)
    data = get_df(os.path.join('data', 'Product.csv'))
    data.to_sql('product', engine, index=False, dtype={
        'product_id': sqlalchemy.types.BigInteger,
        'pricing_line_id': sqlalchemy.types.VARCHAR(50),
        'listed_on': sqlalchemy.types.Date
    })

def upload_item_price():
    engine = sqlalchemy.create_engine(CONNECTION_STRING)
    data = get_df(os.path.join('data', 'Itemprice.csv'))
    data.to_sql('itemprice', engine, index=False, dtype={
        'pricing_line_id': sqlalchemy.types.VARCHAR(50),
        'pricing_tier_id': sqlalchemy.types.Integer,
        'price': sqlalchemy.types.Numeric,
        'timestamp': sqlalchemy.types.TIMESTAMP(timezone=False)
    })


def upload_pos_tier():
    engine = sqlalchemy.create_engine(CONNECTION_STRING)
    data = get_df(os.path.join('data', 'PosTier.csv'))
    data.to_sql('postier', engine, index=False, dtype={
        'pos_id': sqlalchemy.types.VARCHAR(50),
        'name': sqlalchemy.types.VARCHAR(200),
        'pricing_tier_id': sqlalchemy.types.Integer,
    })


def upload_item_pricing_on_sku_in_pos():
    engine = sqlalchemy.create_engine(CONNECTION_STRING)
    data = get_df(os.path.join('data', 'ItemPricingOnSKUinPOS.csv'))
    data.to_sql('ItemPricingOnSKUinPOS', engine, index=False, dtype={
        'pos_id': sqlalchemy.types.VARCHAR(50),
        'product_id': sqlalchemy.types.BigInteger,
        'price': sqlalchemy.types.Numeric,
    })


def main():
    print(CONNECTION_STRING)


if __name__ == '__main__':
    main()
