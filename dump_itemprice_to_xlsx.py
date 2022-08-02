import pandas as pd
import sqlalchemy

from settings import CONNECTION_STRING


def dump_itemprice_to_xlsx():
    engine = sqlalchemy.create_engine(CONNECTION_STRING)
    data = pd.read_sql_query('SELECT * FROM itemprice', engine, index_col=None)
    writer = pd.ExcelWriter('itemprice.xlsx', engine='xlsxwriter')
    
    # Convert timestamp to date only
    data = data.assign(date=lambda x: x.timestamp.dt.date)

    for date, df in data.groupby('date'):
        df.to_excel(
            writer,
            sheet_name=str(date),
            columns=['pricing_line_id', 'pricing_tier_id', 'price', 'timestamp']
        )
    
    writer.save()


if __name__ == '__main__':
    dump_itemprice_to_xlsx()
