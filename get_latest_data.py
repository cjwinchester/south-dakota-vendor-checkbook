import datetime
import os
from glob import glob

import pandas as pd


'''
Common args for reading this data into a pandas dataframe

The state's numbered agency codes would be duplicated
if cast to integers -- e.g. `010` is the governor's
office and `10` is the Department of Labor and
Regulation -- so always need to import agency codes as strings
'''
CSV_READ_SETTINGS = {
    'sep': ',',
    'parse_dates': [
        'document_date',
        'ap_payment_date'
    ],
    'encoding': 'utf-8',
    'dtype': {
        'agency': str
    }
}

DATA_DIR = 'data'


def get_latest():

    this_year = datetime.date.today().year
    base_url = 'http://bfm.sd.gov/ledger/CheckbookDetail'

    urls = {
        this_year: f'{base_url}CurrentYear.csv',
        this_year-1: f'{base_url}PriorYear1.csv',
        this_year-2: f'{base_url}PriorYear2.csv'
    }

    print('Fetching data from state website ...')

    # read data from three remote files into a df
    df_new = pd.concat([pd.read_csv(urls[x], **CSV_READ_SETTINGS) for x in urls])  # noqa

    # read existing data from local data directory
    df_existing = pd.concat([pd.read_csv(x, **CSV_READ_SETTINGS) for x in glob(f'{DATA_DIR}/*.csv')])  # noqa

    print(f'New data from website: {len(df_new):,}')
    print(f'Existing record count: {len(df_existing):,}')

    # merge the two data frames
    df = pd.concat([df_existing, df_new])

    # per 2022-12-02 email from a state accountant,
    # 032 used to be ag and 20 was environment, and
    # the combined agency is now DANR, 03
    def fix_danr_codes(row):
        if row['agency'] in ['032', '20']:
            return '03'
        else:
            return row['agency']

    df['agency'] = df.apply(fix_danr_codes, axis=1)

    # drop duplicates
    df.drop_duplicates(inplace=True)

    # sort by payment date
    df.sort_values(
        'ap_payment_date',
        ascending=False,
        inplace=True
    )

    # add a new column to group by
    df['monthyear'] = df['ap_payment_date'].dt.strftime('%Y%m')

    new_count = 0

    # write out monthly files
    for monthyear, month_df in df.groupby('monthyear'):
        filepath = os.path.join(
            'data',
            f'{monthyear}.csv'
        )
        new_count += len(month_df)
        month_df_cols = [x for x in month_df.columns if x != 'monthyear']
        month_df[month_df_cols].to_csv(filepath, index=False)
        print(f'Wrote {filepath}')

    print(f'New record count: {new_count:,}')


if __name__ == '__main__':
    get_latest()
