from datetime import date
import os
from pathlib import Path

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
        'agency_code': str,
        'agency': str
    },
    'low_memory': False
}

DATA_DIR = Path('data')

TODAY = date.today().strftime('%B %-d, %Y')


def build_readme(metadata={}):
    record_count = metadata.get('record_count')
    start_date = metadata.get('start_date')
    end_date = metadata.get('end_date')
    
    with open('readme-template.md', 'r') as infile:
        tmpl = infile.read()

    replacements = {
        '{% UPDATED %}': TODAY,
        '{% RECORD_COUNT %}': record_count,
        '{% START_DATE %}': start_date,
        '{% END_DATE %}': end_date,
    }

    for key in replacements:
        tmpl = tmpl.replace(key, replacements[key])

    with open('README.md', 'w') as outfile:
        outfile.write(tmpl)

    print('Wrote README.md')


def get_latest():

    base_url = 'http://bfm.sd.gov/ledger/CheckbookDetail'

    urls = [
        f'{base_url}CurrentYear.csv',
        f'{base_url}PriorYear1.csv',
        f'{base_url}PriorYear2.csv'
    ]

    print('Fetching data from state website ...')

    df_codes = pd.read_csv(
        'sd-agency-codes.csv',
        dtype={
            'agency_code': str
        }
    )

    # read data from three remote files into a df
    df_new = pd.concat([pd.read_csv(x, **CSV_READ_SETTINGS) for x in urls])  # noqa

    # per 2022-12-02 email from a state accountant,
    # 032 used to be ag and 20 was environment, and
    # the combined agency is now DANR, 03
    def fix_danr_codes(row):
        if row['agency_code'] in ['032', '20']:
            return '03'
        else:
            return row['agency_code']

    df_new.rename(
        columns={'agency': 'agency_code'},
        inplace=True
    )

    df_new['agency_code'] = df_new.apply(fix_danr_codes, axis=1)

    df_new = pd.merge(
        df_new,
        df_codes,
        how='left',
        on='agency_code'
    )

    # read existing data from local files
    df_existing = pd.concat([pd.read_csv(x, **CSV_READ_SETTINGS) for x in DATA_DIR.glob('*.csv')])  # noqa

    print(f'Data from website: {len(df_new):,}')
    print(f'Existing record count: {len(df_existing):,}')

    df_combo = pd.concat([df_existing, df_new])

    # drop duplicates
    df_combo.drop_duplicates(inplace=True)

    # check for missing agency names
    missing_agency = df_combo[df_combo['agency_name'] == '']

    if len(missing_agency) > 0:
        print(df_combo)
        raise Exception('Missing agency name(s)')

    print(f'Combined, deduplicated data: {len(df_combo):,}')

    # sort by payment date and vendor name
    df_combo.sort_values(
        ['ap_payment_date', 'vendor_name'],
        inplace=True
    )

    # add a new column to group by
    df_combo['monthyear'] = df_combo['ap_payment_date'].dt.strftime('%Y%m')

    # write out monthly files
    for monthyear, month_df in df_combo.groupby('monthyear'):
        filepath = DATA_DIR / f'{monthyear}.csv'

        month_df_cols = [x for x in month_df.columns if x != 'monthyear']  # noqa

        month_df[month_df_cols].to_csv(filepath, index=False)
        print(f'Wrote {filepath}')

    metadata = {
        'record_count': f'{len(df_combo):,}',
        'start_date': df_combo['ap_payment_date'].min().strftime('%B %-d, %Y'),
        'end_date': df_combo['ap_payment_date'].max().strftime('%B %-d, %Y'),
    }

    build_readme(metadata=metadata)


if __name__ == '__main__':
    get_latest()
