arg_date = '2022-01-04'
s3 = boto3.resource('s3')
bucket = s3.Bucket('demo-s3-nhhung2')


date_lst = return_date_list(bucket, arg_date)
b=[]
for i in date_lst:
    k='data/'+ i;
    b.append(k)

date_list=b 

# print(date_list)
# ##Test
s3 = boto3.resource('s3')
bucket = s3.Bucket('demo-s3-nhhung2')


def extract(bucket, date_list):
    
    def csv_to_df_nb(key):
        df = read_csv_to_df(bucket, key)
        return df
    
    files = [key for date in date_list for key in list_files_in_prefix(bucket, date)]
    df = pd.concat(map(csv_to_df_nb, files), ignore_index=True)
    return df



def load(bucket, df, trg_key, trg_format):
    target_key = trg_key + datetime.today().strftime("%Y%m%d_%H%M%S") + trg_format
    write_df_to_s3(bucket, df, target_key)
    return True


# def etl_report1(src_bucket, trg_bucket, date_list, trg_key, trg_format, columns, arg_date):
#     df = extract(src_bucket, date_list)
#     df = transform_report1(df, columns, arg_date)
#     load(trg_bucket, df, trg_key, trg_format)

df = extract(bucket, date_list)
columns = ['ISIN', 'Mnemonic', 'Date', 'Time', 'StartPrice', 'EndPrice', 'MinPrice', 'MaxPrice', 'TradedVolume']
arg_date = '2022-01-04'
trg_key = 'xetra_daily_report_'
trg_format = '.parquet'

# #no bug

def transform_report1(df, columns, arg_date):
    df = df.loc[:, columns]
    df.dropna(inplace=True)
    df['opening_price'] = df.sort_values(by=['Time']).groupby(['ISIN', 'Date'])['StartPrice'].transform('first')
    df['closing_price'] = df.sort_values(by=['Time']).groupby(['ISIN', 'Date'])['StartPrice'].transform('last')
    df = df.groupby(['ISIN', 'Date'], as_index=False).agg(opening_price_eur=('opening_price', 'min'), closing_price_eur=('closing_price', 'min'), minimum_price_eur=('MinPrice', 'min'), maximum_price_eur=('MaxPrice', 'max'), daily_traded_volume=('TradedVolume', 'sum'))
    df['prev_closing_price'] = df.sort_values(by=['Date']).groupby(['ISIN'])['closing_price_eur'].shift(1)
    df['change_prev_closing_%'] = (df['closing_price_eur'] - df['prev_closing_price']) / df['prev_closing_price'] * 100
    df.drop(columns=['prev_closing_price'], inplace=True)
    df = df.round(decimals=2)
    df = df[df.Date >= arg_date]
    df.reset_index(inplace=True, drop=True)
    return df



trg_key = 'xetra_daily_report_'
bucket_trg = s3.Bucket('xetra-112233')
def etl_report1(src_bucket, trg_bucket, date_list, trg_key, trg_format, columns, arg_date):
    df = extract(src_bucket, date_list)
    df = transform_report1(df, columns, arg_date)
    print(df)
    load(trg_bucket, df, trg_key, trg_format)
    return True

etl_report1(bucket, bucket_trg, date_list, trg_key, trg_format, columns, arg_date)  