
def main():
    
    # Parameters/Configurations
    # later read config
    arg_date = '2022-01-04'
    columns = ['ISIN', 'Mnemonic', 'Date', 'Time', 'StartPrice', 'EndPrice', 'MinPrice', 'MaxPrice', 'TradedVolume']
    trg_key = 'xetra_daily_report_'
    trg_format = '.parquet'
    bucket_name_src = 'demo-s3-nhhung2'
    bucket_name_trg = 'xetra-112233'
    
    # Init
    s3 = boto3.resource('s3')
    bucket_src = s3.Bucket(bucket_name_src)
    bucket_trg = s3.Bucket(bucket_name_trg)
    
    #run application
    date_lst = return_date_list(bucket, arg_date)
    b=[]
    for i in date_lst:
        k='data/'+ i;
        b.append(k)    
    date_list=b    
    # date_list = return_date_list(bucket_src, arg_date)
    etl_report1(bucket_src, bucket_trg, date_list, trg_key, trg_format, columns, arg_date)