import helpers
import database_connection as database
import json
import boto3
import zipfile
import io
import pandas as pd
import logging
import datetime
import dateutil.relativedelta
import numpy as np
# import mysql_database
from pandas.io import sql
from pandas import DataFrame
from sqlalchemy.exc import SQLAlchemyError

def etl_job():
  data = json.load(open('/home/anubha/Desktop/praxis/secrets.json'))
  logger = helpers.setup_logging()
  s3_client = boto3.client('s3',aws_access_key_id=data['aws_access_key_id'],
                        aws_secret_access_key=data['aws_secret_access_key'])
  s3_resource = boto3.resource('s3',aws_access_key_id=data['aws_access_key_id'],
                        aws_secret_access_key=data['aws_secret_access_key'])
  keys = []
  resp = s3_client.list_objects_v2(Bucket='dealer-churn-analysis')
  for obj in resp['Contents']:
      keys.append(obj['Key'])
  for key in keys:
      names =  key.split("/")
      obj = s3_resource.Bucket('dealer-churn-analysis').Object(helpers.zip_file_name())
  file_name = 'praxis/etl/logs/log_{file}.txt'.format(file=helpers.date())
  obj_log = s3_resource.Bucket('dealer-churn-analysis').Object(file_name)
  buffer = io.BytesIO(obj.get()["Body"].read())
  zip_file = zipfile.ZipFile(buffer,'r')
  logger.info("Name of csv in zip file :%s",zip_file.namelist())
  logs = ""
  dataframe = pd.DataFrame()
  df = pd.DataFrame()
  df2 = pd.DataFrame()
  for name_of_zipfile in zip_file.namelist():
      zip_open = pd.read_csv(zip_file.open(name_of_zipfile))
      dataframe['created_at'] = pd.Series([datetime.datetime.now()] * len(zip_open))
      dataframe['last_updated_at'] = pd.Series([datetime.datetime.now()] * len(zip_open))
      zip_open = pd.concat([dataframe,zip_open], axis=1)
      zip_open = zip_open.dropna()
      table_name = "{name}".format(name=name_of_zipfile.replace('.csv',''))
      print table_name
      print zip_open.info()
      try :
        zip_open.to_sql(name=name_of_zipfile.replace('.csv',''), con=database.db_connection(), if_exists = 'append', index=False)
      except SQLAlchemyError as sqlalchemy_error:
        print sqlalchemy_error
        logs = '\n{table_name}\n{error}\n{logs}'.format(logs=logs,error=sqlalchemy_error,table_name=table_name)
        logger.error(" %s",sqlalchemy_error)
      database.db_connection().execute('SET FOREIGN_KEY_CHECKS=1;')
  end_time = datetime.datetime.now()
  logger.info("End time of program : %s",end_time)
  logs = '{logs} \nstart_time : {start_time} \nend_time : {end_time}'.format(start_time=helpers.start_time(),logs=logs,end_time=end_time)
  print logs
  obj_log.put(Body=logs)

result = etl_job()

def consume_models():
  engine =  database.db_connection()
  dcpi_variables = pd.DataFrame()

  ads_data = engine.execute('select * from ads_data')
  ads_data_df = DataFrame(data=list(ads_data), columns=ads_data.keys())

  category = engine.execute('select id,name from category')
  category_df = DataFrame(data=list(category), columns=category.keys())
  category_df = category_df.rename(columns={'id': 'category_id', 'name': 'category_name'})

  omniture = engine.execute('select * from omniture_data')
  omniture_df = DataFrame(data=list(omniture), columns=omniture.keys())

  co_op_data = engine.execute('select * from co_op_data')
  co_op_data_df = DataFrame(data=list(co_op_data), columns=co_op_data.keys())
  co_op_data_df['co_op_year'] = pd.DatetimeIndex(co_op_data_df['co_op_date']).year
  co_op_data_df['co_op_month'] = pd.DatetimeIndex(co_op_data_df['co_op_date']).month
  co_op_data_df.drop('co_op_date', axis=1, inplace=True)

  dealer_master = engine.execute('select * from dealer_master')
  dealer_master_df = DataFrame(data=list(dealer_master), columns=dealer_master.keys())
  #print dealer_master_df.info()

  dealer_rep_manager_mapping = engine.execute('select * from dealer_rep_manager_mapping')
  dealer_rep_manager_mapping_df = DataFrame(data=list(dealer_rep_manager_mapping), columns=dealer_rep_manager_mapping.keys())

  manager_master = engine.execute('select * from manager_master')
  manager_master_df = DataFrame(data=list(manager_master), columns=manager_master.keys())

  realm_type = engine.execute('select id,name from realm_type')
  realm_type_df = DataFrame(data=list(realm_type), columns=realm_type.keys())
  realm_type_df = realm_type_df.rename(columns={'id': 'realm_id', 'name': 'realm_name'})

  rep_master = engine.execute('select * from rep_master')
  rep_master_df = DataFrame(data=list(rep_master), columns=rep_master.keys())

  dealer_master_df = pd.merge(dealer_master_df,realm_type_df,on='realm_name', how='left')
  dealer_master_df.drop('realm_name', axis=1, inplace=True)

  dealer_realm_mapping_df = dealer_master_df[['dealer_id','realm_id']]

  dealer_master_df = pd.merge(dealer_master_df,category_df,on='category_name', how='left')
  dealer_master_df.drop('category_name', axis=1, inplace=True)
  
  ads_dealer_data_join_df = pd.merge(ads_data_df,dealer_realm_mapping_df,how='left',on='dealer_id')    #dealer master and ads data merge on basis of dealer_id 
  ads_dealer_data_join_df = ads_dealer_data_join_df[['dealer_id','ad_id','realm_id','photo_count','create_date']]
  
  unique_dealer_ads_data_df = ads_dealer_data_join_df.groupby('dealer_id').filter(lambda group: len(group) > 0)
  temp = pd.DataFrame()
  for unique_dealer_id in unique_dealer_ads_data_df['dealer_id'].unique():
    ads_df = unique_dealer_ads_data_df.loc[unique_dealer_ads_data_df['dealer_id'] == unique_dealer_id].sort_values(by='create_date')
    cycle_posted_ads0_90  = ads_df.loc[(ads_df['create_date'] > helpers.get_last_3months()) & (ads_df['realm_id'] == 1)].sort_values(by='create_date')
    cycle_posted_ads0_90['cycle_posted_ads0_90'] = cycle_posted_ads0_90['photo_count'].sum()
    truck_posted_ads5_0_90  = ads_df.loc[(ads_df['create_date'] > helpers.get_last_3months()) & (ads_df['realm_id'] == 3) & (ads_df['photo_count'] > 5)].sort_values(by='create_date')
    truck_posted_ads5_0_90['truck_posted_ads0_90'] = truck_posted_ads5_0_90['photo_count'].sum()
    rv_posted_ads5_0_90  = ads_df.loc[(ads_df['create_date'] > helpers.get_last_3months()) & (ads_df['realm_id'] == 2) & (ads_df['photo_count'] > 5)].sort_values(by='create_date')
    rv_posted_ads5_0_90['rv_posted_ads0_90'] = rv_posted_ads5_0_90['photo_count'].sum()
    ads_df['cycle_tot_posted_ad_pic_last'] = cycle_posted_ads0_90['cycle_posted_ads0_90'] / cycle_posted_ads0_90['cycle_posted_ads0_90'].count()
    ads_df['truck_tot_posted_ad_pic_last'] = truck_posted_ads5_0_90['truck_posted_ads0_90'] / truck_posted_ads5_0_90['truck_posted_ads0_90'].count()
    ads_df['rv_tot_posted_ad_pic_last'] = rv_posted_ads5_0_90['rv_posted_ads0_90'] / rv_posted_ads5_0_90['rv_posted_ads0_90'].count()
    ads_df = ads_df[['ad_id','cycle_tot_posted_ad_pic_last','truck_tot_posted_ad_pic_last','rv_tot_posted_ad_pic_last']]
    temp = temp.append(ads_df)
  ads_dealer_data_join_df = pd.merge(ads_dealer_data_join_df,temp,on='ad_id',how='outer')
  ads_dealer_data_join_df.fillna(0,inplace=True)
  conditions = [
  (ads_dealer_data_join_df['realm_id'] == 1 ),
  (ads_dealer_data_join_df['realm_id'] == 2 ),
  (ads_dealer_data_join_df['realm_id'] == 3 )]
  choices = ['tot_posted_ad_pic_last3mon_per','tot_posted_ad_5pic_last3mon_per','tot_posted_ad_5pic_last3mon_per']
  ads_dealer_data_join_df['parameter_name'] = np.select(conditions, choices, default='NaN')

  dealer_activity = engine.execute('select * from dealer_activity')
  dealer_activity_data_df = DataFrame(data=list(dealer_activity), columns=dealer_activity.keys())

  join_dealer_activity_dealer_master = pd.merge(dealer_activity_data_df,dealer_realm_mapping_df,how='left',on='dealer_id')  #dealer master and dealer activity merge on basis of dealer_id
  join_dealer_activity_dealer_master = join_dealer_activity_dealer_master[['id','dealer_id','total_time_spend','total_visits','date','realm_id']]
  unique_dealer_dealer_activity_df = join_dealer_activity_dealer_master.groupby('dealer_id').filter(lambda group: len(group) > 0)
  temp = pd.DataFrame()
  temp1 = pd.DataFrame()
  for unique_dealer_id in unique_dealer_dealer_activity_df['dealer_id'].unique():
    dealer_activity_df = unique_dealer_dealer_activity_df.loc[unique_dealer_dealer_activity_df['dealer_id'] == unique_dealer_id].sort_values(by='date')
    total_days_visit_last3mon  = dealer_activity_df.loc[(dealer_activity_df['date'] > helpers.get_last_3months()) & (dealer_activity_df['realm_id'] == 1)].sort_values(by='date')
    avg_TimeSpent_60_90 = dealer_activity_df.loc[(dealer_activity_df['date'] > helpers.get_last_3months()) & (dealer_activity_df['realm_id'] == 1) & (dealer_activity_df['date'] < helpers.get_last_2months())].sort_values(by='date')
    total_days_visit_last3mon['total_days_visit_last3mon'] = total_days_visit_last3mon['total_visits'].sum()
    avg_TimeSpent_60_90['avg_TimeSpent_60_90'] = avg_TimeSpent_60_90['total_time_spend'].sum()
    total_days_visit_last3mon = total_days_visit_last3mon[['id','total_days_visit_last3mon']]
    avg_TimeSpent_60_90 = avg_TimeSpent_60_90[['id','avg_TimeSpent_60_90']]
    temp = temp.append(total_days_visit_last3mon) 
    temp1 = temp1.append(avg_TimeSpent_60_90)
  join_dealer_activity_dealer_master = pd.merge(join_dealer_activity_dealer_master,temp,on='id',how='outer')
  join_dealer_activity_dealer_master = pd.merge(join_dealer_activity_dealer_master,temp1,on='id',how='outer')
  join_dealer_activity_dealer_master['total_days_visit_last3mon'].fillna(0,inplace=True)
  join_dealer_activity_dealer_master['avg_TimeSpent_60_90'].fillna(0,inplace=True)

  billing_data = engine.execute('select * from billing_data')
  billing_data_df = DataFrame(data=list(billing_data), columns=billing_data.keys())
  billing_data_df = pd.merge(billing_data_df,dealer_realm_mapping_df,how='left',on='dealer_id') # billing data and dealer_realm join
  unique_dealer_bill_df = billing_data_df.groupby('dealer_id').filter(lambda group: len(group) > 0)
  temp = pd.DataFrame()
  for unique_dealer_id in unique_dealer_bill_df['dealer_id'].unique():
    billing_df = unique_dealer_bill_df.loc[unique_dealer_bill_df['dealer_id'] == unique_dealer_id].sort_values(by='invoice_date')
    billing_df['billing_change_previous_month'] = billing_df['invoice_amount'].diff()
    billing_df['billing_change_previous_month'].fillna(0,inplace=True)
    conditions = [
    (billing_df['billing_change_previous_month'] > 0 ),
    (billing_df['billing_change_previous_month'] < 0 ),
    (billing_df['billing_change_previous_month'] == 0 )]
    choices = ['U', 'D', 'A']
    billing_df['dealer_change_status'] = np.select(conditions, choices, default='A')
    last_month_billing_amount = (billing_df[billing_df['dealer_change_status'].isin(['A','U'])]
                            .sort_values('invoice_date')
                            .drop_duplicates('dealer_id', keep='last')
                            .set_index('dealer_id')['invoice_amount'])
    billing_df['last_month_billing_amount'] = billing_df['dealer_id'].map(last_month_billing_amount)
    billing_df['last30_billing_500'] = np.where((billing_df['last_month_billing_amount']<500) & (billing_df['realm_id'] == 1),1,0)      #cycle last_30_billing   
    billing_df['last30_billing_350'] = np.where((billing_df['last_month_billing_amount']<350) & (billing_df['realm_id'] == 2),1,0)      #truck last_30_billing
    billing_df['last30_billing_270'] = np.where((billing_df['last_month_billing_amount']<270) & (billing_df['realm_id'] == 3),1,0)      #RV last_30_billing
    billing_df['last30_billing_325'] = np.where((billing_df['last_month_billing_amount']<325) & (billing_df['realm_id'] == 4),1,0)      #eqipment last_30_billing
    billing_df = billing_df[['invoice_no','last30_billing_350','last30_billing_270','last30_billing_325','billing_change_previous_month','dealer_change_status','last_month_billing_amount','last30_billing_500']]
    temp = temp.append(billing_df)
  billing_data_df = pd.merge(billing_data_df,temp,on='invoice_no',how='outer')
  billing_data_df['dealer_change_status'].fillna('A',inplace=True)
  billing_data_df['event_date'] = pd.Series([helpers.get_last_date_previous_month()] * len(billing_data_df))
  billing_data_df['billing_change_previous_month'].fillna(0,inplace=True)
  billing_data_df['invoice_year'] = pd.DatetimeIndex(billing_data_df['invoice_date']).year
  billing_data_df['invoice_month'] = pd.DatetimeIndex(billing_data_df['invoice_date']).month
  billing_data_df.sort_values(by='invoice_date',inplace=True)
  dealer_billing_join_df = billing_data_df.filter(['invoice_no','dealer_id','realm_id','last_month_billing_amount','last30_billing_500','last30_billing_350','last30_billing_270','last30_billing_325','invoice_date'],axis=1)
  billing_data_df.drop('invoice_date', axis=1, inplace=True)
  dealer_billing_join_df['payment_time'] = billing_data_df['payment_date'].sub(billing_data_df['payment_due_date'], axis=0)
  dealer_billing_join_df['payment_time'] = dealer_billing_join_df['payment_time'].sub(pd.Timedelta(30, unit='d'), axis=0).astype('timedelta64[D]')
  dealer_billing_join_df['on_time_payments'] = np.where(dealer_billing_join_df['payment_time'] > 0 ,dealer_billing_join_df['payment_time'],'on time')
  conditions = [
  (dealer_billing_join_df['realm_id'] == 1 ),
  (dealer_billing_join_df['realm_id'] == 2 ),
  (dealer_billing_join_df['realm_id'] == 3 ),
  (dealer_billing_join_df['realm_id'] == 4)]
  choices = ['last30_billing_500','last30_billing_350','last30_billing_270','last30_billing_325']
  dealer_billing_join_df['parameter_name'] = np.select(conditions, choices, default='NaN')

  dealer_on_time_payments = dealer_billing_join_df.filter(['invoice_no','dealer_id','realm_id','payment_time','on_time_payments'],axis=1)
  dealer_billing_join_df.drop('payment_time', axis=1, inplace=True)
  dealer_billing_join_df.drop('on_time_payments', axis=1, inplace=True)

  conditions = [
  (dealer_billing_join_df['realm_id'] == 1 ),
  (dealer_billing_join_df['realm_id'] == 3 ),
  (dealer_billing_join_df['realm_id'] == 4)]
  choices = ['on_time_payments_1','on_time_payments_2','on_time_payments_3']
  dealer_on_time_payments['parameter_name'] = np.select(conditions, choices, default='NaN')

  dealer_omniture_df = pd.merge(omniture_df,dealer_realm_mapping_df,how='left',on='dealer_id')
  unique_dealer_omniture_data_df = dealer_omniture_df.groupby('dealer_id').filter(lambda group: len(group) > 0)
  temp = pd.DataFrame()
  for unique_dealer_id in unique_dealer_omniture_data_df['dealer_id'].unique():
    omni_df = unique_dealer_omniture_data_df.loc[unique_dealer_omniture_data_df['dealer_id'] == unique_dealer_id].sort_values(by='create_date')
    clicks_dealer_website_60_90 = omni_df.loc[(omni_df['hit_date'] > helpers.get_last_3months()) & (omni_df['realm_id'] == 2) & (omni_df['hit_date'] < helpers.get_last_2months())].sort_values(by='date')

consume_models()