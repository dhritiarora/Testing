import pandas as pd
import pytest
##test files added !!!!!!

def load_data():
    df=pd.read_csv("dataset.csv")
    df['Time'] = pd.to_datetime(df['time'])
    df.index = pd.to_datetime(df['time'])
    df = df.rename_axis(index={'time': 'time_index'})
    df = df.sort_index()
    df['counter']=1
    return df

def count_24_last(df):
    return df.groupby("cc_num")['counter'].rolling('24H').count().reset_index().fillna(0).to_dict()['counter']

#testing count of last 24 hours transactions
def test_count_24_last():
    data = load_data()
    expected = {0: 1.0, 1: 2.0, 2: 1.0, 3: 2.0, 4: 1.0, 5: 2.0, 6: 1.0, 7: 2.0, 8: 1.0, 9: 2.0, 10: 2.0, 11: 1.0, 12:2.0, 13:1.0, 14:2.0, 15:1.0, 16:1.0, 17:1.0, 18:1.0, 19:2.0, 20:1.0, 21:2.0, 22:1.0, 23: 2.0, 24:3.0, 25:1.0, 26: 1.0, 27:2.0, 28:2.0, 29:1.0, 30:2.0, 31: 1.0, 32: 1.0, 33: 1.0, 34: 1.0, 35: 2.0, 36: 3.0, 37: 1.0, 38: 1.0, 39: 1.0, 40: 1.0, 41: 2.0, 42: 3.0, 43: 1.0, 44: 2.0, 45: 3.0, 46: 4.0, 47: 1.0, 48: 1.0, 49: 1.0, 50: 1.0, 51:2.0 , 52:1.0 ,53:1.0 ,54:1.0 ,55:1.0 ,56:2.0 ,57:1.0 ,58:1.0 ,59:1.0 ,60:1.0,61:1.0,62:1.0,63:1.0,64:1.0,65:1.0,66:1.0 }
    result = count_24_last(data)
    assert expected == result
    
def amt_last_24(df):
    return df \
    .groupby(['cc_num'])['amount']\
    .rolling('24H')\
    .sum()\
    .reset_index()\
    .fillna(0).to_dict()['amount']

#testing sum in last 24 hours
def test_amt_last_24():
    data = load_data()
    expected = {0: 20.0, 1: 30.0, 2: 5.5, 3: 55.5, 4: 80.5, 5: 125.5, 6: 50.0, 7: 550.0, 8: 200.0, 9: 300.0, 10: 130.0, 11: 80.0, 12:120.0, 13: 10.0, 14:40.0, 15:55.0, 16:100.0, 17:10.0, 18: 100.0, 19: 150.0, 20:10.0, 21: 60.0, 22: 100.5, 23: 140.5, 24: 150.5, 25: 100.0, 26:10.0, 27: 60.0, 28: 55.0, 29: 10.0, 30:110.00, 31: 1200.0, 32: 10.0, 33: 50.0, 34: 100.0, 35: 210.0,36: 250.0, 37: 1800.5, 38: 3000.0, 39: 300.0, 40: 1000.0, 41: 1500.0, 42: 1510.0, 43: 2000.0, 44: 3000.0, 45:4000.0, 46: 6000, 47: 160.5, 48:200.0, 49: 2400.0, 50: 10.0,51:60.0,52:100.0,53:300.0,54:1500.0,55:100.0,56:350.5,57:100.0,58:110.5,59:20.5,60:1000.0,61: 10000.0, 62: 2110.5, 63: 1200.0, 64: 1300.0, 65: 1050.0, 66: 1100.0}
    result = amt_last_24(data)
    assert expected == result

def total_amt_30_days(df):
    df1= df \
        .groupby(['cc_num'])['amount'] \
        .rolling('30D') \
        .sum() \
        .reset_index() \
        .fillna(0)
    df1.columns = ['cc_num', 'trans_date', 'total_amt_30d']
    return df1.to_dict()['total_amt_30d']

# testing total amount spent in last 30 days
def test_total_amt_30_days():
    data = load_data()
    expected = {0: 20.0, 1: 30.0, 2: 5.5, 3: 55.5, 4: 130.5, 5: 175.5, 6: 50.0, 7: 550.0, 8: 200.0, 9: 300.0, 10: 330.0, 11:80.0, 12: 120.0, 13: 10.0, 14: 40.0, 15:95.0, 16: 100.0, 17:110.0, 18:100.0, 19: 150.0, 20:10.0, 21:60.0, 22: 160.5, 23: 200.5, 24: 210.5, 25: 100.0, 26: 10.0, 27: 60.0, 28:65.0, 29:10.0, 30:110, 31: 1200.0, 32: 10.0, 33: 60.0, 34: 160.0, 35: 270.0, 36: 310.0, 37: 1800.5, 38: 4800.5, 39: 300.0, 40: 1000.0, 41: 1500.0, 42: 1510.0, 43: 3510.0, 44: 4510.0, 45: 5510.0, 46: 7510.0, 47: 160.5, 48: 360.5, 49: 2600.0, 50: 10.0,51:60.0,52:160.0,53:300.0,54:1800.0,55:100.0,56:350.5,57:450.5,58:110.5,59:20.5,60:1000.0,61: 11000.0, 62: 2110.5, 63: 1200.0, 64: 1300.0, 65: 2350.0, 66: 1100.0}
    result = total_amt_30_days(data)
    assert expected == result

def count_trans_90D(df):
    df2= df \
            .groupby(['cc_num'])['counter'] \
            .rolling('90D') \
            .count() \
            .reset_index() \
            .fillna(0)
    df2.columns=['cc_num', 'trans_date', 'hist_trans_90d']
    return df2.to_dict()['hist_trans_90d']

# testing total number of transactions made in 90  days
def test_count_trans_90D():
        data = load_data()
        expected = {0: 1.0, 1: 2.0, 2: 1.0, 3: 2.0, 4: 3.0, 5: 4.0, 6: 5.0, 7: 6.0, 8: 3.0, 9: 4.0, 10: 5.0, 11: 1.0, 12:2.0, 13:1.0, 14: 2.0, 15:3.0, 16:4.0, 17:5.0, 18:3.0, 19 :4.0, 20: 1.0, 21: 2.0, 22: 3.0, 23: 4.0, 24: 5.0, 25: 1.0, 26: 1.0, 27: 2.0, 28: 3.0, 29: 2.0, 30: 2.0 , 31: 1.0, 32: 1.0, 33: 2.0, 34: 3.0, 35: 4.0, 36: 5.0, 37: 1.0, 38: 2.0, 39: 3.0, 40: 1.0, 41: 2.0, 42: 3.0, 43: 4.0, 44: 5.0, 45: 6.0, 46: 7.0, 47: 1.0, 48: 2.0, 49: 3.0, 50: 1.0,51:2.0,52:3.0,53:1.0,54:2.0,55:1.0,56:2.0,57:3.0,58:1.0,59:2.0,60:1.0,61: 2.0, 62: 3.0, 63: 4.0, 64: 1.0, 65: 2.0, 66: 1.0}
        result = count_trans_90D(data)
        assert expected == result

def count_merchant_trans_30D(df):
    df3= df \
        .groupby(['cc_num', 'merchant'])['counter'] \
        .rolling('30D') \
        .count() \
        .reset_index() \
        .fillna(0)
    df3.columns=['cc_num','merchant','trans_date_trans_time','merchant_count']
    return df3.to_dict()['merchant_count']

# testing number of transactions made to different merchant in last 30 days
def test_count_merchant_trans_30D():
        data = load_data()
        expected = {0: 1.0, 1: 1.0, 2: 1.0, 3: 2.0, 4: 1.0, 5: 1.0, 6: 2.0, 7: 1.0, 8: 1.0, 9: 2.0, 10: 1.0, 11: 1.0, 12: 2.0, 13: 1.0, 14: 1.0, 15: 1.0, 16:1.0, 17: 1.0, 18: 1.0, 19:2.0,20: 1.0, 21: 1.0, 22:1.0, 23:1.0, 24:1.0, 25:2.0, 26: 1.0, 27:1.0, 28: 1.0, 29:1.0, 30:1.0, 31: 1.0, 32: 1.0, 33: 1.0, 34: 2.0, 35: 1.0, 36: 1.0, 37: 2.0, 38: 3.0, 39: 1.0, 40: 1.0, 41: 1.0, 42: 2.0, 43: 3.0, 44: 4.0, 45: 5.0, 46: 1.0, 47: 1.0, 48: 1.0, 49: 2.0, 50: 1.0,51:1.0,52:2.0,53:1.0,54:1.0,55:1.0,56:1.0,57:2.0,58:1.0,59:1.0,60:1.0,61: 1.0, 62: 1.0, 63: 1.0, 64: 1.0, 65: 1.0, 66: 1.0}
        result = count_trans_90D(data)
        result = count_merchant_trans_30D(data)
        assert expected == result

def count_trans_category_30D(df):
    df4= df \
            .groupby(['cc_num','category'])['counter'] \
            .rolling('30D') \
            .count() \
            .reset_index() \
            .fillna(0)
    df4.columns=['cc_num','category', 'trans_date','category_type_count']
    return df4.to_dict()['category_type_count']

# testing total number of transactions made to different category in last 30 days
def test_count_trans_category_30D():
        data = load_data()
        expected = {0: 1.0, 1: 2.0, 2: 1.0, 3: 1.0, 4: 2.0, 5: 1.0, 6: 1.0, 7: 1.0, 8: 1.0, 9: 1.0, 10: 2.0, 11:1.0, 12: 1.0, 13:1.0, 14: 1.0, 15: 1.0, 16: 2.0, 17:1.0, 18: 2.0, 19:1.0, 20: 1.0, 21:1.0, 22: 1.0, 23: 2.0, 24: 3.0, 25: 1.0, 26:1.0, 27: 1.0, 28:1.0, 29:2.0, 30:1.0, 31: 2.0, 32: 1.0, 33: 1.0, 34: 1.0, 35: 1.0, 36: 2.0, 37: 3.0, 38: 1.0, 39: 2.0, 40: 1.0, 41: 1.0, 42: 1.0, 43: 1.0, 44: 1.0, 45: 2.0, 46: 1.0, 47: 1.0, 48: 1.0, 49: 1.0, 50: 1.0,51:1.0,52:1.0,53:1.0,54:1.0,55:1.0,56:2.0,57:1.0,58:1.0,59:1.0,60:1.0,61: 1.0, 62: 1.0, 63: 1.0, 64: 1.0, 65: 1.0, 66: 1.0}
        result = count_trans_category_30D(data)
        assert expected == result

def avg_amt_trans_30D(df):
    df5= df \
            .groupby(['cc_num'])['amount'] \
            .rolling('30D') \
            .mean() \
            .reset_index() \
            .fillna(0)
    df5.columns=['cc_num', 'trans_date','hist_trans_avg_amt_30d']
    return df5.to_dict()['hist_trans_avg_amt_30d']

# testing average amount of transaction made in last 30 days
def test_avg_amt_trans_30D():
        data = load_data()
        expected = {0: 20.0, 1: 15.0, 2: 5.5, 3: 27.75, 4: 65.25, 5: 58.5, 6: 50.0, 7: 275.0, 8: 200.0, 9: 150.0, 10: 110.0, 11:80.0, 12:60.0, 13:10.0, 14:20.0, 15:31.666666666666668, 16:100.0, 17:55.0, 18: 100.0, 19:75.0, 20: 10.0, 21: 30.0, 22: 53.5, 23: 50.125, 24: 42.1, 25: 100.0, 26: 10.0, 27: 30.0, 28: 21.666666666666668, 29: 10.0, 30: 55.0, 31: 1200.0,32: 10.0, 33:30.0, 34: 53.333333333333336, 35: 67.5, 36: 62.0, 37: 1800.5, 38: 2400.25, 39: 300.0, 40:1000.0, 41:750.0, 42: 503.3333333333333, 43: 877.5, 44: 902.0, 45:918.3333333333334, 46: 1072.857142857143, 47: 160.5,48:180.25,49:1300.0,50:10.0,51:30.0,52:53.333333333333336 , 53:300.0,54:900.0,55:100.0,56:175.25,57:150.16666666666666,58:110.5,59:20.5,60:1000.0,61: 5500.0, 62: 2110.5, 63: 1200.0, 64: 1300.0, 65: 1175.0, 66: 1100.0}
        result = avg_amt_trans_30D(data)
        assert expected == result

def amt_category_30D(df):
    df6= df \
            .groupby(['cc_num','category'])['amount'] \
            .rolling('30D') \
            .sum() \
            .reset_index() \
            .fillna(0)
    df6.columns=['cc_num','category', 'trans_date','hist_category_amt_30d']
    return df6.to_dict()['hist_category_amt_30d']

# testing total amount spent in different category in last 30 days
def test_amt_category_30D():
        data = load_data()
        expected = {0: 20.0, 1: 30.0, 2: 50.0, 3: 100.0, 4: 130.0, 5: 50.0, 6: 200.0, 7: 80.0, 8: 5.5, 9: 80.5,10: 125.5, 11: 500.0, 12: 40.0, 13: 55.0, 14: 100.0, 15: 10.0, 16: 40.0, 17: 100.0, 18: 110.0, 19: 50.0, 20: 40.0, 21: 10.0, 22: 50.0, 23: 150.5, 24: 160.5, 25: 100.0, 26: 1200.0, 27: 10.0, 28: 50.0, 29: 55.0, 30: 10.0, 31: 110.0, 32: 1800.5, 33: 300.0, 34: 3000.0, 35: 10.0, 36:110.0, 37: 220.0, 38: 50.0, 39: 90.0, 40: 200.0, 41: 2400.0, 42: 160.5, 43: 1000.0, 44: 10.0, 45: 2010.0, 46: 500.0, 47: 2000.0, 48: 1000.0,49: 1000.0, 50: 1500.0,51: 300.0, 52: 100.0, 53: 10.0,54: 50.0,55: 100.0, 56: 350.5, 57: 100.0, 58:20.5, 59: 110.5,60: 2110.5,61: 10000.0, 62: 1200.0, 63: 1000.0, 64: 1300.0, 65: 1050.0, 66: 1100.0}
        result = amt_category_30D(data)
        assert expected == result

# testing number and name of columns
def test_no_of_columns():
    df=load_data()
    assert (list(df.columns)) == ['Unnamed: 0','time', 'cc_num', 'amount', 'merchant', 'category', 'gender', 'first', 'last', 'age', 'city_pop', 'state', 'Time', 'counter']

# testing age (must be positive)
def test_age():
    df=load_data()
    assert df[df['age']<0].shape[0]==0

# testing amount of transaction made (must be positive)
def test_transaction_amt():
    df=load_data()
    assert df[df['amount']<0].shape[0]==0
    
# cc_num (must be positive)
def test_cc_num():
    df=load_data()
    assert df[df['cc_num']<0].shape[0]==0
    
