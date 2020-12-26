import pandas as pd
import numpy as np
import datetime
import os


class StockAnalysis:
    def __init__(self, rawfile, buffer=0, slreverse=True):
        self.rawfile = rawfile
        self.buffer = buffer
        self.slreverse = slreverse
        filename = os.path.basename(rawfile)
        self.filename = filename.split('.')[0]

    def process_raw_data(self):
        df = pd.read_csv(self.rawfile, names=['Ticket', 'Date', 'Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'OI'])
        df['Date'] = df['Date'].astype(str)
        df['Time'] = df['Time'].astype(str)
        df['Date'] = pd.to_datetime(df['Date'] + ' ' + df['Time'])
        premarket_list = list()
        postmarket_list = list()
        timeset = set(df['Time'].tolist())
        for i in timeset:
            hr, min = str(i).split(':')
            if int(hr) < 9:
                premarket_list.append(i+':00')
            elif int(hr) == 9 and int(min) <= 15:
                premarket_list.append(i+':00')
            elif int(hr) > 15:
                postmarket_list.append(i+':00')
            elif int(hr) == 15 and int(min) >= 31:
                postmarket_list.append(i+':00')
        df = df.drop(['Time'], axis=1)
        self.pre_post_market(df, premarket_list, postmarket_list)

    def pre_post_market(self, df, premarket_tm, postmarket_tm):
        del_data = list()
        premarket = list()
        for i in df.index:
            time = datetime.datetime.strptime(str(df['Date'][i]), "%Y-%m-%d %H:%M:%S").time()
            date = datetime.datetime.strptime(str(df['Date'][i]), "%Y-%m-%d %H:%M:%S").date()
            if str(time) in premarket_tm:
                premarket.append(df.iloc[i].to_dict())
                del_data.append(i)
            elif str(time) in ['09:16:00']:
                if premarket:
                    predate = datetime.datetime.strptime(str(premarket[0]['Date']), "%Y-%m-%d %H:%M:%S").date()
                    if True if date == predate else False:
                        new_time = df['Date'][i] + datetime.timedelta(minutes=-1)
                        df.at[i, 'Date'] = new_time
                        df.at[i, 'Open'] = premarket[0]['Open']
                        df.at[i, 'High'] = max([premarket[0]['High'], df['High'][i]])
                        df.at[i, 'Low'] = min([premarket[0]['Low'], df['Low'][i]])
                        df.at[i, 'Volume'] = sum([premarket[0]['Volume'], df['Volume'][i]])
                        df.at[i, 'OI'] = sum([premarket[0]['OI'], df['OI'][i]])
                        premarket.clear()
                    else:
                        print(False)
                else:
                    new_time = df['Date'][i] + datetime.timedelta(minutes=-1)
                    df.at[i, 'Date'] = new_time
            elif str(time) in postmarket_tm:
                del_data.append(i)
            else:
                new_time = df['Date'][i] + datetime.timedelta(minutes=-1)
                df.at[i, 'Date'] = new_time

        df.drop(df.index[del_data], inplace=True)
        df.sort_values(by=['Date'], inplace=True)
        self.split_15(df)

    def split_15(self, df):
        u = (df.assign(timestamp=df['Date'].dt.floor('20min'))
             .groupby(pd.Grouper(key='Date', freq='15min'))
             .ngroup())
        df['15min_period'] = np.char.add('period_', (pd.factorize(u)[0] + 1).astype(str))
        grouped = df.groupby('15min_period')
        triggered_point = None
        entryHit = None
        data = list()
        triggered_data = list()
        for i in df['15min_period'].unique():
            grp1 = grouped.get_group(i)
            top1 = grp1.head(1).to_dict('records')[0]
            last1 = grp1.tail(1).to_dict('records')[0]
            time = datetime.datetime.strptime(str(top1['Date']), "%Y-%m-%d %H:%M:%S").time()
            date = datetime.datetime.strptime(str(top1['Date']), "%Y-%m-%d %H:%M:%S").date()
            cur_min = str(time).split(':')
            if int(cur_min[1]) not in [15, 30, 45, 0]:
                currdate = str(date).split('-')
                if 14 >= int(cur_min[1]):
                    top1['Date'] = datetime.datetime(year=int(currdate[0]), month=int(currdate[1]), day=int(currdate[-1]),
                                                     hour=int(cur_min[0]), minute=0, second=int(cur_min[-1]))
                elif 29 >= int(cur_min[1]) >= 15:
                    top1['Date'] = datetime.datetime(year=int(currdate[0]), month=int(currdate[1]), day=int(currdate[-1]),
                                                     hour=int(cur_min[0]), minute=15, second=int(cur_min[-1]))
                elif 44 >= int(cur_min[1]) >= 30:
                    top1['Date'] = datetime.datetime(year=int(currdate[0]), month=int(currdate[1]), day=int(currdate[-1]),
                                                     hour=int(cur_min[0]), minute=30, second=int(cur_min[-1]))
                elif 59 >= int(cur_min[1]) >= 45:
                    top1['Date'] = datetime.datetime(year=int(currdate[0]), month=int(currdate[1]), day=int(currdate[-1]),
                                                     hour=int(cur_min[0]), minute=45, second=int(cur_min[-1]))
                time = datetime.datetime.strptime(str(top1['Date']), "%Y-%m-%d %H:%M:%S").time()
                date = datetime.datetime.strptime(str(top1['Date']), "%Y-%m-%d %H:%M:%S").date()
            if triggered_point:         # to check if trade was triggered
                if triggered_point[0] == date:
                    if triggered_point[1] is not time:
                        if triggered_point[-1] == 'Short':
                            if entryHit:
                                trigpt_high = triggered_point[2] + (triggered_point[2]*self.buffer/100)        # adding buffer
                                if trigpt_high <= grp1['High'].max():    # check if stop loss has hit
                                    triggered_data.append([top1['Ticket'], entryHit['Entry_time'][0], entryHit['Entry_time'][1], 'Short', triggered_point[3],
                                                           triggered_point[2], triggered_point[2], time])
                                    entryHit = None
                                    triggered_point = None
                            if triggered_point and triggered_point[3] >= grp1['Low'].min() and entryHit is None:
                                entryHit = {'Entry_time': [date, time]}

                        elif triggered_point[-1] == 'Long':
                            if entryHit:
                                trigpt_low = triggered_point[3] + (triggered_point[3]*self.buffer/100)       # adding buffer
                                if trigpt_low >= grp1['Low'].min():         # check if stop loss has hit
                                    triggered_data.append([top1['Ticket'], date, time, 'Long', triggered_point[2],
                                                           triggered_point[3], triggered_point[3], time])
                                    entryHit = None
                                    triggered_point = None
                            if triggered_point and triggered_point[2] <= grp1['High'].max() and entryHit is None:
                                entryHit = {'Entry_time': [date, time]}
                        else:
                            pass
                else:
                    if entryHit:
                        triggdate = datetime.datetime.strptime(str(data[-2][1]), "%Y-%m-%d %H:%M:%S").date()
                        triggtime = datetime.datetime.strptime(str(data[-2][1]), "%Y-%m-%d %H:%M:%S").time()
                        if triggered_point[0] == triggdate and str(triggtime) == '15:00:00':
                            closeprice = data[-2][5]
                        else:
                            closeprice = 'NA'
                        triggered_data.append([top1['Ticket'], entryHit['Entry_time'][0], entryHit['Entry_time'][1],
                                               triggered_point[-1], triggered_point[3] if triggered_point[-1] == 'Short' else triggered_point[2],
                                               triggered_point[2] if triggered_point[-1] == 'Short' else triggered_point[3], closeprice, '15:00:00'])
                        entryHit = None
                    triggered_point = None

            if str(time) == '09:45:00':
                if data[-1][3] < data[-2][3] and data[-1][4] > data[-2][4]:
                    if grp1['High'].max() > data[-2][3]:
                        order_typ = 'Long'
                        triggered_point = ([date, time, grp1['High'].max(), grp1['Low'].min(), order_typ])
                    elif grp1['Low'].min() < data[-2][4]:
                        order_typ = 'Short'
                        triggered_point = ([date, time, grp1['High'].max(), grp1['Low'].min(), order_typ])
                    else:
                        order_typ = None
                else:
                    order_typ = None
            else:
                order_typ = None
            data.append([top1['Ticket'], top1['Date'], top1['Open'], grp1['High'].max(), grp1['Low'].min(),
                         last1['Close'], grp1['Volume'].sum(), grp1['OI'].sum(), order_typ])

        outdf = pd.DataFrame(data, columns=['Ticker', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'OI', 'Order_Type'])
        triggered_data_df = pd.DataFrame(triggered_data, columns=['Ticker', 'Date', 'Time', 'OrderType', 'Entry', 'StopLoss', 'Exit', 'Exit_Time'])
        if self.slreverse:
            self.apply_slreverse(triggered_data_df, outdf)
        return outdf, triggered_data_df

    def apply_slreverse(self, trig_df, basedf):
        old_trig_df = trig_df
        oldtrigdf = []
        sldf = trig_df.loc[trig_df['Exit_Time'] != '15:00:00']
        for row in sldf.to_dict('records'):
            datdf = basedf.loc[basedf['Date'].dt.strftime('%Y-%m-%d') == str(row['Date'])]
            newtdf = datdf.loc[datdf['Date'].dt.strftime('%H:%M:%S') >= str(row['Exit_Time'])]
            if row['OrderType'] == 'Short':
                for row2 in newtdf.to_dict('records'):
                    if row['Entry'] >= row2['Low']:
                        oldtrigdf.append({'Ticker': row['Ticker'], 'Date': row['Date'], 'Time': row['Exit_Time'],
                                          'OrderType': 'SL_Long', 'Entry': row['StopLoss'], 'StopLoss': row['Entry'],
                                          'Exit': row['Entry'], 'Exit_Time': str(row2['Date']).split(' ')[-1]})
                        break
                    elif str(row2['Date']).split(' ')[-1] == '15:00:00':
                        oldtrigdf.append({'Ticker': row['Ticker'], 'Date': row['Date'], 'Time': row['Exit_Time'],
                                          'OrderType': 'SL_Long', 'Entry': row['StopLoss'],
                                          'StopLoss': row['Entry'],
                                          'Exit': row2['Close'], 'Exit_Time': '15:00:00'})
                        break
            else:
                for row2 in newtdf.to_dict('records'):
                    if row['Entry'] <= row2['High']:
                        oldtrigdf.append({'Ticker': row['Ticker'], 'Date': row['Date'], 'Time': row['Exit_Time'],
                                          'OrderType': 'SL_Short', 'Entry': row['StopLoss'], 'StopLoss': row['Entry'],
                                          'Exit': row['Entry'], 'Exit_Time': str(row2['Date']).split(' ')[-1]})
                        break
                    elif str(row2['Date']).split(' ')[-1] == '15:00:00':
                        oldtrigdf.append({'Ticker': row['Ticker'], 'Date': row['Date'], 'Time': row['Exit_Time'],
                                          'OrderType': 'SL_Short', 'Entry': row['StopLoss'],
                                          'StopLoss': row['Entry'],
                                          'Exit': row2['Close'], 'Exit_Time': '15:00:00'})
                        break
        for slrow in oldtrigdf:
            old_trig_df = old_trig_df.append(slrow, ignore_index=True)
        old_trig_df.sort_values(by=['Date'], inplace=True)

        basedf.to_excel('15minsData_' + self.filename + '.xlsx', index=False)
        old_trig_df.to_excel(self.filename + '.xlsx', index=False)



files = os.listdir('StockData/2017')
for file in files:
    obj = StockAnalysis(os.path.abspath('StockData/2017/' + file))  # user buffer=percentage to add buffer on stop loss
    obj.process_raw_data()
