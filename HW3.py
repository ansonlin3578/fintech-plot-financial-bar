import glob
from time import time
import pandas as pd
import os
import pdb
import matplotlib.pyplot as plt
import mplfinance as mpf

data_path = r"./HW3_data"
csv_files = glob.glob(data_path + "\*.csv")

print("file names : " , csv_files)
df_container = []
for f in csv_files:
    df = pd.read_csv(f , encoding='big5' , low_memory=False)
    # print(df)
    df_container.append(df)

df_merge = pd.concat(df_container , axis=0 , ignore_index=True)
TX_output = df_merge.loc[(df_merge["商品代號"] == "TX     ")&(df_merge["到期月份(週別)"] == "202110     ")&(df_merge["成交時間"]>=84500)&(df_merge["成交時間"]<=134500)]
TX_output = TX_output.drop(columns=["近月價格" , "遠月價格" , "開盤集合競價 "])
print(TX_output.shape)

excel_limit_split = 1048575
output_1 = TX_output[0 : excel_limit_split]
output_2 = TX_output[excel_limit_split : ]
if os.path.exists("./TX_excel_1.csv"):
    pass
else:
    TX_excel_1= output_1.to_csv("./TX_excel_1.csv" , encoding='utf_8_sig' , index=False)
    # TX_excel_2= output_2.to_csv("./TX_excel_2.csv" , encoding='utf_8_sig' , index=False)

#/////////////////////////////////time bar///////////////////////////////////////
per_day = TX_output.groupby(["成交日期"])
each_date = [20210916, 20210917, 20210922, 20210923, 20210924, 20210927, 20210928, 20210929, 20210930, 20211001, 20211004]
timebar_result = []
number_of_ticks= []
for date in each_date:
    highest = per_day.get_group(date).loc[ : ,"成交價格"].max()
    lowest = per_day.get_group(date).loc[ : ,"成交價格"].min()
    volumn = per_day.get_group(date).loc[ : ,"成交數量(B+S)"].mean()
    open_price = per_day.get_group(date).iloc[1, 4]
    close_price = per_day.get_group(date).iloc[-1, 4]
    temp = [str(date), volumn , open_price , highest , lowest , close_price]
    timebar_result.append(temp)
    tick_count = len(per_day.get_group(date))
    number_of_ticks.append(tick_count)
    
time_bar = pd.DataFrame(timebar_result , columns=["Date", "Volume" , "Open" , "High" , "Low", "Close"])
time_bar["Date"] = pd.to_datetime(time_bar.loc[ : , "Date"], format='%Y-%m-%d')
time_bar.set_index("Date", inplace=True)
mpf.plot(time_bar, type="candle", title="Time Bar" , style='yahoo' , savefig='Time_Bar.png') #綠漲/紅跌

date_idx = [i for i in range(len(each_date))]
plt.xlabel("Time")
plt.ylabel("Number Of Ticks")
plt.bar(date_idx , number_of_ticks , color="r")
plt.suptitle('Number of Ticks when group by date')
plt.grid(False)
plt.savefig('Time_Num_of_ticks.png',dpi=400)
plt.show()

#/////////////////////////////////tick bar///////////////////////////////////////
data_len = len(TX_output)
tick_result = []
number_of_ticks =[]
for i in range(data_len//10000 + 1):
    start_idx = i * 10000
    if i == data_len // 10000:
        # pdb.set_trace()
        end_idx = data_len
    else:
        end_idx = (i + 1) * 10000
    tick_start_date = TX_output[start_idx : end_idx].iloc[1,0]
    highest = TX_output[start_idx : end_idx].loc[ : ,"成交價格"].max()
    lowest = TX_output[start_idx : end_idx].loc[ : ,"成交價格"].min()
    open_price = TX_output[start_idx : end_idx].iloc[1, 4]
    close_price = TX_output[start_idx : end_idx].iloc[-1, 4]
    volume = TX_output[start_idx : end_idx].loc[ : ,"成交數量(B+S)"].mean()
    temp = [str(tick_start_date) , volume , open_price, highest , lowest , close_price]
    tick_result.append(temp)

    number_of_ticks.append(end_idx - start_idx)

tick_bar = pd.DataFrame(tick_result , columns=["Date", "Volume", "Open", "High" , "Low" , "Close"])
tick_bar["Date"] = pd.to_datetime(tick_bar.loc[ : , "Date"], format='%Y-%m-%d')
tick_bar.set_index("Date", inplace=True)
mpf.plot(tick_bar, type="candle", title="Tick Bar" , style='yahoo' , savefig='Tick_Bar.png') 

Tick_idx = [i for i in range(data_len//10000 + 1)]
plt.xlabel("Time")
plt.ylabel("Number Of Ticks")
plt.bar(Tick_idx , number_of_ticks , color="r")
plt.suptitle('Number of Ticks when group by Tick')
plt.grid(False)
plt.savefig('Tick_Num_of_ticks.png',dpi=400)
plt.show()


# ////////////////////////////////////volume_bar///////////////////////////////////////////
TX_output = TX_output.assign(cumsum = TX_output.loc[: , "成交數量(B+S)"].cumsum())
TX_output = TX_output.assign(gp_idx = TX_output.loc[: , "cumsum"].values // 100000)

gp_by_volume = TX_output.groupby(["gp_idx"])
volume_result = []
number_of_ticks =[]
for i in range(len(gp_by_volume)):
    highest = gp_by_volume.get_group(i).loc[ : , "成交價格"].max()
    lowest = gp_by_volume.get_group(i).loc[ : , "成交價格"].min()
    open_price = gp_by_volume.get_group(i).iloc[1 , 4]
    close_price = gp_by_volume.get_group(i).iloc[-1 , 4]
    volume = gp_by_volume.get_group(i).loc[ : , "成交數量(B+S)"].mean()
    volume_start_date = gp_by_volume.get_group(i).iloc[1,0]
    temp = [str(volume_start_date) , highest , lowest , open_price , close_price , volume]
    volume_result.append(temp)

    number_of_ticks.append(len(gp_by_volume.get_group(i)))

volume_bar = pd.DataFrame(volume_result , columns=["Date","High" , "Low", "Open" , "Close", "Volume"])
volume_bar["Date"] = pd.to_datetime(volume_bar.loc[ : , "Date"], format='%Y-%m-%d')
volume_bar.set_index("Date", inplace=True)
mpf.plot(volume_bar, type="candle", title="Volume Bar" , style='yahoo' , savefig='Volume_Bar.png') 

volume_idx = [i for i in range(len(gp_by_volume))]
plt.xlabel("Time")
plt.ylabel("Number Of Ticks")
plt.bar(volume_idx , number_of_ticks , color="r")
plt.suptitle('Number of Ticks when group by Volume')
plt.grid(False)
plt.savefig('Volume_Num_of_ticks.png',dpi=400)
plt.show()
# ////////////////////////////////////dollar_bar///////////////////////////////////////////
TX_output = TX_output.assign(dollar_per_trade = TX_output.loc[:,"成交價格"] * TX_output.loc[:,"成交數量(B+S)"])
TX_output = TX_output.assign(cumsum_dollar = TX_output.loc[:,"dollar_per_trade"].cumsum())
TX_output = TX_output.assign(dollar_idx = TX_output.loc[:,"cumsum_dollar"].values // 1000_000_000)
gp_by_dollar = TX_output.groupby(["dollar_idx"])
dollar_result = []
number_of_ticks =[]
for i in range(len(gp_by_dollar)):
    highest = gp_by_dollar.get_group(i).loc[ : , "成交價格"].max()
    lowest = gp_by_dollar.get_group(i).loc[ : , "成交價格"].min()
    open_price = gp_by_dollar.get_group(i).iloc[1 , 4]
    close_price = gp_by_dollar.get_group(i).iloc[-1 , 4]
    volume = gp_by_dollar.get_group(i).loc[ : , "成交數量(B+S)"].mean()
    dollar_start_date = gp_by_dollar.get_group(i).iloc[1,0]
    temp = [str(dollar_start_date) , highest , lowest , open_price , close_price , volume]
    dollar_result.append(temp)

    number_of_ticks.append(len(gp_by_dollar.get_group(i)))

dollar_bar = pd.DataFrame(dollar_result , columns=["Date","High" , "Low", "Open" , "Close", "Volume"])
dollar_bar["Date"] = pd.to_datetime(dollar_bar.loc[ : , "Date"], format='%Y-%m-%d')
dollar_bar.set_index("Date", inplace=True)
mpf.plot(dollar_bar, type="candle", title="Dollar Bar" , style='yahoo' , savefig='Dollar_Bar.png') 

dollar_idx = [i for i in range(len(gp_by_dollar))]
plt.xlabel("Time")
plt.ylabel("Number Of Ticks")
plt.bar(dollar_idx , number_of_ticks , color="r")
plt.suptitle('Number of Ticks when group by Dollar')
plt.grid(False)
plt.savefig('Dollar_Num_of_ticks.png',dpi=400)
plt.show()
# pdb.set_trace()
