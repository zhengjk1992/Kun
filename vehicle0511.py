# -*- coding: utf-8 -*-
import math
from sklearn.cluster import KMeans
import warnings
import time
import tkinter
import pandas as pd
from pyecharts.charts import Geo
from pyecharts import options as opts
from pyecharts.globals import GeoType

warnings.filterwarnings("ignore")


def rad(d):  # 辅助函数
    return d * math.pi / 180.0


def getDistance(lat1, lng1, lat2, lng2):  # 根据经纬度计算球面距离
    radLat1 = rad(lat1)
    radLat2 = rad(lat2)
    a = rad(lat1) - rad(lat2)
    b = rad(lng1) - rad(lng2)
    s = 2000 * math.asin(math.sqrt(
        math.pow(math.sin(a / 2), 2) + math.cos(radLat1) * math.cos(radLat2) * math.pow(math.sin(b / 2), 2))) * 6378.137

    return s


def time_dis(distance=1000, timedif=30, dist=1000):
    if dist > distance:
        return 1  # 数据疑似为假
    else:
        return 2  # 数据疑似为真


def func1(path='加油20200504.xlsx',time =99999999):
    df = pd.read_excel('加油20200504.xlsx')
    data_set = df
    df = df[['station', 'dx', 'dy', 'dif']]
    df.iloc

    grouped = df.groupby(['station'])['dif'].idxmin()
    grouped = grouped.values.tolist()

    station_loc = df.iloc[grouped].reset_index(drop=True)
    # station_loc = station_loc[station_loc['dif'] < time]
    station_loc.to_csv("station_loc.csv", index=False)

    new_col = ['station', 'dx_station', 'dy_station','dif_station']
    station_loc.columns = new_col

    all = data_set.join(station_loc.set_index('station'), on='station')
    all = all.reset_index()
    all['dis'] = None
    for index, row in all.iterrows():

        dis = getDistance(row['dy'], row['dx'], row['dy_station'],row['dx_station'])
        all.set_value(index, 'dis', dis)
    all.drop(['id'],axis=1,inplace=True)
    all.to_csv('temp.csv', index=False)
    return all



def func2(path='temp.csv', distance=1000, timedif=30):
    timedif *= 60
    data = pd.read_csv(path)
    data['flag_legal'] = None
    data['dis'] = None

    for index, row in data.iterrows():
        if row['dif'] > timedif:
            data.set_value(index, 'flag_legal', 0)
        else:
            dis = getDistance(row['dy'], row['dx'], row['dy_station'],row['dx_station'])
            data.set_value(index, 'dis', dis)
            data.set_value(index, 'flag_legal', time_dis(distance, timedif, dis))
    data.to_csv('answer.csv')
    return data





if __name__ == '__main__':
    func1(path='加油20200504.xlsx',time =30)
    func2(path='temp.csv', distance=200, timedif=10)




    # print(getDistance(117.048458, 28.283899, 117.048303, 28.283826))