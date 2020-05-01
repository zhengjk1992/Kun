# -*- coding: utf-8 -*-
import math
import pandas as pd
from sqlalchemy import create_engine
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
import warnings
import time

warnings.filterwarnings("ignore")

plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号
# pd.set_option('display.max_rows',None)
# pd.set_option('display.max_columns',None)

def rad(d):  # 辅助函数
    return d * math.pi / 180.0

# 根据经纬度计算球面距离
def getDistance(lat1, lng1, lat2, lng2):
    radLat1 = rad(lat1)
    radLat2 = rad(lat2)
    a = rad(lat1) - rad(lat2)
    b = rad(lng1) - rad(lng2)
    s = 2000 * math.asin(math.sqrt(
        math.pow(math.sin(a / 2), 2) + math.cos(radLat1) * math.cos(radLat2) * math.pow(math.sin(b / 2), 2))) * 6378.137

    return s

# 所有的加油记录都处理
def func1(minutes_param=10, user='xt_zjk', pwd='Xtz_r019', link_port='10.234.237.39:15258', db='audit_mx'):

    # 初始化数据库连接，使用pymysql模块MySQL的用户：root, 密码:147369, 端口：3306,数据库：test
    conn = 'mysql+pymysql://' + user + ':' + pwd + '@' + link_port + '/' + db
    # engine = create_engine('mysql+pymysql://root:123456@localhost:3306/test')
    engine = create_engine(conn)
    sql = ' select seq,VEHICLE_NUMBER,fill_time,gas,station from audit_mx.mod_vehicle_fill;' # 查询语句,待补充

    df_gasoline = pd.read_sql_query(sql, engine)  # 读取所有加油记录 （主键、车牌号vehicle_number、加油时间fill_time、加油量gas、车站名称station）

    df_gasoline['RECORD_TIME'] = None
    df_gasoline['dif_time'] = None
    df_gasoline['dx'] = None
    df_gasoline['dy'] = None
    df_gasoline[u'距离加油站（米）'] = None
    df_gasoline[u'数据有效性'] = None
    # print(df_gasoline)
     # 填充每一条加油数据
    for index, row in df_gasoline.iterrows():  # vehicle_number,RECORD_TIME,dx,dy
        strtime1 = time.clock()
        # print('------------------------------------------------------------')
        # print(index)
        datatime_gasoline = row["fill_time"]  # 获取时间
        # print(datatime_gasoline)
        starttime = datatime_gasoline - pd.DateOffset(minutes=minutes_param)  # 前30分钟
        endtime = datatime_gasoline + pd.DateOffset(minutes=minutes_param)  # 后30分钟
        vehicle_number = row["VEHICLE_NUMBER"]  # 车牌号
        # print(vehicle_number)
        sql = " SELECT t1.VEHICLE_NUMBER,t.dx,t.dy,t.RECORD_TIME FROM DWH_SGUVM.UVMP_MON_TRACK_HIS  t INNER JOIN ( SELECT m.ID,m.VEHICLE_NUMBER FROM DWH_SGUVM.UVMP_INF_VEHICLE m where m.vehicle_number= '%s' ) t1 ON t.VEHICLE_ID=t1.id WHERE  T.RECORD_TIME BETWEEN '%s'  AND  '%s' "  %(vehicle_number,starttime,endtime)
        df = pd.read_sql_query(sql, engine)  # 获取全部正负30分钟之内的记录
        # print(df)
        if df.empty is True:
            df_gasoline.set_value(index, u'数据有效性', "数据无效")
        else:
            minindex = df['RECORD_TIME'].apply(lambda x: abs(x - datatime_gasoline)).idxmin()  # 时间相差最小的数据行索引
            df_gasoline.set_value(index, 'RECORD_TIME', df['RECORD_TIME'][minindex])
            df_gasoline.set_value(index, 'dif_time', abs(df['RECORD_TIME'][minindex] - datatime_gasoline))
            df_gasoline.set_value(index, 'dy', df['dy'][minindex])
            df_gasoline.set_value(index, 'dx', df['dx'][minindex])
            # print(df['RECORD_TIME'][minindex])
        strtime2 = time.clock()
        print("第"+str(index)+"条"+str(round(strtime2-strtime1,2))+"秒")
    print(df_gasoline)
    df_gasoline.to_csv("temp.csv")

    return df_gasoline

def func2(path='temp.csv',distance=1000):
    dataall = pd.read_csv(path,encoding='GBK')
    datafailed = dataall[dataall[u'数据有效性'] == dataall[u'数据有效性']]
    data = dataall[dataall['dx'] == dataall['dx']]
    data = data.reset_index(drop=True)
    # print(data)
    count_gasstation = len(data['station'].value_counts())  # 加油站的个数
    # print(count_gasstation)
    model = KMeans(n_clusters=count_gasstation)  # 分为加油站个数个类
    model.fit(data[['dy', 'dx']])  # 开始聚类
    # print(data)
    # print("--------------")
    # print(pd.Series(model.labels_))
    finaldata = pd.concat([data, pd.Series(model.labels_)], axis=1)  # 每个样本对应的类别
    finaldata.columns = list(data.columns) + [u'聚类类别']  # 重命名表头

    # print(finaldata)
    for index, row in finaldata.iterrows():
        cluster = int(row[u'聚类类别'])
        dis = getDistance(row['dy'], row['dx'], model.cluster_centers_[cluster][0], model.cluster_centers_[cluster][1])
        finaldata.set_value(index, u'距离加油站（米）', dis)

        if dis > distance:
            finaldata.set_value(index, u'数据有效性', "数据可能为假")
        else:
            finaldata.set_value(index, u'数据有效性', "数据可能为真")
    returndata = pd.concat([finaldata, datafailed], axis=0)
    returndata = returndata.sort_values('seq', ascending=True)
    returndata = returndata.set_index('seq')
    returndata = returndata.drop(['Unnamed: 0'],axis=1)

    return returndata

if __name__ == '__main__':
    data = func1(minutes_param=30)
    r = func2(path='temp.csv')
    r.to_csv("11.csv",distance=1000)
    # print(r)

