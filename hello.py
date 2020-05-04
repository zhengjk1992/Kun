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

def getDistance(lat1, lng1, lat2, lng2):# 根据经纬度计算球面距离
    radLat1 = rad(lat1)
    radLat2 = rad(lat2)
    a = rad(lat1) - rad(lat2)
    b = rad(lng1) - rad(lng2)
    s = 2000 * math.asin(math.sqrt(
        math.pow(math.sin(a / 2), 2) + math.cos(radLat1) * math.cos(radLat2) * math.pow(math.sin(b / 2), 2))) * 6378.137

    return s

def time_dis(distance=1000,timedif=30,dist=1000):

    if dist > distance:
        return 1  #数据疑似为假
    else:
        return 2  #数据疑似为真

# def func1(minutes_param=10, user='xt_zjk', pwd='Xtz_r019', link_port='10.234.237.39:15258', db='audit_mx'):
#
#     # 初始化数据库连接，使用pymysql模块MySQL的用户：root, 密码:147369, 端口：3306,数据库：test
#     conn = 'mysql+pymysql://' + user + ':' + pwd + '@' + link_port + '/' + db
#     # engine = create_engine('mysql+pymysql://root:123456@localhost:3306/test')
#     engine = create_engine(conn)
#     sql = ' select seq,VEHICLE_NUMBER,fill_time,gas,station from audit_mx.mod_vehicle_fill;' # 查询语句,待补充
#
#     df_gasoline = pd.read_sql_query(sql, engine)  # 读取所有加油记录 （主键、车牌号vehicle_number、加油时间fill_time、加油量gas、车站名称station）
#
#     df_gasoline['RECORD_TIME'] = None
#     df_gasoline['dif_time'] = None
#     df_gasoline['dx'] = None
#     df_gasoline['dy'] = None
#     df_gasoline[u'距离加油站（米）'] = None
#     df_gasoline[u'数据有效性'] = None
#     # print(df_gasoline)
#      # 填充每一条加油数据
#     for index, row in df_gasoline.iterrows():  # vehicle_number,RECORD_TIME,dx,dy
#         strtime1 = time.clock()
#         # print('------------------------------------------------------------')
#         # print(index)
#         datatime_gasoline = row["fill_time"]  # 获取时间
#         # print(datatime_gasoline)
#         starttime = datatime_gasoline - pd.DateOffset(minutes=minutes_param)  # 前30分钟
#         endtime = datatime_gasoline + pd.DateOffset(minutes=minutes_param)  # 后30分钟
#         vehicle_number = row["VEHICLE_NUMBER"]  # 车牌号
#         # print(vehicle_number)
#         sql = " SELECT t1.VEHICLE_NUMBER,t.dx,t.dy,t.RECORD_TIME FROM DWH_SGUVM.UVMP_MON_TRACK_HIS  t INNER JOIN ( SELECT m.ID,m.VEHICLE_NUMBER FROM DWH_SGUVM.UVMP_INF_VEHICLE m where m.vehicle_number= '%s' ) t1 ON t.VEHICLE_ID=t1.id WHERE  T.RECORD_TIME BETWEEN '%s'  AND  '%s' "  %(vehicle_number,starttime,endtime)
#         df = pd.read_sql_query(sql, engine)  # 获取全部正负30分钟之内的记录
#         # print(df)
#         if df.empty is True:
#             df_gasoline.set_value(index, u'数据有效性', "数据无效")
#         else:
#             minindex = df['RECORD_TIME'].apply(lambda x: abs(x - datatime_gasoline)).idxmin()  # 时间相差最小的数据行索引
#             df_gasoline.set_value(index, 'RECORD_TIME', df['RECORD_TIME'][minindex])
#             df_gasoline.set_value(index, 'dif_time', abs(df['RECORD_TIME'][minindex] - datatime_gasoline))
#             df_gasoline.set_value(index, 'dy', df['dy'][minindex])
#             df_gasoline.set_value(index, 'dx', df['dx'][minindex])
#             # print(df['RECORD_TIME'][minindex])
#         strtime2 = time.clock()
#         print("第"+str(index)+"条"+str(round(strtime2-strtime1,2))+"秒")
#     print(df_gasoline)
#     df_gasoline.to_csv("temp.csv")
#
#     return df_gasoline

def func2(path='加油20200504.xlsx', distance=1000, timedif=30):
    timedif *= 60
    data = pd.read_excel(path,encoding='GBK')
    data['flag_legal'] = None
    data['dis'] = None
    datafailed = data[data['dif'] > timedif]
    data = data[data['dif'] <= timedif]
    datafailed['flag_legal'] = 0
    datafailed['category'] = None
    data = data.reset_index(drop=True)
    datafailed = datafailed.reset_index(drop=True)

    count_gasstation = len(data['station'].value_counts())  # 加油站的个数
    model = KMeans(n_clusters=count_gasstation)  # 分为加油站个数个类
    model.fit(data[['dy', 'dx']])  # 开始聚类

    finaldata = pd.concat([data, pd.Series(model.labels_)], axis=1)  # 每个样本对应的类别
    finaldata.columns = list(data.columns) + ['category']  # 重命名表头

    for index, row in finaldata.iterrows():
        cluster = int(row['category'])
        dis = getDistance(row['dy'], row['dx'], model.cluster_centers_[cluster][0], model.cluster_centers_[cluster][1])
        finaldata.set_value(index, 'dis', dis)

        finaldata.set_value(index, 'flag_legal', time_dis(distance,timedif,dis))

    finaldata = pd.concat([finaldata, datafailed])
    finaldata = finaldata.reset_index(drop=True)
    finaldata.to_csv("temp.csv")
    return finaldata

def func3(path="temp.csv"):
    win = tkinter.Tk()
    x = win.winfo_screenwidth()
    y = win.winfo_screenheight()

    pieces = [
        {'min': 0, 'max': 0, 'label': '数据无效', 'color': '#00B2EE'},
        {'min': 1, 'max': 1, 'label': '数据疑似为假', 'color': '#71C671'},
        {'min': 2, 'max': 2, 'label': '数据疑似为真', 'color': '#CD4F39'},
    ]

    data = pd.read_csv(path)  # 读取数据

    geo_sight_coord = data[['dx', 'dy']]  # 构造位置字典数据
    # data_pair = [(str(i), 1) for i in range(len(geo_sight_coord))]  # 构造项目租金数据
    data_pair = []
    for index,row in data.iterrows():
        data_pair.append((str(index),int(row['flag_legal'])))

    g = Geo(
        init_opts=opts.InitOpts(
            page_title="车辆轨迹",
            width=str(x / 1.1) + 'px',
            height=str(y / 1.1) + 'px',
        )
    )  # 地理初始化

    g.add_schema(maptype="江西")  # 限定上海市范围
    for key, value in geo_sight_coord.iterrows():  # 对地理点循环
        g.add_coordinate(str(key), value['dx'], value['dy'])  # 追加点位置

    g.add("", data_pair, symbol_size=4)  # 追加项目名称和租金
    g.set_series_opts(
        label_opts=opts.LabelOpts(is_show=False),
        type='scatter',
    )  # 星散点图scatter

    g.set_global_opts(
        visualmap_opts=opts.VisualMapOpts(
            is_piecewise=True,
            pieces=pieces,
            pos_left='left',
            pos_bottom="100px",
        ),
        title_opts=opts.TitleOpts(title="车辆轨迹")
    )
    g.render(path="车辆加油地图.html")

if __name__ == '__main__':
    # func1(minutes_param=30)
    func2(path='加油20200504.xlsx')
    func3()

