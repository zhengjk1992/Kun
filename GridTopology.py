# -*- coding: utf-8 -*-
# @Time    : 2020/6/24 23:37
# @Author  : Zheng Jinkun
# @FileName: GridTopology.py
# @Software: PyCharm
# @Github  ：https://github.com/zhengjk1992

import os
import pandas as pd
import numpy as np
from sklearn.cluster import KMeans

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


class GridTopology():

	def __init__(self, path_empcurve, path_relation):
		# e_mp_curve为e_mp_curve表（全部电表每一刻的示数）的全部数据，类型为Dataframe
		self.e_mp_curve = self.get_e_mp_curve(path_empcurve)
		# completedata_id_datatime为有完整96点数据的集合，set(tuple),{(ID1, DATA_TIME1), ..., (IDn, DATA_TIMEn)}
		# DATA_TIME1为datetime.date(2020, 4, 2)
		self.completedata_id_datatime = self.get_completedata_id_datatime()
		# single,triple分别存储单相表、三相表ID集合，类型为set和set
		self.single, self.triple = self.get_electricity_meter_type()
		# raw_96_data为所有表的的示数原始数据，每个表、每一天生成一条记录，一共96维，类型为Dataframe
		self.raw_96_data = self.get_raw_96_data()
		# relation为台区ID和电表ID直接映射关系，DataFrameGroupBy,包含台区ID、表计ID、表类型三个属性
		self.relation = self.get_relation(path_relation)

	def get_e_mp_curve(self, path_empcurve):
		df = pd.read_csv(path_empcurve, encoding='GBK')[['ID', 'DATA_TIME', 'UA', 'UB', 'UC']]
		df['DATA_TIME'] = pd.to_datetime(df['DATA_TIME'])
		return df

	def get_relation(self, path_relation):
		df = pd.read_csv(path_relation, encoding='GBK')[['TG_ID', 'ID']]
		df_single = pd.DataFrame(self.single, columns=['ID'])
		df_single['TYPE'] = 'SINGLE'
		df_triple = pd.DataFrame(self.triple, columns=['ID'])
		df_triple['TYPE'] = 'TRIPLE'
		df_merge = pd.concat([df_single, df_triple], axis=0)
		df = df.merge(df_merge, left_on="ID", right_on="ID").groupby(by=['TG_ID'])
		return df

	def get_completedata_id_datatime(self):
		df = self.e_mp_curve.groupby(by=['ID', pd.Grouper(key='DATA_TIME', freq='D')]).count()
		df = df.reset_index()
		df = df[df['UA'] == 96]
		df = df.reset_index(drop=True).drop(labels=['UA', 'UB', 'UC'], axis=1)
		# print(df)
		# print(df.dtypes)
		return {(row[0], row[1].date()) for _, row in df.iterrows()}

	def get_electricity_meter_type(self):
		# -------集合single用于存储所有单相表的ID，集合triple用于存储所有三相表的ID
		df = self.e_mp_curve.groupby(by=['ID', pd.Grouper(key='DATA_TIME', freq='D')]).count()
		df = df.reset_index()
		df = df[df['UA'] == 96]
		single = set()
		triple = set()
		for index, row in df.iterrows():
			if row['UB'] == 0 and row['UC'] == 0:
				single.add(row['ID'])
			else:
				triple.add(row['ID'])
		return single, triple

	def get_raw_96_data(self):
		df = self.e_mp_curve.groupby(by=['ID', pd.Grouper(key='DATA_TIME', freq='D')])
		list_single = []
		list_triple = []
		for df_section in df:
			# print(df_section[0])  # (182430492, Timestamp('2020-04-01 00:00:00', freq='D'))
			# print(type(df_section[0]))
			ID = df_section[0][0]  # 电表ID号，例如182430492
			DATE = df_section[0][1].date()  # 日期，datetime.date(2020, 4, 1)

			# print(df_section[1])  # <class 'pandas.core.frame.DataFrame'>
			# print(type(df_section[1]))
			if (ID, DATE) in self.completedata_id_datatime:  # 如果数据完整则执行程序
				if ID in self.single:  # 如果属于单相表，只取UA数值
					listforsingle = [ID, DATE, 'SINGLE']
					for index, row in df_section[1].iterrows():
						listforsingle.append(row['UA'])
					# print(listforsingle)
					list_single.append(listforsingle)
				else:  # 如果属于三项表，取UA、UA、UC的数值
					listfortriple = list(df_section[0]) + ['TRIPLE']
					for index, row in df_section[1].iterrows():
						listfortriple.append((row['UA'], row['UB'], row['UC']))
					# print(listfortriple)
					list_triple.append(listfortriple)
			else:
				# print(f"数据不完整\t{ID}\t{DATE}")
				continue
		columns = ['ID', 'DATA_TIME', 'TYPE'] + ['U' + str(i) for i in range(1, 97)]
		df_single = pd.DataFrame(data=list_single, columns=columns)
		df_triple = pd.DataFrame(data=list_triple, columns=columns)

		# df_single.to_csv('df_single.csv')
		# df_triple.to_csv('df_triple.csv')
		return df_single, df_triple


if __name__ == '__main__':
	path_empcurve = 'E_MP_CURVE.csv'
	path_relation = '用户台区计量点关系.csv'

	c = GridTopology(path_empcurve, path_relation)
