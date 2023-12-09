# -*- coding: utf-8 -*-
'''
date: 2018/12/20
deal_h8
'''

import datetime
from natsort import natsorted
import netCDF4 as nc
import h5py 
from netCDF4 import Dataset
import logging
import os
import re
import numpy as np
from multiprocessing import Pool, cpu_count

# 标准区域
# llcrnrlon=116.1
# llcrnrlat=37.6
# urcrnrlon=119.6
# urcrnrlat=40.3

# area A
# llcrnrlon=111.
# llcrnrlat=35.3
# urcrnrlon=114.5
# urcrnrlat=38.

# area B
llcrnrlon=111.
llcrnrlat=38.0
urcrnrlon=114.5
urcrnrlat=40.7

# 渤海
# llcrnrlon=117.5
# llcrnrlat=37
# urcrnrlon=124.5
# urcrnrlat=41

logger_format = '%(asctime)s %(levelname)s %(filename)s %(funcName)s %(lineno)s %(message)s'

# 正则匹配
s = r'NC_H08_\w+_\w+R21_FLDK.06001_06001.nc'

def deal_data(file_path, save_path):
    logging.basicConfig(filename=os.path.join(save_path,'deal_h8.log'),
                            format=logger_format, level=logging.DEBUG)

    NCFile_name = file_path.split('\\')[-1]
    year_mon_day_str = NCFile_name.split('_')[2]
    hour_sec_str = NCFile_name.split('_')[3]
    date_str = year_mon_day_str + hour_sec_str

    nc_datetime_UTC = datetime.datetime.strptime(date_str, '%Y%m%d%H%M')
    # nc_datetime_UTC8 = nc_datetime_UTC + datetime.timedelta(hours=8)
    # nc_datetime_str = nc_datetime_UTC8.strftime('%Y-%m-%d-%H-%M-%S' + '.nc')
    nc_datetime_str = nc_datetime_UTC.strftime('%Y-%m-%d-%H-%M-%S' + '.hdf5')
    save_path = os.path.join(save_path, nc_datetime_str)
    # 将‘\’转换为‘//’
    save_path = save_path.replace('\\', '//')
    file_path = file_path.replace('\\', '//')

    # 换为’/‘
    save_path = save_path.replace('//', '/')
    file_path = file_path.replace('//', '/')

    if os.path.exists(save_path):
        print('{} 该文件已经存在'.format(save_path))
        return
        
    print('write to', save_path)

    count = 0 #计数
    count += 1
    logging.info('dealing %s', nc_datetime_str)
    try:
        write2file(file_path, save_path)
    except Exception as e:
        logging.info('error: %s', e)
        print(e)
    logging.info('\n count of .nc: %d ....................', count)

def write2file(input_path, output_path):

    fr = nc.Dataset(input_path)
    # 'albedo_01', 'albedo_02', 'albedo_03', 'sd_albedo_03', 'albedo_04', 'albedo_05', 'albedo_06', 'tbb_07', 'tbb_08', 'tbb_09', 'tbb_10', 'tbb_11', 'tbb_12', 'tbb_13', 'tbb_14', 'tbb_15', 'tbb_16'
    albedo = ['albedo_01', 'albedo_02', 'albedo_03', 'sd_albedo_03', 'albedo_04', 'albedo_05', 'albedo_06']
    tbb = ['tbb_07', 'tbb_08', 'tbb_09', 'tbb_10', 'tbb_11', 'tbb_12', 'tbb_13', 'tbb_14', 'tbb_15', 'tbb_16']
    albedo_data = []
    tbb_data = []
    for i in albedo:
        albedo_data.append(fr.variables[i][0:2000, 500:2500])
    for i in tbb:
        tbb_data.append(fr.variables[i][0:2000, 500:2500])
    # 文件压缩
    albedo_data = np.array(albedo_data)
    tbb_data = np.array(tbb_data)

    # 存为hdf5文件
    with h5py.File(output_path, 'w') as f:
        f.create_dataset('albedo', data=albedo_data, compression='gzip', compression_opts=9)
        f.create_dataset('tbb', data=tbb_data, compression='gzip', compression_opts=9)
    fr.close()
    # os.remove(input_path)

def nc_data_read():
    path = r'G:\himawari8\2020\07\01\NC_H08_20200701_0000_R21_FLDK.06001_06001.nc'
    # 路径转换
    path = path.replace('\\', '/')
    # 读取nc文件
    nc_data = Dataset(path, 'r')
    # 获取nc文件中的所有变量
    nc_vars = nc_data.variables.keys()
    print(nc_vars)
    # -------------------获取nc文件的经纬度信息-------------------
    # latitude = nc_data.variables['latitude'][:]
    # # 获得20 - 60范围内的数据索引
    # latitude_index = np.where((latitude >= 20) & (latitude <= 60))
    # print(min(latitude_index[0]), max(latitude_index[0]))
    # longitude = nc_data.variables['longitude'][:]
    # # 获得90 - 130范围内的数据索引
    # longitude_index = np.where((longitude >= 90) & (longitude <= 130))
    # print(min(longitude_index[0]), max(longitude_index[0]))
    # ------------------------------------------------------------

    albedo0 = nc_data.variables['albedo_01'][:]
    tbb0 = nc_data.variables['tbb_07']
    print(albedo0)

if __name__ == '__main__':
    nc_path = r'G://himawari8//2020//10'
    output_path = r'G://himawari8//2020_spilit//10'
    os.makedirs(output_path, exist_ok=True)

    # 首先遍历文件夹，得到所有的nc文件，nc_path下面还有文件夹
    nc_files = []
    for root, dirs, files in os.walk(nc_path):
        for file in files:
            if file.endswith('.nc'):
                nc_files.append(os.path.join(root, file))

    # 使用多进程处理文件
    pool = Pool(4)
    results = []
    for file_path in nc_files:
        pool.apply_async(deal_data, args=(os.path.join(nc_path, file_path), output_path))
    pool.close()
    pool.join()
    # nc_data_read()