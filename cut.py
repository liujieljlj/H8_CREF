from netCDF4 import Dataset
import h5py
import os
import sys
from natsort import natsorted
import numpy as np
import time


ori_path = '/home/wyq/wyqDisk/himawari8/h8_CREF/2018'
NWP_path = '/home/wyq/wyqDisk/himawari8/h8_CREF/2018_splitted'
wanted_vars = ['albedo_01', 'albedo_02', 'albedo_03', 'albedo_04', 'albedo_05', 'albedo_06', 
            'tbb_07', 'tbb_08', 'tbb_09', 'tbb_10', 'tbb_11', 'tbb_12', 'tbb_13', 'tbb_14', 'tbb_15', 'tbb_16']

# wanted_vars = ['albedo_03', 'tbb_07', 'tbb_13']

# test_read
# file = '/media/data2/wyp/himiwari8/h8_00-50_NWP/03/NC_H08_20210301_0000_R21_FLDK.06001_06001.h5'
# with h5py.File(file, 'r') as f:
#     data = f['data'][:]
# print(data.shape)


while True:
    month_list = natsorted(os.listdir(ori_path))

    # 记录每个月已经下载的文件
    # key: month, value: list of downloaded files
    downed_files = {}
    for month in month_list:
        temp_month_path = os.path.join(ori_path, month)
        files = natsorted(os.listdir(temp_month_path))
        
        # 初始化所有月份的列表
        if month not in downed_files.keys():
            downed_files[month] = []
        
        for file in files:
            if file.endswith('.nc'):
                downed_files[month].append(file)
        
        print(month, len(downed_files[month]))

    # 从已下载的文件中提取出NWP(0~60N, 100~160E)区域的数据
    for month in month_list:
        print("start extracting data from", month, "...")
        temp_ori_path = os.path.join(ori_path, month)
        temp_NWP_path = os.path.join(NWP_path, month)
        
        if not os.path.exists(temp_NWP_path):
            os.makedirs(temp_NWP_path)
        
        for file in downed_files[month]:
            try:
                h8_data = Dataset(os.path.join(temp_ori_path, file), 'r')
            except:
                print("error in reading ", file)
                continue
            # print(h8_data.variables['albedo_01'].dimensions)
            
            # total area is 60°S-60°N, 80°E-200°E
            # extract the area of 0°-60°N, 100°E-160°E
            lat = h8_data.variables['latitude'][:].data
            lon = h8_data.variables['longitude'][:].data
            
            lat_index = np.where((lat >= 12.2) & (lat <= 54.2))[0]
            lon_index = np.where((lon >= 80) & (lon <= 135))[0]
            
            temp_data = []
            for temp_var in h8_data.variables.keys():
                if temp_var in wanted_vars:
                    temp_data.append(h8_data.variables[temp_var][:].data[lat_index[0]:lat_index[-1]+1, lon_index[0]:lon_index[-1]+1])
            h8_data.close()
            
            temp_data = np.array(temp_data, dtype=np.float32)
            
            # save data use h5py
            with h5py.File(os.path.join(temp_NWP_path, file[:-3] + '.h5'), 'w') as f:
                f.create_dataset('data', data=temp_data, dtype=np.float32, compression='gzip', compression_opts=4)
                f.close()
                print(file[:-3] + '.h5', 'saved!')
            
            # save data use nc
            # with Dataset(os.path.join(temp_NWP_path, file), 'w', format='NETCDF4') as fw:
            #     fw.createDimension('latitude', 3001)
            #     fw.createDimension('longitude', 3001)
                
            #     for var in wanted_vars:
            #         fw.createVariable(var, np.float32, ('latitude', 'longitude'))
            #         value = temp_data[wanted_vars.index(var)]
            #         fw.variables[var][:] = value

            # remove the original nc file
            os.remove(os.path.join(temp_ori_path, file))
            print(file, 'removed!')
            
    # 打印当前时间
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    print("sleep 3 hour...")
    time.sleep(3600*3)