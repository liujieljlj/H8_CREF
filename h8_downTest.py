# 导入包
from downloadH8 import downloadH8
from datetime import datetime
import calendar
# from natsort import natsorted
import os
import multiprocessing as mul

downMon = '202007'

# 自定义下载分辨率5KM的每日整点L1级数据的函数
def down_h8(start, end):
    """
    :start 开始时间
    :end   终止时间
    """
    # 读取对应月份的文件名列表
    cref_file_list = []
    with open('cref_fileNameList/{}.txt'.format(downMon), 'r') as f:
        for line in f.readlines():
            # 201807122030 以30结尾的不要
            if line.strip('\n')[-2:] == '30':
                continue
            cref_file_list.append(line.strip('\n'))
        f.close()

    # 将输入的开始时间转换成datetime格式，后面的"%Y%m%d%H%M"格式化方式按照自己的需要进行修改
    nowdate = datetime.strptime(start, "%Y%m%d%H%M")
    # 将输入的终止时间转换为datetime格式
    enddate = datetime.strptime(end, '%Y%m%d%H%M')
    # 初始化下载类，将你的用户名和密码分别赋值给username和password两个参数
    down = downloadH8(username='wenyouqi_stu.ouc.edu.cn', password='SP+wari8')
    # 下载自己需要的nc数据
    # 获取下载列表
    filelist = down.search_ahi8_l1_netcdf(nowdate, enddate)
    # 新建一个列表用来保存需要下载的文件链接
    down_list = []
    # 循环获取到的文件列表按照自己需要进行筛选
    for file in filelist:
        # 按照需要进行匹配（这里是需要下载5KM分辨率的每日整时数据）
        name_list = file.split("_")
        # 要下载半点的数据，与cref_file_list中的文件名进行匹配
        if (name_list[6][1] == '6' and name_list[1] == 'H08') and (name_list[2] + name_list[3] in cref_file_list):
            down_list.append(file)

    # 如果目标文件夹下存在则跳过
    newDownList = []
    cutted_filePath = '/media/data9/wy/h8/{}_splitted/{}'.format(downMon[:4], downMon[4:])
    for file in down_list:
        if not os.path.exists(os.path.join(cutted_filePath, file.split('/')[-1].split('.')[0] + '.' + file.split('/')[-1].split('.')[1] + '.h5')):
            newDownList.append(file)

    # 进行数据下载
    dest_path = '/media/data9/wy/h8/{}/{}'.format(downMon[:4], downMon[4:])
    if not os.path.exists(dest_path):
        os.makedirs(dest_path)
    #使用多线程下载,将newDownList分成4份, mulitiprocessing
    # for i in range(4):
    #     pool = mul.Pool(processes=4)
    #     pool.apply_async(down.download, (dest_path, newDownList[i*len(newDownList)//4:(i+1)*len(newDownList)//4]))
    #     pool.close()
    #     pool.join()
    
    down.download(dest_path, newDownList)

# 在 dataChecker.py 中获取未下载完全的文件名列表
# def get_incomplete_files():
#     # TODO: 实现获取未下载完全的文件名列表的代码
#     PATH=r'/media/data2/wyq/H8_downLoad/20160815~20160831'
#     incomplete_files=[]
#     for file in natsorted(os.listdir(PATH)):
#         if file.endswith('.download'):
#             incomplete_files.append(file.split('.')[0].split('_')[2] + file.split('.')[0].split('_')[3])
#     print('未下载完成的文件为:{}'.format(incomplete_files))
#     return incomplete_files

# # 在 h8_downTest.py 中重新下载未下载完全的文件
# def redownload_incomplete_files():
#     # 获取未下载完全的文件名列表
#     incomplete_files = get_incomplete_files()

#     # 初始化下载类，将你的用户名和密码分别赋值给username和password两个参数
#     down = downloadH8(username='wenyouqi_stu.ouc.edu.cn', password='SP+wari8')
#     down_list = []
#     # 循环获取到的文件列表按照自己需要进行筛选
#     for file in incomplete_files:
#         nowdate = datetime.strptime(file, "%Y%m%d%H%M")
#         # 将输入的终止时间转换为datetime格式
#         enddate = datetime.strptime(file, '%Y%m%d%H%M')
#         # 筛选
#         for file in down.search_ahi8_l1_netcdf(nowdate, enddate):
#             name_list = file.split("_")
#             if (name_list[6][1] == '6' and name_list[1] == 'H08'):
#                 down_list.append(file)

#     # 进行数据下载
#     assert len(down_list) == len(incomplete_files)
#     down.download(r'/media/data2/wyq/H8_downLoad/test', down_list)


if __name__ == '__main__':

    # 根据downMon获得当前月的起始和结束时间(到分钟)
    year = int(downMon[:4])
    month = int(downMon[4:])
    _, end_day = calendar.monthrange(year, month)
    start = downMon + '010000'
    end = downMon + str(end_day) + '0350'
    # 调用函数
    down_h8(start, end) # 调用函数就可以下载
    # redownload_incomplete_files() # 调用函数就可以重新下载未下载完全的文件
