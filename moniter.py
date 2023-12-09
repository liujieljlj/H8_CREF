import os
import sys
import time


# 监控 python -u h8_downTest.py 程序是否在运行
# 最多重启5次
def moniter():
    restart_times = 0
    
    while restart_times < 5:
        # 获取当前运行的所有进程
        cmd = 'ps -ef | grep "python -u h8_downTest.py" | grep -v grep'
        # 获取当前运行的所有进程
        r = os.popen(cmd)
        # 读取所有行
        text = r.readlines()
        # 关闭
        r.close()
        # 如果没有运行，启动程序
        if len(text) == 0 or (len(text) > 0 and text[0].find('wyq')) == -1:
            print('h8_downTest.py is not running, start it.')
            os.system('nohup python -u h8_downTest.py > 201812_cref_h8.txt 2>&1 &')
            restart_times += 1
            time.sleep(60)
        else:
            print('h8_downTest.py is running, do nothing.')
        
        time.sleep(60*10)
    

if __name__ == '__main__':
    moniter()