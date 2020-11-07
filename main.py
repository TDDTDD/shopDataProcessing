import pandas as pd
import numpy as np
import pickle as pk

'''
南京：
1经度 = 193220.3389830508m
1维度 = 111864.406779661m
# child[i][j][k]:第i层第j个矩阵的第k个小矩阵
# fa[i][j]:第i层第j个矩阵的父矩阵
# num[i]:第i层的计数
# areaShop[i][j]:最后一层第i个矩阵的第j个店铺
# shopArea[i]:第i个店铺属于哪个矩阵
# nameShop['肯德基']:店铺为肯德基的店铺id
# shopName[23132]:店铺id为23132的店铺
'''
child = np.full((15, 5000000, 5), -1, dtype=int)
fa = np.full((25, 5000000), -1, dtype=int)
num = np.zeros(15, dtype=int)
areaShop = [[] for _ in range(5000000)]
shopArea = {}
nameShop = {}
shopName = {}
'''
# 每个小矩阵在大矩阵中的位置,矩阵构造
# 1 2 ... 2048
# 2
# .
# .
# 2048
# 最后矩阵经纬度差
# Area[1] = [...] 每个小矩阵id对应的经纬度范围，便于验证正确性
'''
Square = {}
longitude_difference = 0
latitude_difference = 0
Area = {}


# 给出矩阵对角线两点的经纬度和经纬度差 计算出迭代次数 和 最后的经纬度之差
def init(x_longitude_boundary, x_latitude_boundary,
         y_longitude_boundary, y_latitude_boundary, difference_boundary):
    global longitude_difference, latitude_difference
    step = 0
    longitude_difference = y_longitude_boundary - x_longitude_boundary
    latitude_difference = x_latitude_boundary - y_latitude_boundary
    while latitude_difference > difference_boundary and longitude_difference > difference_boundary:
        longitude_difference /= 2.0
        latitude_difference /= 2.0
        step += 1
    return step


# 划分区域
def distribute_data(x1, x2, y1, y2, now_step, nfa, x_latitude, x_longitude):
    fa[now_step][num[now_step]] = nfa
    if x1 == x2 and y1 == y2:
        Square[(x1, y1)] = num[now_step]
        Area[num[now_step]] = [x_latitude - x1 * latitude_difference, x_latitude - (x1 - 1) * latitude_difference,
                               x_longitude + (y1 - 1) * longitude_difference, x_longitude + y1 * longitude_difference]

        if num[now_step] % 100000 == 0:
            print(str(now_step) + " build num :" + str(num[now_step]))
    else:
        child[now_step][num[now_step]][0] = distribute_data(x1, int((x1 + x2) / 2), y1, int((y1 + y2) / 2),
                                                            now_step + 1, num[now_step], x_latitude, x_longitude)
        child[now_step][num[now_step]][1] = distribute_data(x1, int((x1 + x2) / 2), int((y1 + y2) / 2) + 1, y2,
                                                            now_step + 1, num[now_step], x_latitude, x_longitude)
        child[now_step][num[now_step]][2] = distribute_data(int((x1 + x2) / 2) + 1, x2, y1, int((y1 + y2) / 2),
                                                            now_step + 1, num[now_step], x_latitude, x_longitude)
        child[now_step][num[now_step]][3] = distribute_data(int((x1 + x2) / 2) + 1, x2, int((y1 + y2) / 2) + 1, y2,
                                                            now_step + 1, num[now_step], x_latitude, x_longitude)
    num[now_step] += 1
    return num[now_step] - 1


# 将店铺划分到指定区域，并将店铺连接到对应的类型
def distribute_shop(x_longitude_boundary, x_latitude_boundary, y_longitude_boundary, y_latitude_boundary,
                    now_longitude_difference, now_latitude_difference, now_data):
    dis_num = 0
    check_num = 0
    for index, row in now_data.iterrows():
        longitude = row["longitude"]
        latitude = row["latitude"]
        # 判断地点的合法性 并计算出当前地点属于哪个矩阵块
        if longitude < x_longitude_boundary or longitude > y_longitude_boundary or latitude > x_latitude_boundary or latitude < y_latitude_boundary:
            continue
        else:
            x_difference = x_latitude_boundary - latitude
            y_difference = longitude - x_longitude_boundary
            # 余数大于等于0就加1 ，避免出现（0，0）
            dx = x_difference / now_latitude_difference - x_difference // now_latitude_difference
            dy = y_difference / now_longitude_difference - y_difference // now_longitude_difference
            x = x_difference // now_latitude_difference + (1 if dx >= 0.0 else 0)
            y = y_difference // now_longitude_difference + (1 if dy >= 0.0 else 0)
            area_id = Square[(x, y)]

            # 检查连接的矩阵范围是否正确
            if Area[area_id][0] <= latitude <= Area[area_id][1] and Area[area_id][2] <= longitude <= Area[area_id][3]:
                check_num += 1
            else:
                print("error :" + str(x) + ", " + str(y))
                print(area_id, longitude, latitude, Area[(x, y)])

            if check_num % 100000 == 0:
                print("check num :" + str(check_num))
            # 检查结束

            areaShop[area_id].append(row["shop_id"])
            shopArea[row["shop_id"]] = area_id
            # 将店铺id与店名相连
            if row["name"] not in nameShop:
                nameShop[row["name"]] = [row["shop_id"]]
            else:
                nameShop[row["name"]].append(row["shop_id"])
            shopName[row["shop_id"]] = row["name"]
            dis_num += 1
        if dis_num % 100000 == 0:
            print("distribute success num : " + str(dis_num))
    print("distribute over : " + str(dis_num))


def data_process(x_longitude, x_latitude, y_longitude, y_latitude, difference_boundary, url, store_url):
    my_data = pd.read_csv(url, low_memory=False)
    step = init(x_longitude, x_latitude, y_longitude, y_latitude, difference_boundary)
    print(step, longitude_difference, latitude_difference)
    # 11 0.0005900000000000002 0.0008850000000000004
    distribute_data(1, 2 ** step, 1, 2 ** step, 0, 0, x_latitude, x_longitude)
    print("build success! num: " + str(num[step]))
    distribute_shop(x_longitude, x_latitude, y_longitude, y_latitude, longitude_difference, latitude_difference,
                    my_data)
    print("distribute success!")
    # 存储
    file = open(store_url, 'wb')
    pk.dump(child, file)
    pk.dump(fa, file)
    pk.dump(areaShop, file)
    pk.dump(shopArea, file)
    pk.dump(nameShop, file)
    pk.dump(shopName, file)
    pk.dump(step, file)
    pk.dump(longitude_difference, file)
    pk.dump(latitude_difference, file)
    pk.dump(Area, file)
    file.close()
    print("store over!")


# 传入要划分区域的左上角坐标，右下角坐标，经纬度差阈值，数据所在地址，结果存储地址
data_process(118.256892, 32.405757, 119.465212, 30.593277, 0.000885, 'data/shopData.csv', 'data/processData.pkl')
'''
# test:
print(shopName[538008])
print(nameShop['肯德基'])
'''
