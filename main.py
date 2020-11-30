import pandas as pd
import numpy as np
import pickle as pk

'''
Nanjing：
one longitude = 92819.61471090886m
one latitude = 111864.406779661m
child[i][j][k]:The kth child matrix of the i-th level j-th matrix
fa[i][j]:The father matrix of the i-th level j-th matrix
num[i]:The number of i-th level
areaShop[i][j]:The last level, the jth store of the i-th matrix
shopArea[i]:Which matrix does the i-th store belong to?
nameShop['肯德基']:Store for KFC store id
shopName[23132]:Stores with store id 23132
'''
child = np.full((15, 5000000, 5), -1, dtype=int)
fa = np.full((25, 5000000), -1, dtype=int)
num = np.zeros(15, dtype=int)
areaShop = [[] for _ in range(5000000)]
shopArea = {}
nameShop = {}
shopName = {}
'''
# The position of each small matrix in the larger matrix, matrix construction.
# Row and Column Numbers
# 1 2 ... 2048
# 2
# .
# .
# 2048
# Area[1] = [...] The latitude and longitude range for each sub-matrix id for easy verification of correctness.
'''
Square = {}
longitude_difference = 0
latitude_difference = 0
Area = {}


# Given the longitude and latitude of two points on the diagonal of the matrix and the difference between the last longitude and latitude,
# calculate the number of iterations and the difference between the last longitude and latitude.
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


# Subdivision of regions
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


# Assign shops to specific zones and connect shops to the corresponding types.
def distribute_shop(x_longitude_boundary, x_latitude_boundary, y_longitude_boundary, y_latitude_boundary,
                    now_longitude_difference, now_latitude_difference, now_data):
    dis_num = 0
    check_num = 0
    for index, row in now_data.iterrows():
        longitude = row["longitude"]
        latitude = row["latitude"]
        if longitude < x_longitude_boundary or longitude > y_longitude_boundary or latitude > x_latitude_boundary or latitude < y_latitude_boundary:
            continue
        else:
            x_difference = x_latitude_boundary - latitude
            y_difference = longitude - x_longitude_boundary
            dx = x_difference / now_latitude_difference - x_difference // now_latitude_difference
            dy = y_difference / now_longitude_difference - y_difference // now_longitude_difference
            x = x_difference // now_latitude_difference + (1 if dx >= 0.0 else 0)
            y = y_difference // now_longitude_difference + (1 if dy >= 0.0 else 0)
            area_id = Square[(x, y)]

            # Check that the connected matrix range is correct
            if Area[area_id][0] <= latitude <= Area[area_id][1] and Area[area_id][2] <= longitude <= Area[area_id][3]:
                check_num += 1
            else:
                print("error :" + str(x) + ", " + str(y))
                print(area_id, longitude, latitude, Area[(x, y)])

            if check_num % 100000 == 0:
                print("check num :" + str(check_num))
            # 

            areaShop[area_id].append(row["shop_id"])
            shopArea[row["shop_id"]] = area_id
            # Linking store id to store name
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
    # 10 0.0010773583984375057 0.0008939394531249992
    distribute_data(1, 2 ** step, 1, 2 ** step, 0, 0, x_latitude, x_longitude)
    print("build success! num: " + str(num[step]))
    distribute_shop(x_longitude, x_latitude, y_longitude, y_latitude, longitude_difference, latitude_difference,
                    my_data)
    print("distribute success!")
    # store
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


# Passes the upper left corner coordinates, lower right corner coordinates, latitude/longitude difference threshold, address where the data is located,
# and the address where the results are stored for the area to be divided.
data_process(118.606465, 32.159281, 119.709680, 31.2438870, 0.001, 'data/shopData.csv', 'data/processData.pkl')
'''
# test:
print(shopName[538008])
print(nameShop['肯德基'])
'''
