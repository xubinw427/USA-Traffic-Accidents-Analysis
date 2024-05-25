filename = "USZipCodesMapping"

# 读取文件并创建一个字典，其中邮编作为键，经纬度作为值
zipcode_dict = {}
with open(filename, 'r') as file:
    for line in file:
        parts = line.strip().split(',')
        if len(parts) == 3:
            zipcode, lat, lng = parts
            zipcode_dict[zipcode] = (lat, lng)

def get_lat_lng_by_zipcode(zipcode):
    return zipcode_dict.get(zipcode, "postcode cannot be found")

print(get_lat_lng_by_zipcode("00601")) 
print(get_lat_lng_by_zipcode("12345")) 
