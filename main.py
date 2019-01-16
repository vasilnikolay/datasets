from bs4 import BeautifulSoup
import requests
import psycopg2
import json
from time import sleep

website = "http://dom.mingkh.ru"


def getUrlList():
    urls = []
    api = "http://dom.mingkh.ru/api/houses"
    post_content = {"current": 1,
                    "rowCount": 1,
                    "searchPhrase": "",
                    "region_url": "moskva",
                    "city_url": "moskva"}

    html = requests.post(api, data=post_content)
    encoded = html.content.decode(encoding="utf-8")
    content = json.loads(encoded)
    ''' example of content
    {'current': 2, 'rowCount': 1, 
    'rows': [{'rownumber': '2', 
                'address': 'п. Рязановское, п. Остафьево, д. 24, Москва', 
                'square': '206.00', 
                'year': '1957', 
                'floors': '1', 
                'url': '/moskva/moskva/739001', 
                'managestartdate': '01.05.2017'}], 
    'total': 30972}
    '''
    max_rows = content["total"]
    post_content["rowCount"] = max_rows
    html = requests.post(api, data=post_content)
    encoded = html.content.decode(encoding="utf-8")
    content = json.loads(encoded)
    for elem in content["rows"]:
        urls.append([website + elem["url"], elem["address"], elem["square"]])
    return urls


def executeQuery(query):
    conn = psycopg2.connect("dbname='msk_estate' user='postgres' host='localhost' password='admin'")
    cur = conn.cursor()
    print(query)
    cur.execute(query)
    conn.commit()
    conn.close()


def insertData(data):
    query = '''
        INSERT INTO public.buildings (    bid,
                                        address,
                                        created_year,
                                        max_floors,
	                                    houses_qty,
	                                    floor_type,
	                                    wall_type,
	                                    playground,
	                                    cadastr,
	                                    is_emergency,
	                                    garbage_chute,
                                        living_area,
                                        latitude,
                                        longitude, 
	                                    link_to_website)
        VALUES ({}, '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}');
    '''
    lst = [str(i) for i in data]
    q = query.format(lst[0], lst[1], lst[2], lst[3], lst[4], lst[5], lst[6], lst[7], lst[8], lst[9],
                     lst[10], lst[11], lst[12], lst[13], lst[14])
    print(q)
    executeQuery(q)




def getHouseDescription(link):
    html = requests.get(link)
    enc = html.content.decode(encoding="utf-8")
    bs = BeautifulSoup(enc, 'html.parser')
    div = bs.find('div')
    info_values = [i.get_text().strip() for i in div.find_all("dd")]
    info_keys = [i.get_text().strip() for i in div.find_all("dt")]
    desc = dict(zip(info_keys, info_values))
    year = desc.get("Год постройки", "Unknown")
    floors = desc.get("Количество этажей", "Unknown")
    houses_number = desc.get("Жилых помещений", "Unknown")
    floor_type = desc.get("Тип перекрытий", "Unknown")
    wall_type = desc.get("Материал несущих стен", "Unknown")
    playground = desc.get("Детская площадка", "Unknown")
    cadastr = desc.get("Кадастровый номер", "Unknown")
    emergency = desc.get("Дом признан аварийным", "Unknown")
    garbage = desc.get("Тип мусоропровода", "Unknown")
    try:
        x = bs.find(id="mapcenterlat")
        lat = x["value"]
    except: lat = 'Unknown'
    try:
        x = bs.find(id="mapcenterlng")
        long = x["value"]
    except: long = "Unknown"
    description = [year, floors, houses_number, floor_type,
                    wall_type, playground, cadastr, emergency, garbage, lat, long]
    print(desc)
    print(description)
    return description



def main():
    executeQuery("TRUNCATE TABLE public.buildings;")
    cnt = 0
    wrong_data = []
    for item in getUrlList():
        cnt += 1
        try:
            raw = getHouseDescription(item[0])
            data = [cnt, item[1], raw[0], raw[1], raw[2], raw[3], raw[4], raw[5], raw[6], raw[7],
                     raw[8], item[2], raw[9], raw[10], item[0]]
            insertData(data)
        except Exception as ex:
            wrong_data.append(item)
            print(str(ex))
        sleep(0.7)
    print(wrong_data)
    print(len(wrong_data))


main()
