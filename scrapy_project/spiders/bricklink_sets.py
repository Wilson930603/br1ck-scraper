# import needed libraries
import csv
import json
import os
import platform
import re
import time
import secrets
import requests
import scrapy
from scrapy_project.spiders.base_spider import BaseSpider
from requests_oauthlib import OAuth1

set_numbers_file_path = './bricklink_set_numbers_total.txt'
previously_scraped_json_file_path = ''          # add previous scraped json file .json

# set up OAuth1 credentials
# Add many API keys
API_KEYS = [
    {
        'client_key': '3B2E69F3C50D408EA73506A1DFFCC870',
        'client_secret': '8E027040E4CB4C828B7545E314829E77',
        'access_token': '074B44B248D34989A0AE6CD39FF6D93A',
        'access_token_secret': 'FD1912AF9EBC4F4995C7B7AF5CE3805D',
    },
    {
        'client_key': 'BF63123D61284575A478B152F3AE5D8C',
        'client_secret': '6909707A0C084DECB56E88650A23D14A',
        'access_token': '372F36DAA1474ADBBE1844BC8B73B3D0',
        'access_token_secret': '601E1C2347144C1A83CD2D98D581AB47',
    },
]
API_CALLS_DAILY_LIMIT = 5000        # limit per each key

n = 0
previously_scraped = []
# scrape data in chunks due to API rate limits
if previously_scraped_json_file_path:
    # with open(previously_scraped_file_path, 'r', encoding='utf-8-sig') as csvfile:
    #     reader = csv.DictReader(csvfile)
    #     for row in reader:
    #         previously_scraped.append(row)

    with open(previously_scraped_json_file_path, 'r', encoding='utf-8') as jsonfile:
        lines = jsonfile.readlines()
        for line in lines:
            try:
                row = json.loads(line.strip('[],\r\n'))
                previously_scraped.append(row)
            except Exception:
                continue


def get_key():
    global n
    yield API_KEYS[n]
    n += 1
    if n >= len(API_KEYS):
        n = 0


def get_oauth():
    oauth_dict = list(get_key())[0]
    # create OAuth1 object with credentials, timestamp, and nonce
    return OAuth1(oauth_dict['client_key'], oauth_dict['client_secret'],
                  oauth_dict['access_token'], oauth_dict['access_token_secret'],
                  timestamp=str(int(time.time())), nonce=secrets.token_urlsafe())


def cleanHtml(raw_txt):
    clean_txt = raw_txt
    clean_txt = re.sub(r"[\s\t\n\r]+", ' ', clean_txt).strip()
    return clean_txt


class Spider(BaseSpider):
    # spider name; used for calling spider
    name = 'bricklink_sets'
    domain = 'https://www.bricklink.com'
    start_urls = ['https://www.bricklink.com']

    done_sets = list()
    API_requests = 0

    field_names = ['ItemNumber', 'Name', 'Theme', 'SubTheme', 'Categories',
                   'ProductionYears', 'Tags', 'Parts-Quantity', 'Minifigs-Quantity',
                   'PartsCount', 'Weight', 'Dimensions', 'Instructions', 'ImageLinks', 'Quantity',
                   'RebrickableID', 'BrickLinkID', 'BrickOwlID', 'LegoID', 'LegoCsID', 'BricksetID',
                   'BricksetInternalID',
                   'BrickOwlName', 'BricksetName', 'RebrickableName', 'BrickLinkName', 'LegoName', 'BaseName',
                   'UPC_Barcode', 'EAN_Barcode', 'Min_Age', 'RebricableLink', 'BricklinkLink', 'BrickOwlLink',
                   'BricksetLink'
                   ]

    custom_settings = {
        'DOWNLOAD_DELAY': 0.5,
        'CONCURRENT_REQUESTS': 4,
    }

    headers = {
        'Authority': 'bricklink.com',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36',
    }

    if platform.system()=='Linux':
        URL='file:////' + os.getcwd()+'/scrapy.cfg'
    else:
        URL='file:///' + os.getcwd()+'/scrapy.cfg'

    def parse(self, response, **kwargs):
        self.logger.info(f'NUMBER OF PREVIOUSLY SCRAPED SETS = {len(previously_scraped)}')
        for set_item in previously_scraped:
            # Initialize item
            item = dict()
            for k in self.field_names:
                item[k] = ''

            item.update(set_item)
            yield item

            if item['ItemNumber'] not in self.done_sets:
                self.done_sets.append(item['ItemNumber'])

        sets_lst = []
        memo_sets = set()
        with open(set_numbers_file_path, 'r', encoding='utf-8') as ifile:
            for line in ifile.readlines():
                try:
                    j = json.loads(line.strip())
                    if j['set_num'] not in memo_sets:
                        sets_lst.append(j)
                        memo_sets.add(j['set_num'])
                except:
                    pass
        self.logger.info(f'NUMBER OF AVAILABLE SETS TO SCRAPE = {len(sets_lst)}')

        for set_item in sets_lst:
            if set_item['set_num'] not in self.done_sets:
                yield scrapy.Request(url=self.URL, callback=self.parse_set,
                                     meta={'set_item': set_item}, dont_filter=True)

    def parse_set(self, response):
        set_item = response.meta['set_item']
        set_num = set_item['set_num']

        if self.API_requests > API_CALLS_DAILY_LIMIT * len(API_KEYS):
            try:
                self.crawler.engine.stop()
                self.logger.info('STOP: API RATE LIMIT REACHED')
            except:
                pass
            return

        if set_num in self.done_sets:
            return

        api_url = f'https://api.bricklink.com/api/store/v1/items/set/{set_num}'
        try:
            response = requests.get(api_url, auth=get_oauth())
            self.API_requests += 1
            self.logger.info(f'API REQUEST #{self.API_requests} FOR URL {api_url}')
            set_data = json.loads(response.text)['data']
        except Exception as e:
            self.logger.info(f'ERROR REQUEST TO {api_url}, {e}, {response.text}')
            set_data = None

        if set_data:
            self.done_sets.append(set_num)

            # Initialize item
            item = dict()
            for k in self.field_names:
                item[k] = ''

            item['BricklinkLink'] = set_item['set_url']
            item['BrickLinkID'] = set_data.get('no', '')
            item['ItemNumber'] = set_data.get('no', '')
            item['Name'] = set_data.get('name', '')
            item['BrickLinkName'] = set_data.get('name', '')
            item['Categories'] = set_item['category_name']
            item['ProductionYears'] = set_data.get('year_released', '')
            item['ImageLinks'] = set_data.get('image_url', '')
            if item['ImageLinks'] and not item['ImageLinks'].lower().startswith('https:'):
                item['ImageLinks'] = 'https:' + item['ImageLinks']

            item['Weight'] = set_data.get('weight', '')
            item['Dimensions'] = 'x '.join([set_data.get(cord) for cord in ['dim_x', 'dim_y', 'dim_z'] if set_data.get(cord)])

            inventory_url = f'https://api.bricklink.com/api/store/v1/items/set/{set_num}/subsets?break_subsets=true&break_minifigs=true&instruction=true'
            try:
                response = requests.get(inventory_url, auth=get_oauth())
                self.API_requests += 1
                self.logger.info(f'API REQUEST #{self.API_requests} FOR URL {inventory_url}')

                inventory_data = json.loads(response.text)['data']
            except Exception as e:
                self.logger.info(f'ERROR REQUEST TO {inventory_url}, {e}, {response.text}')
                inventory_data = None

            if inventory_data:
                item['part_json'] = inventory_data
                item['PartsCount'] = len(inventory_data)

                item['Parts-Quantity'] = list()
                item['Minifigs-Quantity'] = list()
                item['Other-Quantity'] = list()

                for entry in inventory_data:
                    part = entry['entries'][0]
                    if part['item']['type'] == 'PART' and not part['item']['name'].startswith('Minifigure'):
                        item['Parts-Quantity'].append({'ElementID': part['item'].get('no', ''),
                                                       'BrickLinkID': part['item'].get('no', ''),
                                                       'ColorID': part.get('color_id', ''),
                                                       'Name': part['item']['name'],
                                                       'Qty': part.get('quantity', ''),
                                                       'Alternates': [itm['item']['no'] for itm in entry['entries'][1:]] if len(entry['entries']) > 1 else ''
                                                       }
                                                      )
                    elif part['item']['type'] == 'PART' and part['item']['name'].startswith('Minifigure'):
                        item['Minifigs-Quantity'].append({
                            'ElementID': part['item'].get('no', ''),
                            'BrickLinkID': part['item'].get('no', ''),
                            'ColorID': part.get('color_id', ''),
                            'Name': part['item']['name'],
                            'Qty': part.get('quantity', ''),
                            'Alternates': [itm['item']['no'] for itm in entry['entries'][1:]] if len(entry['entries']) > 1 else ''
                        })
                    else:
                        item['Other-Quantity'].append(part)

            yield item
