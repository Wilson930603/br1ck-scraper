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

part_numbers_file_path = './bricklink_part_numbers.txt'
previously_scraped_file_path = './output/bricklink_2023-08-02_10.18.50.csv'

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
if previously_scraped_file_path:
    with open(previously_scraped_file_path, 'r', encoding='utf-8-sig') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            previously_scraped.append(row)


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
    name = 'bricklink'
    domain = 'https://www.bricklink.com'
    start_urls = ['https://www.bricklink.com']

    done_parts = list()
    API_requests = 0

    field_names = ['ElementID', 'DesignID', 'Name', 'Description', 'LDrawName', 'LegoName', 'BrickOwlName',
                   'BricksetName', 'RebrickableName', 'BrickLinkName', 'ColorID', 'CategoryID', 'SubCategoryID',
                   'Categories', 'Tags', 'ProductionYears', 'Weight', 'Dimensions', 'IdenticalParts', 'Sets', 'Type',
                   'PartOf', 'Alternates', 'RebrickableID', 'BrickLinkID', 'BrickOwlID', 'LegoID', 'LDrawID', 'BricksetID', 'PeeronID',
                   'ImageLinks', 'RebricableLink', 'BricklinkLink', 'BrickOwlLink', 'BricksetLink', 'LDrawLink', 'Quantity']

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
        self.logger.info(f'NUMBER OF PREVIOUSLY SCRAPED PARTS = {len(previously_scraped)}')
        for part in previously_scraped:
            # Initialize item
            item = dict()
            for k in self.field_names:
                item[k] = ''

            item.update(part)
            yield item

            if item['DesignID'] and item['DesignID'] not in self.done_parts:
                self.done_parts.append(item['DesignID'])
            if item['ElementID'] and item['ElementID'] not in self.done_parts:
                self.done_parts.append(item['ElementID'])

        parts_lst = []
        memo_parts = set()
        with open(part_numbers_file_path, 'r', encoding='utf-8') as ifile:
            for line in ifile.readlines():
                try:
                    j = json.loads(line.strip())
                    if j['part_num'] not in memo_parts:
                        parts_lst.append(j)
                        memo_parts.add(j['part_num'])
                except:
                    pass
        self.logger.info(f'NUMBER OF AVAILABLE PARTS TO SCRAPE = {len(parts_lst)}')

        for part in parts_lst:
            if part['part_num'] not in self.done_parts:
                yield scrapy.Request(url=self.URL, callback=self.parse_part,
                                     meta={'part': part}, dont_filter=True)

    def parse_part(self, response):
        part = response.meta['part']
        part_num = part['part_num']

        if self.API_requests > API_CALLS_DAILY_LIMIT * len(API_KEYS):
            try:
                self.crawler.engine.stop()
                self.logger.info('STOP: API RATE LIMIT REACHED')
            except:
                pass
            return

        if part_num in self.done_parts:
            return

        api_url = f'https://api.bricklink.com/api/store/v1/items/part/{part_num}'
        try:
            response = requests.get(api_url, auth=get_oauth())
            self.API_requests += 1
            self.logger.info(f'API REQUEST #{self.API_requests} FOR URL {api_url}')
            part_data = json.loads(response.text)['data']
        except Exception as e:
            self.logger.info(f'ERROR REQUEST TO {api_url}, {e}, {response.text}')
            part_data = None

        if part_data:
            self.done_parts.append(part_num)

            # Initialize item
            item = dict()
            for k in self.field_names:
                item[k] = ''

            item['BricklinkLink'] = part['part_url']
            item['BrickLinkID'] = part_data.get('no', '')
            item['DesignID'] = part_data.get('no', '')
            item['Name'] = part_data.get('name', '')
            item['BrickLinkName'] = part_data.get('name', '')
            item['CategoryID'] = part_data.get('category_id', '')
            item['ProductionYears'] = part_data.get('year_released', '')
            item['ImageLinks'] = part_data.get('image_url', '')
            if item['ImageLinks'] and not item['ImageLinks'].lower().startswith('https:'):
                item['ImageLinks'] = 'https:' + item['ImageLinks']

            item['Weight'] = part_data.get('weight', '')
            item['Dimensions'] = 'x '.join([part_data.get(cord) for cord in ['dim_x', 'dim_y', 'dim_z'] if part_data.get(cord)])

            item['Description'] = cleanHtml(part_data.get('description', ''))
            item['Alternates'] = part_data.get('alternate_no', '')

            yield item

            elements_url = f'https://api.bricklink.com/api/store/v1/item_mapping/part/{part_num}'
            try:
                response = requests.get(elements_url, auth=get_oauth())
                self.API_requests += 1
                self.logger.info(f'API REQUEST #{self.API_requests} FOR URL {elements_url}')

                elements_data = json.loads(response.text)['data']
            except Exception as e:
                self.logger.info(f'ERROR REQUEST TO {elements_url}, {e}, {response.text}')
                elements_data = None

            if elements_data:
                # color_lst = []
                for element in elements_data:
                    element_id = element.get('element_id', '')
                    color_id = element.get('color_id', '')
                    # if element_id and element_id != item['DesignID']:
                    if True:
                        if element_id and element_id not in self.done_parts:
                            self.done_parts.append(element_id)

                        item['ElementID'] = element_id
                        item['ColorID'] = color_id
                        yield item
                    # elif color_id:
                    #     color_lst.append(color_id)

                # if color_lst:
                #     item['ElementID'] = ''
                #     item['ColorID'] = color_lst
                #     yield item
