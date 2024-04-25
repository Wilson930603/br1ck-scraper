# import needed libraries
import json
import math
import re
import time
import scrapy
from scrapy_project.spiders.base_spider import BaseSpider


def cleanHtml(raw_txt):
    clean_txt = raw_txt
    clean_txt = re.sub(r"[\s\t\n\r]+", ' ', clean_txt).strip()
    return clean_txt


n = 0
API_KEYS = [
    '34c4f26c2392f569e8956f7a5f2fb8a6',
    '8a37bcbacfe69459086a86ce07634f83',
    '27998a2725a88f4adf1384386e0cb4f5',
    '9f70a197f54832be70fa89009cfad175',
    'f3525afa047ae4cebc08192fd5f991c9',
]


def get_key():
    global n
    yield API_KEYS[n]
    n += 1
    if n >= len(API_KEYS):
        n = 0


def get_headers():
    return {
        'Authority': 'rebrickable.com',
        'Authorization': f'key {list(get_key())[0]}',
        'Accept': 'application/json',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36',
    }


class Spider(BaseSpider):
    # spider name; used for calling spider
    name = 'rebrickable_sets'
    domain = 'https://www.rebrickable.com'

    page_size = 100

    field_names = ['ItemNumber', 'Name', 'Theme', 'SubTheme', 'Categories',
                   'ProductionYears', 'Tags', 'Parts-Quantity', 'Minifigs-Quantity',
                   'PartsCount', 'Weight', 'Dimensions', 'Instructions', 'ImageLinks', 'Quantity',
                   'RebrickableID', 'BrickLinkID', 'BrickOwlID', 'LegoID', 'LegoCsID', 'BricksetID',
                   'BricksetInternalID',
                   'BrickOwlName', 'BricksetName', 'RebrickableName', 'BrickLinkName', 'LegoName', 'BaseName',
                   'UPC_Barcode', 'EAN_Barcode', 'Min_Age', 'RebricableLink', 'BricklinkLink', 'BrickOwlLink',
                   'BricksetLink'
                   ]

    theme_dict = {}

    custom_settings = {
        'DOWNLOAD_DELAY': 1,
        'CONCURRENT_REQUESTS': 1,
        # 'CLOSESPIDER_ITEMCOUNT': 1000,
    }

    def start_requests(self):
        theme_link = 'https://rebrickable.com/api/v3/lego/themes/?page=1&page_size=1000'
        yield scrapy.Request(url=theme_link, callback=self.parse,
                             headers=get_headers())

    def parse(self, response):
        themes_data = json.loads(response.text)
        themes = themes_data['results']
        self.logger.info(f'Main themes has #{len(themes)} themes')
        self.theme_dict = {theme['id']: {'id': theme['id'],
                                         'name': theme['name'],
                                         'parent_id': theme['parent_id']} for theme in themes}

        page = 1
        sets_link = f'https://rebrickable.com/api/v3/lego/sets/?page={page}&page_size={self.page_size}'
        yield scrapy.Request(url=sets_link, callback=self.parse_sets,
                             meta={'page': page},
                             headers=get_headers(), priority=1)

    def parse_sets(self, response):
        page = response.meta['page']

        if response.status == 429:
            self.logger.info(f'ERROR GET 429 RESPONSE STATUS, WILL PAUSE FOR 1 MIN')
            self.crawler.engine.pause()
            time.sleep(60)
            self.crawler.engine.unpause()
            yield scrapy.Request(url=response.url, callback=self.parse_sets, meta=response.meta, dont_filter=True, priority=1)
        elif response.status != 200:
            self.logger.info(f'ERROR GETTING MAIN PAGE RESPONSE STATUS <{response.status}>')
        else:
            try:
                sets_data = json.loads(response.text)
                sets = sets_data['results']
                self.logger.info(f'Main Sets page #{page} has #{len(sets)} sets')

                for set_json in sets:

                    # Initialize item
                    item = dict()
                    for k in self.field_names:
                        item[k] = ''

                    item['ItemNumber'] = set_json['set_num']
                    item['Name'] = set_json['name']
                    item['RebrickableName'] = set_json['name']
                    item['PartsCount'] = set_json['num_parts']

                    item['RebrickableID'] = item['ItemNumber']
                    item['BricksetID'] = item['ItemNumber']

                    item['RebricableLink'] = set_json['set_url']
                    item['BricklinkLink'] = f'https://www.bricklink.com/v2/search.page?q={item["ItemNumber"]}&utm_source=rebrickable#T=A'
                    item['BrickOwlLink'] = f'https://www.brickowl.com/search/catalog?query={item["ItemNumber"]}&utm_source=rebrickable'
                    item['BricksetLink'] = f'https://brickset.com/sets/{item["ItemNumber"]}'

                    item['ImageLinks'] = set_json['set_img_url']
                    item['ProductionYears'] = set_json['year']
                    set_theme_id = set_json['theme_id']
                    if set_theme_id in self.theme_dict.keys():
                        if self.theme_dict[set_theme_id]['parent_id']:
                            item['Theme'] = self.theme_dict[self.theme_dict[set_theme_id]['parent_id']]['name']
                            item['SubTheme'] = self.theme_dict[set_theme_id]['name']
                        else:
                            item['Theme'] = self.theme_dict[set_theme_id]['name']
                    else:
                        item['Theme'] = set_json['theme_id']

                    if set_json['num_parts']:
                        item['Parts-Quantity'] = list()
                        item['Minifigs-Quantity'] = dict()
                        parts_page = 1
                        parts_link = f'https://rebrickable.com/api/v3/lego/sets/{item["ItemNumber"]}/parts/?page={parts_page}&page_size={self.page_size}&inc_part_details=0&inc_color_details=0&inc_minifig_parts=1'
                        yield scrapy.Request(url=parts_link, callback=self.parse_inventory,
                                             meta={'item': item,
                                                   'parts_page': parts_page},
                                             headers=get_headers(),
                                             priority=5)
                    else:
                        yield item

                # follow next page
                if len(sets) == self.page_size or sets_data['next']:
                    page += 1
                    sets_link = f'https://rebrickable.com/api/v3/lego/sets/?page={page}&page_size={self.page_size}'
                    yield scrapy.Request(url=sets_link, callback=self.parse_sets,
                                         meta={'page': page},
                                         headers=get_headers(), priority=1)

            except Exception as e:
                self.logger.info(f'ERROR Obtain sets json, {e}, {response.text}')

    def parse_inventory(self, response):
        item = response.meta['item']
        parts_page = response.meta['parts_page']

        if response.status == 429:
            self.logger.info(f'ERROR GET 429 RESPONSE STATUS, WILL PAUSE FOR 1 MIN')
            self.crawler.engine.pause()
            time.sleep(60)
            self.crawler.engine.unpause()
            yield scrapy.Request(url=response.url, callback=self.parse_inventory, meta=response.meta, dont_filter=True, priority=5)
        elif response.status != 200:
            self.logger.info(f'ERROR GETTING SET # {item["ItemNumber"]} PAGE {parts_page} PARTS RESPONSE STATUS <{response.status}>')
        else:
            try:
                parts_data = json.loads(response.text)
                set_parts = parts_data['results']
                self.logger.info(f'Set NUM {item["ItemNumber"]} parts page #{parts_page} has #{len(set_parts)} parts')

                for part in set_parts:
                    if not part['set_num'].startswith('fig-'):
                        item['Parts-Quantity'].append({'ElementID': part.get('element_id', ''),
                                                       'RebrickableID': part.get('id', ''),
                                                       'RebricableLink': part.get('part', {}).get('part_url', ''),
                                                       'DesignID': part.get('part', {}).get('part_num', ''),
                                                       'ColorID': part.get('color', {}).get('id', ''),
                                                       'Name': part.get('part', {}).get('name', ''),
                                                       'Qty': part.get('quantity', '')
                                                       }
                                                      )
                    else:
                        if part['set_num'] not in item['Minifigs-Quantity'].keys():
                            item['Minifigs-Quantity'][part['set_num']] = []

                        item['Minifigs-Quantity'][part['set_num']].append({
                            'ElementID': part.get('element_id', ''),
                            'RebrickableID': part.get('id', ''),
                            'RebricableLink': part.get('part', {}).get('part_url', ''),
                            'DesignID': part.get('part', {}).get('part_num', ''),
                            'ColorID': part.get('color', {}).get('id', ''),
                            'Name': part.get('part', {}).get('name', ''),
                            'Qty': part.get('quantity', '')
                        })

                # follow next page
                if len(set_parts) == self.page_size or parts_data['next']:
                    parts_page += 1
                    parts_link = f'https://rebrickable.com/api/v3/lego/sets/{item["ItemNumber"]}/parts/?page={parts_page}&page_size={self.page_size}&inc_part_details=0&inc_color_details=0&inc_minifig_parts=1'
                    yield scrapy.Request(url=parts_link, callback=self.parse_inventory,
                                         meta={'item': item,
                                               'parts_page': parts_page},
                                         headers=get_headers(),
                                         priority=5)
                else:
                    yield item

            except Exception as e:
                self.logger.info(f'ERROR Obtain set parts json, {e}, {response.text}')
