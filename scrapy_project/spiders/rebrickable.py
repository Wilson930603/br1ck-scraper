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
    name = 'rebrickable'
    domain = 'https://www.rebrickable.com'

    page_size = 100

    done_links = list()

    field_names = ['ElementID', 'DesignID', 'Name', 'Description', 'LDrawName', 'LegoName', 'BrickOwlName',
                   'BricksetName', 'RebrickableName', 'BrickLinkName', 'ColorID', 'CategoryID', 'SubCategoryID',
                   'Categories', 'Tags', 'ProductionYears', 'Weight', 'Dimensions', 'IdenticalParts', 'Sets', 'Type',
                   'PartOf', 'Alternates', 'RebrickableID', 'BrickLinkID', 'BrickOwlID', 'LegoID', 'LDrawID', 'BricksetID', 'PeeronID',
                   'ImageLinks', 'RebricableLink', 'BricklinkLink', 'BrickOwlLink', 'BricksetLink', 'LDrawLink', 'Quantity']

    custom_settings = {
        'DOWNLOAD_DELAY': 1,
        'CONCURRENT_REQUESTS': 1,
    }

    def start_requests(self):
        page = 1
        categories_link = f'https://rebrickable.com/api/v3/lego/part_categories/?page={page}&page_size={self.page_size}'
        yield scrapy.Request(url=categories_link, callback=self.parse_categories,
                             meta={'page': page},
                             headers=get_headers())

    def parse_categories(self, response):
        page = response.meta['page']

        if response.status == 429:
            self.logger.info(f'GET 429 RESPONSE STATUS, WILL PAUSE FOR 1 MIN')
            self.crawler.engine.pause()
            time.sleep(60)
            self.crawler.engine.unpause()
            yield scrapy.Request(url=response.url, callback=self.parse_categories, meta=response.meta, dont_filter=True)
        elif response.status != 200:
            self.logger.info(f'ERROR GETTING MAIN PAGE RESPONSE STATUS <{response.status}>')
        else:
            try:
                categories_data = json.loads(response.text)
                categories = categories_data['results']
                self.logger.info(f'Main categories page #{page} has #{len(categories)} categories')

                for category in categories:
                    category_name = category['name']
                    category_part_count = category['part_count']
                    category_id = category['id']

                    for category_page in range(1, math.ceil(category_part_count/self.page_size)+1):
                        category_link = f'https://rebrickable.com/api/v3/lego/parts/?page={category_page}&page_size={self.page_size}&part_cat_id={category_id}'
                        yield scrapy.Request(url=category_link, callback=self.parse_category,
                                             meta={
                                                 'category_page': category_page,
                                                 'category_id': category_id,
                                                 'category_name': category_name,
                                             },
                                             headers=get_headers(),
                                             priority=2)

                # follow next page
                if len(categories) == self.page_size:
                    page += 1
                    categories_link = f'https://rebrickable.com/api/v3/lego/part_categories/?page={page}&page_size={self.page_size}'
                    yield scrapy.Request(url=categories_link, callback=self.parse_categories,
                                         meta={'page': page},
                                         headers=get_headers())

            except Exception as e:
                self.logger.info(f'ERROR Obtain categories json, {e}, {response.text}')

    def parse_category(self, response):
        category_page = response.meta['category_page']
        category_id = response.meta['category_id']
        category_name = response.meta['category_name']

        if response.status == 429:
            self.logger.info(f'GET 429 RESPONSE STATUS, WILL PAUSE FOR 1 MIN')
            self.crawler.engine.pause()
            time.sleep(60)
            self.crawler.engine.unpause()
            yield scrapy.Request(url=response.url, callback=self.parse_category, meta=response.meta, dont_filter=True, priority=2)
        elif response.status != 200:
            self.logger.info(f'ERROR GETTING MAIN PAGE RESPONSE STATUS <{response.status}>')
        else:
            try:
                category_data = json.loads(response.text)
                parts = category_data['results']
                self.logger.info(f'Page #{category_page} of category {category_name} has #{len(parts)}')

                part_nums = [part['part_num'] for part in parts]
                parts_info_url = f'https://rebrickable.com/api/v3/lego/parts/?part_nums={",".join(part_nums)}&inc_part_details=1'
                yield scrapy.Request(url=parts_info_url, callback=self.parse_category_parts,
                                     meta={
                                         'category_page': category_page,
                                         'category_name': category_name,
                                         'category_id': category_id,
                                     },
                                     headers=get_headers(),
                                     priority=3)

            except Exception as e:
                self.logger.info(f'ERROR Obtain categories json, {e}, {response.text}')

    def parse_category_parts(self, response):
        category_page = response.meta['category_page']
        category_id = response.meta['category_id']
        category_name = response.meta['category_name']

        if response.status == 429:
            self.logger.info(f'GET 429 RESPONSE STATUS, WILL PAUSE FOR 1 MIN')
            self.crawler.engine.pause()
            time.sleep(60)
            self.crawler.engine.unpause()
            yield scrapy.Request(url=response.url, callback=self.parse_category_parts, meta=response.meta, dont_filter=True, priority=2)
        elif response.status != 200:
            self.logger.info(f'ERROR GETTING MAIN PAGE RESPONSE STATUS <{response.status}>')
        else:
            try:
                category_data = json.loads(response.text)
                parts = category_data['results']
                self.logger.info(f'PARTS INFO OF PAGE #{category_page} of category {category_name} has #{len(parts)}')

                for part in parts:
                    # Initialize item
                    item = dict()
                    for k in self.field_names:
                        item[k] = ''

                    item['RebricableLink'] = part['part_url']
                    item['DesignID'] = part['part_num']
                    item['Name'] = part['name']
                    item['RebrickableName'] = part['name']
                    item['Categories'] = [category_name]
                    item['CategoryID'] = category_id
                    item['ImageLinks'] = part['part_img_url']

                    years_lst = []
                    if 'year_from' in part:
                        years_lst.append(str(part['year_from']))
                    if 'year_to' in part:
                        years_lst.append(str(part['year_to']))
                    if years_lst:
                        item['ProductionYears'] = ' to '.join(years_lst)

                    if 'external_ids' in part:
                        for website, external_id in part['external_ids'].items():
                            if 'BrickLink' == website:
                                item['BrickLinkID'] = external_id
                                item['BricklinkLink'] = f'https://www.bricklink.com/v2/catalog/catalogitem.page?P={external_id}&utm_source=rebrickable#T=S'
                            elif 'BrickOwl' == website:
                                item['BrickOwlID'] = external_id
                                item['BrickOwlLink'] = f'https://brickowl.com/catalog/{external_id}?utm_source=rebrickable'
                            elif 'Brickset' == website:
                                item['BricksetID'] = external_id
                                item['BricksetLink'] = f'https://brickset.com/parts/design-{external_id}'
                            elif 'LDraw' == website:
                                item['LDrawID'] = external_id
                                item['LDrawLink'] = f'https://www.ldraw.org/cgi-bin/ptscan.cgi?q={external_id}&utm_source=rebrickable'
                            elif 'LEGO' == website:
                                item['LegoID'] = external_id
                            elif 'Peeron' == website:
                                item['PeeronID'] = external_id

                    item['part_json'] = part

                    # follow colors
                    part_num = item["DesignID"]
                    color_page = 1
                    colors_link = f'https://rebrickable.com/api/v3/lego/parts/{part_num}/colors/?page={color_page}&page_size={self.page_size}'
                    yield scrapy.Request(url=colors_link, callback=self.parse_colors,
                                         meta={
                                             'item': item,
                                             'part_num': part_num,
                                             'color_page': color_page,
                                             'category_name': category_name,
                                             'category_id': category_id,
                                         },
                                         headers=get_headers(),
                                         priority=4)

            except Exception as e:
                self.logger.info(f'ERROR Obtain categories json, {e}, {response.text}')

    def parse_colors(self, response):
        item = response.meta['item']
        part_num = response.meta['part_num']
        color_page = response.meta['color_page']
        category_id = response.meta['category_id']
        category_name = response.meta['category_name']

        if response.status == 429:
            self.logger.info(f'GET 429 RESPONSE STATUS, WILL PAUSE FOR 1 MIN')
            self.crawler.engine.pause()
            time.sleep(60)
            self.crawler.engine.unpause()
            yield scrapy.Request(url=response.url, callback=self.parse_colors, meta=response.meta, dont_filter=True, priority=4)
        elif response.status != 200:
            self.logger.info(f'ERROR RESPONSE parse_colors of item: {part_num} color_page {color_page}, {response.request.url}, STATUS <{response.status}>')
        else:
            try:
                colors_data = json.loads(response.text)
                colors = colors_data['results']
                self.logger.info(f'Page #{color_page} of part {part_num} has #{len(colors)} colors')

                if colors:
                    item['ColorID'] = [color_part['color_id'] for color_part in colors]

                    for color_part in colors:
                        elements = color_part['elements']
                        for element_id in elements:
                            element_link = f'https://rebrickable.com/api/v3/lego/elements/{element_id}/'
                            yield scrapy.Request(url=element_link, callback=self.parse_element,
                                                 meta={'part_num': part_num,
                                                       'element_id': element_id,
                                                       'category_id': category_id,
                                                       'category_name': category_name},
                                                 headers=get_headers(),
                                                 priority=6)

                    # follow next page
                    if len(colors) == self.page_size:
                        color_page += 1
                        colors_link = f'https://rebrickable.com/api/v3/lego/parts/{part_num}/colors/?page={color_page}&page_size={self.page_size}'
                        yield scrapy.Request(url=colors_link, callback=self.parse_colors,
                                             meta={
                                                 'part_num': part_num,
                                                 'color_page': color_page,
                                             },
                                             headers=get_headers(),
                                             priority=4)

            except Exception as err:
                self.logger.info(f'ERROR GET colors: {response.request.url}, err <{err}>')
        yield item

    def parse_element(self, response):
        part_num = response.meta['part_num']
        element_id = response.meta['element_id']
        category_id = response.meta['category_id']
        category_name = response.meta['category_name']

        # Initialize item
        item = dict()
        for k in self.field_names:
            item[k] = ''

        item['DesignID'] = part_num
        item['ElementID'] = element_id

        if response.status == 429:
            self.logger.info(f'GET 429 RESPONSE STATUS, WILL PAUSE FOR 1 MIN')
            self.crawler.engine.pause()
            time.sleep(60)
            self.crawler.engine.unpause()
            yield scrapy.Request(url=response.url, callback=self.parse_element, meta=response.meta, dont_filter=True, priority=6)
        elif response.status != 200:
            self.logger.info(f'ERROR RESPONSE parse_element of element: {element_id}, {response.request.url}, STATUS <{response.status}>')
        else:
            try:
                element_data = json.loads(response.text)
                item['part_json'] = element_data

                part = element_data['part']
                item['ColorID'] = element_data['color']['id']

                item['RebricableLink'] = part['part_url']
                item['Name'] = part['name']
                item['RebrickableName'] = part['name']
                item['Categories'] = [category_name]
                item['CategoryID'] = category_id
                item['ImageLinks'] = part['part_img_url']

                years_lst = []
                if 'year_from' in part:
                    years_lst.append(str(part['year_from']))
                if 'year_to' in part:
                    years_lst.append(str(part['year_to']))
                if years_lst:
                    item['ProductionYears'] = ' to '.join(years_lst)

                if 'external_ids' in part:
                    for website, external_id in part['external_ids'].items():
                        if 'BrickLink' == website:
                            item['BrickLinkID'] = external_id
                            item['BricklinkLink'] = f'https://www.bricklink.com/v2/catalog/catalogitem.page?P={external_id}&utm_source=rebrickable#T=S'
                        elif 'BrickOwl' == website:
                            item['BrickOwlID'] = external_id
                            item['BrickOwlLink'] = f'https://brickowl.com/catalog/{external_id}?utm_source=rebrickable'
                        elif 'Brickset' == website:
                            item['BricksetID'] = external_id
                            item['BricksetLink'] = f'https://brickset.com/parts/design-{external_id}'
                        elif 'LDraw' == website:
                            item['LDrawID'] = external_id
                            item['LDrawLink'] = f'https://www.ldraw.org/cgi-bin/ptscan.cgi?q={external_id}&utm_source=rebrickable'
                        elif 'LEGO' == website:
                            item['LegoID'] = external_id
                        elif 'Peeron' == website:
                            item['PeeronID'] = external_id

            except Exception as err:
                self.logger.info(f'ERROR GET element: {response.request.url}, err <{err}>')

        yield item
