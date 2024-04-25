# import needed libraries
import csv
import os
import platform
import re
import scrapy
from scrapy_project.spiders.base_spider import BaseSpider


previously_scraped_file_path = './output/bricklink_2023-08-02_19.13.09.csv'
previously_scraped = []
# scrape data in chunks due to API rate limits
if previously_scraped_file_path:
    with open(previously_scraped_file_path, 'r', encoding='utf-8-sig') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            previously_scraped.append(row)


def cleanHtml(raw_txt):
    clean_txt = raw_txt
    clean_txt = re.sub(r"[\s\t\n\r]+", ' ', clean_txt).strip()
    return clean_txt


class Spider(BaseSpider):
    # spider name; used for calling spider
    name = 'bricklink_dimension'
    domain = 'https://www.bricklink.com'
    if platform.system() == 'Linux':
        URL = 'file:////' + os.getcwd() + '/scrapy.cfg'
    else:
        URL = 'file:///' + os.getcwd() + '/scrapy.cfg'

    # start_urls = ['https://www.bricklink.com']
    start_urls = [URL]

    field_names = ['ElementID', 'DesignID', 'Name', 'Description', 'LDrawName', 'LegoName', 'BrickOwlName',
                   'BricksetName', 'RebrickableName', 'BrickLinkName', 'ColorID', 'CategoryID', 'SubCategoryID',
                   'Categories', 'Tags', 'ProductionYears', 'Weight', 'Dimensions', 'IdenticalParts', 'Sets', 'Type',
                   'PartOf', 'Alternates', 'RebrickableID', 'BrickLinkID', 'BrickOwlID', 'LegoID', 'LDrawID',
                   'BricksetID', 'PeeronID',
                   'ImageLinks', 'RebricableLink', 'BricklinkLink', 'BrickOwlLink', 'BricksetLink', 'LDrawLink',
                   'Quantity']

    custom_settings = {
        # 'DOWNLOAD_DELAY': 0.5,
        'CONCURRENT_REQUESTS': 32,
        'CONCURRENT_REQUESTS_PER_IP': 1,
        'DOWNLOADER_MIDDLEWARES': {
            'rotating_proxies.middlewares.RotatingProxyMiddleware': 610,
            'rotating_proxies.middlewares.BanDetectionMiddleware': 620},
        'ROTATING_PROXY_LIST_PATH': 'proxy_25000.txt',
        'ROTATING_PROXY_PAGE_RETRY_TIMES': 10,
    }

    # custom_settings = {
    #     'DOWNLOAD_DELAY': 0.5,
    #     'CONCURRENT_REQUESTS': 2,
    # }

    requested_parts = list()
    dimensions_dict = {}

    headers = {
        'Authority': 'bricklink.com',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36',
    }

    if platform.system() == 'Linux':
        URL = 'file:////' + os.getcwd() + '/scrapy.cfg'
    else:
        URL = 'file:///' + os.getcwd() + '/scrapy.cfg'

    def parse(self, response, **kwargs):
        self.logger.info(f'NUMBER OF PREVIOUSLY SCRAPED PARTS = {len(previously_scraped)}')

        for part in previously_scraped:
            if part['BricklinkLink'] not in self.dimensions_dict and part['BricklinkLink'] not in self.requested_parts:
                self.requested_parts.append(part['BricklinkLink'])
                yield scrapy.Request(url=part['BricklinkLink'], callback=self.parse_part, priority=10,
                                     meta={'part': part}, headers=self.headers, dont_filter=True)
            elif part['BricklinkLink'] not in self.dimensions_dict and part['BricklinkLink'] in self.requested_parts:
                yield scrapy.Request(url=self.URL, callback=self.parse_element, priority=5,
                                     meta={'part': part, 'trial': 1}, dont_filter=True)
            else:
                part['Dimensions'] = self.dimensions_dict[part['BricklinkLink']]
                # Initialize item
                item = dict()
                for k in self.field_names:
                    item[k] = ''

                item.update(part)

                yield item

    def parse_part(self, response):
        part = response.meta['part']
        dim = ''

        if response.status != 200:
            self.logger.info(f'ERROR GETTING PART PAGE {part["BricklinkLink"]} RESPONSE STATUS <{response.status}>')
        else:
            try:
                dim = cleanHtml(response.xpath('//span[@id="dimSec"][2]/text()').get())
            except:
                try:
                    dim = cleanHtml(response.xpath('//span[@id="dimSec"][1]/text()').get())
                except:
                    self.logger.info(f"ERROR GETTING DIMENSION OF PART {part['BricklinkLink']}")

        if dim:
            if '(' in dim:
                dim = dim[:dim.find('(')]
            part['Dimensions'] = dim

            self.logger.info(f"RESPONSE: {part['BricklinkLink']}")

        self.dimensions_dict[part['BricklinkLink']] = dim

        # Initialize item
        item = dict()
        for k in self.field_names:
            item[k] = ''

        item.update(part)

        yield item

    def parse_element(self, response):
        part = response.meta['part']
        trial = response.meta['trial']
        try:
            part['Dimensions'] = self.dimensions_dict[part['BricklinkLink']]

            self.logger.info(f"DELAYED SUCCESS: {part['BricklinkLink']}")

            # Initialize item
            item = dict()
            for k in self.field_names:
                item[k] = ''

            item.update(part)

            yield item
        except KeyError:
            if trial < 10:
                yield scrapy.Request(url=self.URL, callback=self.parse_element, priority=5,
                                     meta={'part': part, 'trial': trial+1}, dont_filter=True)
            else:
                self.logger.info(f"REPEAT: {part['BricklinkLink']}")

                yield scrapy.Request(url=part['BricklinkLink'], callback=self.parse_part, priority=20,
                                     meta={'part': part}, headers=self.headers, dont_filter=True)
