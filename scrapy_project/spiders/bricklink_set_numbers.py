# import needed libraries
import json
import math
import random
import re
import time
from bs4 import BeautifulSoup as BS
import scrapy
from scrapy_project.spiders.base_spider import BaseSpider

# file path to save scraped Bricklink part numbers
sets_file_path = './bricklink_set_numbers_2.txt'


class Spider(BaseSpider):
    # spider name; used for calling spider
    name = 'bricklink_set_numbers'
    domain = 'https://www.bricklink.com'
    categories_page = 'https://www.bricklink.com/catalogTree.asp?itemType=S'

    custom_settings = {
        'DOWNLOAD_DELAY': 0.5,
        'CONCURRENT_REQUESTS': 1,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
        'CONCURRENT_REQUESTS_PER_IP': 1,
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy.downloadermiddlewares.defaultheaders.DefaultHeadersMiddleware': 100,
            'rotating_proxies.middlewares.RotatingProxyMiddleware': 610,
            'rotating_proxies.middlewares.BanDetectionMiddleware': 620},
        'ROTATING_PROXY_LIST_PATH': 'proxy_25000.txt',
        'ROTATING_PROXY_PAGE_RETRY_TIMES': 5,
    }

    headers = {
        'Authority': 'bricklink.com',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36',
    }

    def start_requests(self):
        yield scrapy.Request(url=self.categories_page, callback=self.parse_categories_page,
                             headers=self.headers)

    def parse_categories_page(self, response):
        if response.status != 200:
            self.logger.info(f'ERROR GETTING MAIN PAGE RESPONSE STATUS <{response.status}>')
        else:
            soup = BS(response.text, "html.parser")
            categories = soup.find_all('a', {'href': re.compile('/catalogList\.asp\?catType=S&catString=')})
            for category in categories:
                category_name = category.text
                category_link = 'https://www.bricklink.com' + category['href']
                cat_string = category_link.split('catString=')[1]
                if '.' not in cat_string:
                    page = 1
                    yield scrapy.Request(url=category_link+f'&pg={page}', callback=self.parse,
                                         meta={'page': page,
                                               'category_name': category_name,
                                               'category_link': category_link},
                                         headers=self.headers)

    def parse(self, response, **kwargs):
        page = response.meta['page']
        category_name = response.meta['category_name']
        category_link = response.meta['category_link']
        if response.status != 200:
            self.logger.info(f'ERROR GETTING category PAGE {response.url} RESPONSE STATUS <{response.status}>')
        else:
            soup = BS(response.text, "html.parser")

            try:
                if '{' in category_name:
                    breadcrumbs_div = soup.find('div', {'class': 'catalog-list__header-breadcrumbs'})
                    td_element = breadcrumbs_div.find_parent('table').find_parent('td')
                    breadcrumbs_div.clear()
                    if td_element.text.strip():
                        category_name = td_element.text.strip()
            except Exception:
                pass

            parts = soup.find_all('a', {'href': re.compile('/v2/catalog/catalogitem\.page\?S=')})
            self.logger.info(f'Number of sets in {category_name} page #{page} = {len(parts)}')

            for _ in range(50):
                try:
                    ifile = open(sets_file_path, 'a', encoding='utf-8')
                    break
                except Exception:
                    time.sleep(round(random.uniform(0.1, 1.0), 1))
                    continue
            for part_link in parts:
                part_url = self.domain + part_link['href']
                part_num = part_link.text.strip()
                ifile.write(json.dumps({"category_name": category_name,
                                        "category_page": page,
                                        "set_url": part_url,
                                        "set_num": part_num}) + "\n")

            ifile.close()

            # follow next page
            try:
                next_page = soup.find('div', {'class': 'catalog-list__pagination--top'}).find('a', text='Next')
                if next_page:
                    page += 1
                    yield scrapy.Request(url=category_link+f'&pg={page}', callback=self.parse,
                                         meta={'page': page,
                                               'category_name': category_name,
                                               'category_link': category_link},
                                         headers=self.headers)
            except Exception as e:
                pass
