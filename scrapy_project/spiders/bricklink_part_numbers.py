# import needed libraries
import json
import random
import re
import time
from bs4 import BeautifulSoup as BS
import scrapy
from scrapy_project.spiders.base_spider import BaseSpider

# file path to save scraped Bricklink part numbers
part_numbers_file_path = './bricklink_part_numbers.txt'


class Spider(BaseSpider):
    # spider name; used for calling spider
    name = 'bricklink_part_numbers'
    domain = 'https://www.bricklink.com'
    categories_page = 'https://www.bricklink.com/catalogTree.asp?itemType=P'

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
            categories = soup.find_all('a', {'href': re.compile('/catalogList\.asp\?catType=P&catString=')})
            for category in categories:
                page = 1
                category_name = category.find('b').text
                yield scrapy.Request(url=self.domain + category['href'], callback=self.parse,
                                     meta={'page': page,
                                           'category_name': category_name},
                                     headers=self.headers)

    def parse(self, response, **kwargs):
        page = response.meta['page']
        category_name = response.meta['category_name']
        if response.status != 200:
            self.logger.info(f'ERROR GETTING category PAGE {response.url} RESPONSE STATUS <{response.status}>')
        else:
            soup = BS(response.text, "html.parser")
            parts = soup.find_all('a', {'href': re.compile('/v2/catalog/catalogitem\.page\?P=')})

            self.logger.info(f'Number of parts in {category_name} page #{page} = {len(parts)}')

            for _ in range(50):
                try:
                    ifile = open(part_numbers_file_path, 'a', encoding='utf-8')
                    break
                except Exception:
                    time.sleep(round(random.uniform(0.1, 1.0), 1))
                    continue
            for part_link in parts:
                part_url = self.domain + part_link['href']
                part_num = part_link.text.strip()
                ifile.write(json.dumps({"category_name": category_name,
                                        "category_page": page,
                                        "part_url": part_url,
                                        "part_num": part_num}) + "\n")

            ifile.close()

            # follow next page
            try:
                next_page = soup.find('div', {'class': 'catalog-list__pagination--top'}).find('a', text='Next')
                if next_page:
                    next_page_link = next_page['href']
                    page = re.search(r"\?pg=(\d+)", next_page_link).group(1)
                    yield scrapy.Request(url=self.domain + next_page_link, callback=self.parse,
                                         meta={'page': page,
                                               'category_name': category_name},
                                         headers=self.headers)
            except Exception as e:
                pass
