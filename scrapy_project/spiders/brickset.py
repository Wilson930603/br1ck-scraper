# import needed libraries
import json
import re
import time
import urllib.parse

import requests
from bs4 import BeautifulSoup as BS
import scrapy
from scrapy_project.spiders.base_spider import BaseSpider


def cleanHtml(raw_txt):
    clean_txt = raw_txt
    clean_txt = re.sub(r"[\s\t\n\r]+", ' ', clean_txt).strip()
    return clean_txt


class Spider(BaseSpider):
    # spider name; used for calling spider
    name = 'brickset'
    domain = 'https://brickset.com'
    parts_main_page = 'https://brickset.com/parts'

    field_names = ['ElementID', 'DesignID', 'Name', 'Description', 'LDrawName', 'LegoName', 'BrickOwlName',
                   'BricksetName', 'RebrickableName', 'BrickLinkName', 'ColorID', 'CategoryID', 'SubCategoryID',
                   'Categories', 'Tags', 'ProductionYears', 'Weight', 'Dimensions', 'IdenticalParts', 'Sets', 'Type',
                   'PartOf', 'Alternates', 'RebrickableID', 'BrickLinkID', 'BrickOwlID', 'LegoID', 'LDrawID',
                   'ImageLinks', 'RebricableLink', 'BricklinkLink', 'BrickOwlLink', 'BricksetLink', 'LDrawLink', 'Quantity']

    custom_settings = {
        # 'DOWNLOAD_DELAY': 0.5,
        'CONCURRENT_REQUESTS': 8,
    }

    headers1 = {
        'Authority': 'brickset.com',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36',
    }

    headers2 = {
        'Authority': 'brickset.com',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36',
        'Cookie': 'PreferredCountry2=CountryCode=US&CountryName=United States; partsPageLength=100; setsPageLength=100; .ASPXAUTH=9317B75DA06D0D36D158202EC2A62E084FDAAE5C16AA5CB741460030279951423ED71FC4463F63246C7FF26D3ED220DE0C471962C4AF5634F2A1498A937E733932AE899359F347E8C8F1ED0EA6F9F184FCD10FBD745055135A44BD0F6F766F6259E6A178223D051F077009D69ADB47C3AEAF062CC2E39878BC17D67B4E0C02A7868A3FDE	; ASP.NET_SessionId=zmd4rruoh4wswpfjvh2pmid3',
    }

    def start_requests(self):
        yield scrapy.Request(url=self.parts_main_page, callback=self.parse_platforms,
                             headers=self.headers1)

    def parse_platforms(self, response):
        if response.status != 200:
            self.logger.info(f'ERROR GETTING MAIN PAGE RESPONSE STATUS <{response.status}>')
        else:
            soup = BS(response.text, "html.parser")
            selects = soup.find_all('select')
            for select in selects:
                if 'Platform' in select.text:
                    options = select.find_all('option', {'value': True})
                    for option in options:
                        page = 1
                        platform = option['value']
                        platform_link = self.domain + platform + f'/page-{page}'
                        yield scrapy.Request(url=platform_link, callback=self.parse,
                                             meta={'page': page,
                                                   'platform': platform},
                                             cookies={
                                                 'PreferredCountry2': 'CountryCode=US&CountryName=United States',
                                                 'partsPageLength': 100,
                                                 'setsPageLength': 100,
                                                 '.ASPXAUTH': '9317B75DA06D0D36D158202EC2A62E084FDAAE5C16AA5CB741460030279951423ED71FC4463F63246C7FF26D3ED220DE0C471962C4AF5634F2A1498A937E733932AE899359F347E8C8F1ED0EA6F9F184FCD10FBD745055135A44BD0F6F766F6259E6A178223D051F077009D69ADB47C3AEAF062CC2E39878BC17D67B4E0C02A7868A3FDE',
                                                 'ASP.NET_SessionId': 'zmd4rruoh4wswpfjvh2pmid3',
                                             },
                                             headers=self.headers2)

    def parse(self, response, **kwargs):
        page = response.meta['page']
        platform = response.meta['platform']
        if response.status != 200:
            self.logger.info(f'ERROR GETTING PLATFORM PAGE {response.url} RESPONSE STATUS <{response.status}>')
        else:
            soup = BS(response.text, "html.parser")
            parts = [part.find('h1').find('a', {'href': True})['href'] for part in soup.find_all('article') if
                     part.find('h1') and part.find('h1').find('a', {'href': True})]

            self.logger.info(f'Number of parts in {platform} page #{page} = {len(parts)}')

            ifile = open('brickset_part_links.txt', 'a', encoding='utf-8')

            for part_link in parts:
                ifile.write(f'platform {platform}, Page:{page}; ' + self.domain + part_link + "\n")

                yield scrapy.Request(url=self.domain + part_link, callback=self.parse_part_page,
                                     headers=self.headers1)

            ifile.close()

            # follow next page
            try:
                next_page = soup.find('div', {'class': 'pagination'}).find('li', {'class': 'next'}).find('a', {'href': re.compile('page-')})
                if next_page:
                    next_page_link = next_page['href']
                    page = re.search(r"page-(\d+)", next_page_link).group(1)

                    yield scrapy.Request(url=next_page_link, callback=self.parse,
                                         meta={'page': page,
                                               'platform': platform},
                                         headers=self.headers1)
            except Exception:
                pass

    def parse_part_page(self, response):
        if response.status != 200:
            self.logger.info(f'ERROR GET part LINK: {response.request.url}, STATUS <{response.status}>')
        else:
            soup = BS(response.text, "html.parser")
            if not soup.find('section', {'class': 'main'}) or not soup.find('section', {'class': 'main'}).find('h1'):
                self.logger.info(f'ERROR2 GET part LINK: {response.request.url}, STATUS <{response.status}>')
                return

            # Initialize item
            item = dict()
            for k in self.field_names:
                item[k] = ''

            item['BricksetLink'] = response.request.url
            item['Name'] = soup.find('section', {'class': 'main'}).find('h1').text
            item['Categories'] = []
            item['Tags'] = []
            sections = soup.find_all('section', {'class': 'featurebox'})
            for section in sections:
                if 'Element number' in section.text:
                    item_table = section
                    break
            else:
                self.logger.info(f'ERROR FIND Part Table: {response.request.url}')
                return

            dt = [x.text.strip() for x in item_table.find_all('dt')]
            dd = [x.text.strip() for x in item_table.find_all('dd')]

            table_rows = list(zip(dt, dd))

            for row in table_rows:
                if 'Element number' in row[0]:
                    item['ElementID'] = row[-1]
                elif 'Element name' in row[0]:
                    item['Name'] = row[-1]
                elif 'Design' in row[0]:
                    item['DesignID'] = row[-1]
                elif 'Category' in row[0]:
                    item['Categories'].append(row[-1])
                elif 'Tags' in row[0]:
                    item['Tags'].append(row[-1])
                elif 'Produced' in row[0]:
                    item['ProductionYears'] = row[-1]
                elif 'Colour ID' in row[0]:
                    item['ColorID'] = row[-1]
                elif 'BrickLink Name' in row[0]:
                    item['BrickLinkName'] = row[-1]

            part_image = soup.find('a', {'href': re.compile(r'/ajax/parts/mainImage')})
            if part_image:
                try:
                    item['ImageLinks'] = urllib.parse.unquote(part_image['href'].split('/ajax/parts/mainImage?image=')[1])
                except Exception:
                    pass

            # handle design page
            if item_table and item['DesignID']:
                design_link = item_table.find('a', {'href': re.compile('/parts/design-')})
                if design_link and design_link['href']:
                    yield scrapy.Request(url=self.domain + design_link['href'], callback=self.parse_generic_part,
                                         headers=self.headers1)

            identical_link = soup.find('a', text=re.compile('Identical parts'))['href'] if soup.find('a', text=re.compile('Identical parts')) else ''
            colors_link = soup.find('a', text=re.compile('All colours'))['href'] if soup.find('a', text=re.compile('All colours')) else ''
            if identical_link:
                yield scrapy.Request('https://brickset.com' + identical_link, callback=self.parse_identical,
                                     headers=self.headers2,
                                     cookies={
                                         'PreferredCountry2': 'CountryCode=US&CountryName=United States',
                                         'partsPageLength': 100,
                                         'setsPageLength': 100,
                                         'X-Requested-With': 'XMLHttpRequest',
                                         '.ASPXAUTH': '9317B75DA06D0D36D158202EC2A62E084FDAAE5C16AA5CB741460030279951423ED71FC4463F63246C7FF26D3ED220DE0C471962C4AF5634F2A1498A937E733932AE899359F347E8C8F1ED0EA6F9F184FCD10FBD745055135A44BD0F6F766F6259E6A178223D051F077009D69ADB47C3AEAF062CC2E39878BC17D67B4E0C02A7868A3FDE',
                                         'ASP.NET_SessionId': 'zmd4rruoh4wswpfjvh2pmid3',
                                     },
                                     meta={'item': item,
                                           'colors_link': colors_link},
                                     dont_filter=True
                                     )
    
    def parse_generic_part(self, response):
        if response.status != 200:
            self.logger.info(f'ERROR RESPONSE parse_generic_part of link: {response.request.url}, STATUS <{response.status}>')
        else:
            soup = BS(response.text, "html.parser")
            if not soup.find('section', {'class': 'iteminfo'}) or not soup.find('section', {'class': 'iteminfo'}).find('h1'):
                self.logger.info(f'ERROR2 GET generic part LINK: {response.request.url}, STATUS <{response.status}>')

            # Initialize item
            item = dict()
            for k in self.field_names:
                item[k] = ''

            item['BricksetLink'] = response.request.url
            item['Name'] = soup.find('section', {'class': 'iteminfo'}).find('h1').text

            x = re.search(r'Design number (.*)', item['Name'])
            if x:
                item['DesignID'] = x.group(1)

            item_table = soup.find('section', {'class': 'iteminfo'})
            dt = [x.text.strip() for x in item_table.find_all('dt')]
            dd = [x.text.strip() for x in item_table.find_all('dd')]

            table_rows = list(zip(dt, dd))

            for row in table_rows:
                if 'Produced' in row[0]:
                    item['ProductionYears'] = row[-1]
                elif 'Element name' in row[0]:
                    item['Name'] = row[-1]

            if item_table and item_table.find('div', {'class': 'tags'}):
                item['Categories'] = [href.text for href in item_table.find('div', {'class': 'tags'}).find_all('a')]

            part_image = item_table.find('img', {'src': True})
            if part_image:
                try:
                    item['ImageLinks'] = part_image['src']
                except Exception:
                    pass

            yield item

    def parse_identical(self, response):
        item = response.meta['item']
        colors_link = response.meta['colors_link']

        if response.status != 200:
            self.logger.info(f'ERROR RESPONSE parse_identical of item: {item["BricksetLink"]}, {response.request.url}, STATUS <{response.status}>')
        else:
            try:
                item['IdenticalParts'] = []
                soup = BS(response.text, "html.parser")
                identical_parts = soup.find_all('article')
                for identical_part in identical_parts:
                    if identical_part.find('div', {'class': 'tags'}) and identical_part.find('div', {'class': 'tags'}).find('a'):
                        _id = identical_part.find('div', {'class': 'tags'}).find('a').text
                        if _id and _id != item['ElementID']:
                            item['IdenticalParts'].append(_id)
            except Exception as e:
                self.logger.info(f'ERROR SOUP parse_identical of item: {item["BricksetLink"]}, {response.request.url}, {e}')

        yield item

        if colors_link:
            yield scrapy.Request('https://brickset.com' + colors_link, callback=self.parse_colors,
                                 meta={'parent_link': item['BricksetLink']},
                                 headers=self.headers2,
                                 cookies={
                                     'PreferredCountry2': 'CountryCode=US&CountryName=United States',
                                     'partsPageLength': 100,
                                     'setsPageLength': 100,
                                     'X-Requested-With': 'XMLHttpRequest',
                                     '.ASPXAUTH': '9317B75DA06D0D36D158202EC2A62E084FDAAE5C16AA5CB741460030279951423ED71FC4463F63246C7FF26D3ED220DE0C471962C4AF5634F2A1498A937E733932AE899359F347E8C8F1ED0EA6F9F184FCD10FBD745055135A44BD0F6F766F6259E6A178223D051F077009D69ADB47C3AEAF062CC2E39878BC17D67B4E0C02A7868A3FDE',
                                     'ASP.NET_SessionId': 'zmd4rruoh4wswpfjvh2pmid3',
                                 })

    def parse_colors(self, response):
        parent_link = response.meta['parent_link']
        if response.status != 200:
            self.logger.info(f'ERROR RESPONSE parse_colors of item: {parent_link}, {response.request.url}, STATUS <{response.status}>')
        else:
            try:
                soup = BS(response.text, "html.parser")
                colors_parts = soup.find_all('article')
                self.logger.info(f'FOUND colors OF {parent_link} = {len(colors_parts)}')

                for colors_part in colors_parts:
                    try:
                        if colors_part.find('div', {'class': 'tags'}) and colors_part.find('div', {'class': 'tags'}).find('a', {'href': True}):
                            color_link = colors_part.find('div', {'class': 'tags'}).find('a', {'href': True})['href']

                            yield scrapy.Request(url=self.domain + color_link,
                                                 callback=self.parse_part_page,
                                                 headers=self.headers1)
                    except Exception as e:
                        self.logger.info(f'iteration color {colors_part} error {e}')
            except Exception as err:
                self.logger.info(f'ERROR GET colors: {response.request.url}, err <{err}>')
