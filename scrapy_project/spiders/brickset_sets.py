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
    name = 'brickset_sets'
    domain = 'https://brickset.com'
    sets_main_page = 'https://brickset.com/sets'

    done_links = list()

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
        # 'DOWNLOAD_DELAY': 0.5,
        'CONCURRENT_REQUESTS': 8,
        # 'CLOSESPIDER_ITEMCOUNT': 1050,
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
        yield scrapy.Request(url=self.sets_main_page, callback=self.parse_themes,
                             headers=self.headers1)

    def parse_themes(self, response):
        if response.status != 200:
            self.logger.info(f'ERROR GETTING MAIN PAGE RESPONSE STATUS <{response.status}>')
        else:
            soup = BS(response.text, "html.parser")
            selects = soup.find_all('select')
            for select in selects:
                if 'Theme' in select.text:
                    options = select.find_all('option', {'value': True})
                    for option in options:
                        page = 1
                        theme = option['value']
                        theme_link = self.domain + theme + f'/page-{page}'
                        yield scrapy.Request(url=theme_link, callback=self.parse,
                                             meta={'page': page,
                                                   'theme': theme},
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
        theme = response.meta['theme']
        if response.status != 200:
            self.logger.info(f'ERROR GETTING Theme PAGE {response.url} RESPONSE STATUS <{response.status}>')
        else:
            soup = BS(response.text, "html.parser")
            sets = [article_tag.find('div', {'class': 'meta'}).find('h1').find('a', {'href': True})['href'] for article_tag in soup.find_all('article', {'class': 'set'})]

            self.logger.info(f'Number of sets in {theme} page #{page} = {len(sets)}')

            ifile = open('brickset_sets_links.txt', 'a', encoding='utf-8')

            for set_link in sets:
                ifile.write(f'theme {theme}, Page:{page}; ' + self.domain + set_link + "\n")

                if set_link in self.done_links:
                    continue

                self.done_links.append(set_link)
                yield scrapy.Request(url=self.domain + set_link, callback=self.parse_set_page,
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
                                               'theme': theme},
                                         cookies={
                                             'PreferredCountry2': 'CountryCode=US&CountryName=United States',
                                             'partsPageLength': 100,
                                             'setsPageLength': 100,
                                             '.ASPXAUTH': '9317B75DA06D0D36D158202EC2A62E084FDAAE5C16AA5CB741460030279951423ED71FC4463F63246C7FF26D3ED220DE0C471962C4AF5634F2A1498A937E733932AE899359F347E8C8F1ED0EA6F9F184FCD10FBD745055135A44BD0F6F766F6259E6A178223D051F077009D69ADB47C3AEAF062CC2E39878BC17D67B4E0C02A7868A3FDE',
                                             'ASP.NET_SessionId': 'zmd4rruoh4wswpfjvh2pmid3',
                                         },
                                         headers=self.headers2)
            except Exception:
                pass

    def parse_set_page(self, response):
        if response.status != 200:
            self.logger.info(f'ERROR GET set LINK: {response.request.url}, STATUS <{response.status}>')
        else:
            soup = BS(response.text, "html.parser")
            if not soup.find('section', {'class': 'main'}) or not soup.find('section', {'class': 'main'}).find('h1'):
                self.logger.info(f'ERROR2 GET set LINK: {response.request.url}, STATUS <{response.status}>')

            # Initialize item
            item = dict()
            for k in self.field_names:
                item[k] = ''

            item['BricksetLink'] = response.request.url
            item['Name'] = soup.find('section', {'class': 'main'}).find('h1').text
            item['BricksetName'] = item['Name']
            item['Categories'] = []
            minifigs_link = None

            sections = soup.find_all('section', {'class': 'featurebox'})
            for section in sections:
                if 'Set number' in section.text:
                    item_table = section
                    break
            else:
                self.logger.info(f'ERROR FIND Part Table: {response.request.url}')
                return

            dt = [x.text.strip() for x in item_table.find_all('dt')]
            dd = [x for x in item_table.find_all('dd')]

            table_rows = list(zip(dt, dd))

            for row in table_rows:
                if 'Set number' in row[0]:
                    item['ItemNumber'] = row[-1].text.strip()
                elif 'Name' in row[0]:
                    item['Name'] = row[-1].text.strip()
                elif 'Theme' == row[0]:
                    item['Theme'] = row[-1].text.strip()
                elif 'Subtheme' == row[0]:
                    item['SubTheme'] = row[-1].text.strip()
                elif 'Category' in row[0]:
                    item['Categories'].append(row[-1].text.strip())
                elif 'Tags' in row[0]:
                    item['Tags'] = [tag.text for tag in row[-1].find_all('a', {'href': re.compile('/sets/tag-')})]
                elif 'Year released' in row[0]:
                    item['ProductionYears'] = row[-1].text.strip()
                elif 'Age ' in row[0]:
                    item['Min_Age'] = row[-1].text.strip()
                elif 'Dimensions' in row[0]:
                    item['Dimensions'] = row[-1].text.strip()
                elif 'Weight' in row[0]:
                    item['Weight'] = row[-1].text.strip()
                elif 'Produced' in row[0]:
                    item['ProductionYears'] = row[-1].text.strip()
                elif 'Pieces' in row[0]:
                    item['PartsCount'] = row[-1].text.strip()
                elif 'LEGO' in row[0] and 'number' in row[0]:
                    item['LegoID'] = row[-1].text.strip()
                elif 'Barcodes' in row[0]:
                    data = row[-1].text.splitlines()
                    for line in data:
                        if line.strip():
                            if 'UPC' in line:
                                try:
                                    item['UPC_Barcode'] = line.split(':')[1].strip()
                                except Exception:
                                    item['UPC_Barcode'] = line.strip()
                            elif 'EAN' in line:
                                try:
                                    item['EAN_Barcode'] = line.split(':')[1].strip()
                                except Exception:
                                    item['EAN_Barcode'] = line.strip()
                elif 'Minifigs' in row[0]:
                    minifigs_link = f'https://brickset.com/minifigs/in-{item["ItemNumber"]}'

            set_image = soup.find('img', {'src': re.compile(r'images\.brickset\.com/sets/images/')})
            if set_image:
                item['ImageLinks'] = set_image['src']

            pieces_link = f'https://brickset.com/inventories/{item["ItemNumber"]}'

            yield scrapy.Request(url=pieces_link, callback=self.parse_set_parts,
                                 meta={'item': item,
                                       'minifigs_link': minifigs_link},
                                 headers=self.headers1)

    def parse_set_parts(self, response):
        item = response.meta['item']
        minifigs_link = response.meta['minifigs_link']

        if response.status != 200:
            self.logger.info(f'ERROR RESPONSE parse_set_parts of link: {response.request.url}, STATUS <{response.status}>')
        else:
            soup = BS(response.text, "html.parser")
            item['Parts-Quantity'] = list()
            parts_table = soup.find('tbody')
            for tr in parts_table.find_all('tr'):
                tds = tr.find_all('td')
                item['Parts-Quantity'].append({'ElementID': tds[0].text.strip(),
                                               'DesignID': tds[5].text.strip(),
                                               'Name': tds[6].text.strip(),
                                               'Qty': tds[2].text.strip()})

        if minifigs_link:
            yield scrapy.Request(url=minifigs_link, callback=self.parse_set_minifigs,
                                 meta={'item': item},
                                 headers=self.headers1)
        else:
            yield item

    def parse_set_minifigs(self, response):
        item = response.meta['item']
        if response.status != 200:
            self.logger.info(f'ERROR RESPONSE parse_set_minifigs of link: {response.request.url}, STATUS <{response.status}>')
        else:
            soup = BS(response.text, "html.parser")
            if not soup.find('section', {'class': 'minifiglist'}):
                self.logger.info(f'ERROR2 GET set minifigs LINK: {response.request.url}, STATUS <{response.status}>')

            item['Minifigs-Quantity'] = list()
            for minifig in soup.find('section', {'class': 'minifiglist'}).find_all('article'):
                qty = minifig.find('div', {'class': 'qty'}).text.strip('x')
                min_text = minifig.find('div', {'class': 'meta'}).find('h1').text
                item['Minifigs-Quantity'].append({'Minifig number': min_text.split(':')[0],
                                               'Name': min_text.split(':')[1],
                                               'Qty': qty})

        yield item
