# import needed libraries
import csv
import json
import re
import time

import requests
from bs4 import BeautifulSoup as BS
import scrapy
from scrapy_project.spiders.base_spider import BaseSpider


def cleanHtml(raw_txt):
    clean_txt = raw_txt
    clean_txt = re.sub(r"[\s\t\n\r]+", ' ', clean_txt).strip()
    return clean_txt


def get_parts_data():
    with open('brickowl_parts_fullData.csv', 'r', encoding='utf-8-sig') as csvfile:
        reader = csv.DictReader(csvfile)
        rows = []
        for row in reader:
            if row['ElementID'].strip():
                rows.append(row)
    return rows


def parse_edits(item, edits_response, logger):
    try:
        data = json.loads(edits_response.text)
        for i in data:
            if i['command'] == 'insert':
                edits_soup = BS(i['data'], "html.parser")
                item['edits'] = {}

                details_tab = edits_soup.find('div', {'id': 'edit-details'})
                if details_tab:
                    base_name_tag = details_tab.find('input', {'name': 'main_details_base_name', 'value': True})
                    if base_name_tag:
                        item['BaseName'] = base_name_tag['value']

                    item['edits']['details'] = {}
                    rows = details_tab.find_all('p')
                    for p in rows:
                        k = p.find('strong')
                        if k and ':' in p.text:
                            key = k.text.strip().strip(':').strip()
                            k.decompose()
                            item['edits']['details'][key] = item['edits']['details'].get(key, [])
                            item['edits']['details'][key].append(p.text)

                dimensions_tab = edits_soup.find('div', {'id': 'edit-dimensions'})
                if dimensions_tab:
                    weight = dimensions_tab.find('input', {'name': 'main_dimensions_weight', 'value': True})
                    if weight:
                        item['Weight'] = weight['value']
                    else:
                        item['Weight'] = ''

                IDs_tab = edits_soup.find('div', {'id': 'edit-ids'})
                if IDs_tab:
                    item['edits']['ID_Numbers'] = {}
                    rows = [tr.find_all('td') for tr in IDs_tab.find('table').find_all('tr') if
                            len(tr.find_all('td')) >= 2]
                    for row in rows:
                        k = row[0].text.strip()
                        v = row[1].find('input', {'value': True})['value'] if row[1].find('input', {'value': True}) else ''
                        if k and v:
                            item['edits']['ID_Numbers'][k] = item['edits']['ID_Numbers'].get(k, [])
                            item['edits']['ID_Numbers'][k].append(v)

                    for k, v in item['edits']['ID_Numbers'].items():
                        if 'Brick Owl ID' in k:
                            item['BrickOwlID'] = v

                        elif 'Brickset Internal ID' in k:
                            item['BricksetInternalID'] = v

                        elif 'Brickset Set Number' in k:
                            item['BricksetID'] = v

                        elif 'BL ID' in k:
                            if 'Unable to display' not in v:
                                item['BrickLinkID'] = v

                        elif 'Lego Item No' in k:
                            item['LegoID'] = v

                        elif 'Rebrickable Part Num' in k:
                            item['RebrickableID'] = v

                tags_tab = edits_soup.find('div', {'id': 'edit-tags'})
                if tags_tab:
                    item['edits']['tags'] = []
                    rows = [tr.find_all('td') for tr in tags_tab.find('table').find_all('tr') if
                            len(tr.find_all('td')) >= 1]
                    for row in rows:
                        k = row[0].find('input', {'value': True})['value'].strip() if row[0].find('input', {'value': True}) else ''
                        if k:
                            item['edits']['tags'].append(k)

    except Exception as e:
        logger.info(f'ERROR: Parse set edits: {e}')


def search_parts_ElementID(BOID, BOLink, rows):
    try:
        for row in rows:
            if BOID.strip() == row['BrickOwlID'].strip() or BOLink.strip() == row['BrickOwlLink'].strip():
                return row['ElementID']
        raise Exception
    except Exception:
        return ''


class Spider(BaseSpider):
    # spider name; used for calling spider
    name = 'brickowl_sets'
    domain = 'https://www.brickowl.com'

    done_links = list()

    part_list = get_parts_data()

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
        # 'CLOSESPIDER_ITEMCOUNT': 300,
    }

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36',
        'Cookie': 'SSESS96636da61f62e4e8dc28f1bac0edf597=kx-ChncmJBJ6ZIe28AHc5VzLd6LpyJ8Dv23RWuGPYyQ',
    }

    headers2 = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36',
        'Cookie': 'SSESS96636da61f62e4e8dc28f1bac0edf597=oSZ71bzcdmfKYdxwW-CcMFtR_-93cUK0Wzejr3VL4kw; _ga=GA1.1.2111869156.1686762143; _ga_8JK085KL5V=GS1.1.1686762143.1.1.1686763064.0.0.0',
    }

    def start_requests(self):
        page = 1
        page_link = f'https://www.brickowl.com/catalog/lego-sets?page={page}'
        yield scrapy.Request(url=page_link, callback=self.parse,
                             meta={'page': page},
                             headers=self.headers)

    def parse(self, response, **kwargs):
        page = response.meta['page']

        if response.status != 200:
            self.logger.info(f'ERROR GETTING MAIN PAGE #{page} RESPONSE STATUS <{response.status}>')
        else:
            soup = BS(response.text, "html.parser")
            sets = [lego_set.find('a', {'href': True})['href'] for lego_set in soup.find_all('li', {'data-boid': True}) if
                     lego_set.find('a', {'href': True})]

            self.logger.info(f'Number of sets in page #{page} = {len(sets)}')

            ifile = open('brickowl_sets_links.txt', 'a', encoding='utf-8')

            for set_link in sets:
                ifile.write(f'Page:{str(page).zfill(5)}; ' + self.domain + set_link + "\n")

                if set_link in self.done_links:
                    continue

                self.done_links.append(set_link)
                yield scrapy.Request(url=self.domain + set_link, callback=self.parse_set_page,
                                     cookies={'SSESS96636da61f62e4e8dc28f1bac0edf597': 'kx-ChncmJBJ6ZIe28AHc5VzLd6LpyJ8Dv23RWuGPYyQ'},
                                     headers=self.headers)

            ifile.close()

            # follow next page
            next_page = soup.find('a', {'href': re.compile('page='), 'title': 'Next'})
            if next_page:
                next_page_link = self.domain + next_page['href']
                page = re.search(r"page=(\d+)", next_page_link).group(1)
                yield scrapy.Request(url=next_page_link, callback=self.parse,
                                     meta={'page': page},
                                     headers=self.headers)

    def parse_set_page(self, response):
        if response.status != 200:
            self.logger.info(f'ERROR GET set LINK: {response.request.url}, STATUS <{response.status}>')
        else:
            soup = BS(response.text, "html.parser")
            if not soup.find('h1', {'id': 'page-title'}):
                self.logger.info(f'ERROR2 GET set LINK: {response.request.url}, STATUS <{response.status}>')

            # Initialize item
            item = dict()
            for k in self.field_names:
                item[k] = ''

            item['BrickOwlLink'] = response.request.url
            item['Name'] = soup.find('h1', {'id': 'page-title'}).text
            item['BrickOwlName'] = item['Name']

            item_table = soup.find('table', {'class': 'item-right-table'})
            table_rows = [tr.find_all('td') for tr in item_table.find_all('tr') if len(tr.find_all('td')) == 2]
            for row in table_rows:
                if 'Set Number' in row[0].text:
                    item['ItemNumber'] = row[1].text
                elif 'BOID' in row[0].text:
                    item['BrickOwlID'] = row[1].text
                elif 'Piece Count' in row[0].text:
                    item['PartsCount'] = row[-1].text.strip()
                elif 'UPC ' in row[0].text:
                    item['UPC_Barcode'] = row[1].text
                elif 'EAN ' in row[0].text:
                    item['EAN_Barcode'] = row[1].text
                elif 'LDraw ID' in row[0].text:
                    item['LDrawID'] = row[1].text
                elif 'Year Released' in row[0].text:
                    item['ProductionYears'] = row[1].text
                elif 'Item No' in row[0].text:
                    item['ElementID'] = row[1].text
                elif 'Design ID' in row[0].text:
                    item['DesignID'] = row[1].text

            if not item['ProductionYears']:
                for tr in item_table.find_all('tr'):
                    if re.search(r"\d{4}\s*to\s*\d{4}", tr.text):
                        item['ProductionYears'] = tr.text
                        break

            part_images = soup.find('div', {'class': 'product-img-box'}).find_all('a', {'href': re.compile('img\.brickowl\.com/files')}) if soup.find('div', {'class': 'product-img-box'}) else []
            item['ImageLinks'] = [link['href'] for link in part_images]

            if soup.find('div', {'id': 'item-right'}):
                for div in soup.find('div', {'id': 'item-right'}):
                    if "Lego name:" in div.text:
                        item['LegoName'] = div.text.split('Lego name:')[1].rsplit('(')[0]
                        break

            categories = soup.find_all('span', {'property': "itemListElement"})
            if categories:
                item['Categories'] = [cat.text for cat in categories if cat.text not in ['Catalog', 'LEGO Sets']]

            dimensions_div = item_table.find('div', {'class': 'dimseg-outer'})
            if dimensions_div:
                item['Dimensions'] = dimensions_div.text

            scripts = soup.find_all('script')
            for script in scripts:
                if '"item_id":"' in script.text:
                    item_id = script.text.split('"item_id":"')[1].split('"')[0].strip()
                    break
            else:
                item_id = None

            if item_id:
                edits_link = f'https://www.brickowl.com/ajax/edit/{item_id}'
                for trial in range(3):
                    try:
                        edits_response = requests.post(edits_link, headers=self.headers2)
                        parse_edits(item, edits_response, self.logger)
                        break
                    except Exception as e:
                        self.logger.info(f'RETRY GET set edits {edits_link}, {e}')
                        time.sleep(2)
                else:
                    self.logger.info(f'FAILED GET set edits {edits_link}')

            if soup.find('div', id='item-right') and soup.find('div', id='item-right').find('a', {'href': re.compile('instructions', re.IGNORECASE)}):
                set_instructions_link = item['BrickOwlLink'] + '-instructions/viewer'
            else:
                set_instructions_link = ''

            set_inventory_link = item['BrickOwlLink'] + '/inventory'

            yield scrapy.Request(url=set_inventory_link, callback=self.parse_inventory,
                                 meta={'item': item,
                                       'set_instructions_link': set_instructions_link},
                                 headers=self.headers)

    def parse_inventory(self, response):
        item = response.meta['item']
        set_instructions_link = response.meta['set_instructions_link']

        if response.status != 200:
            self.logger.info(f'ERROR RESPONSE parse_inventory of link: {response.request.url}, STATUS <{response.status}>')
        else:
            soup = BS(response.text, "html.parser")
            try:
                set_parts = soup.find('ul', {'class': 'category-grid'}).find_all('li', recursive=False)
                self.logger.info(f'FOUND inventory parts OF {response.request.url} = {len(set_parts)}')
                item['Parts-Quantity'] = list()
                item['Minifigs-Quantity'] = list()
            except Exception as e:
                if not 'No Inventory' in soup.text:
                    self.logger.info(f'ERROR GET inventory of link: {response.request.url}, {e}')
                set_parts = []
            for part in set_parts:
                name = part.find('h2', {'class': 'category-item-name'}).text if part.find('h2', {'class': 'category-item-name'}) else ''
                link = part.find('h2', {'class': 'category-item-name'}).find('a', {'href':re.compile('/catalog/')})['href'] if part.find('h2', {'class': 'category-item-name'}) and part.find('h2', {'class': 'category-item-name'}).find('a', {'href':re.compile('/catalog/')}) else ''
                if link:
                    link = self.domain + link

                if '-set-' in link:
                    if link not in self.done_links:
                        self.done_links.append(link)
                        yield scrapy.Request(url=link, callback=self.parse_set_page,
                                             cookies={'SSESS96636da61f62e4e8dc28f1bac0edf597': 'kx-ChncmJBJ6ZIe28AHc5VzLd6LpyJ8Dv23RWuGPYyQ'},
                                             headers=self.headers)

                _id = part['data-boid']
                qty_text = part.find('div', {'class': 'cat-item-bot'}).text if part.find('div', {'class': 'cat-item-bot'}) else ''
                if qty_text and re.search(r"Qty:\s?(\d+)", qty_text):
                    qty = re.search(r"Qty:\s?(\d+)", qty_text).group(1)
                else:
                    qty = ''
                if 'minifigure' in part.text.lower():
                    part_key = 'Minifigs-Quantity'
                    element_id = ''
                else:
                    part_key = 'Parts-Quantity'
                    element_id = search_parts_ElementID(_id, link, self.part_list)

                item[part_key].append({'ElementID': element_id,
                                       'BOID': _id,
                                       'Name': name,
                                       'BrickOwlLink': link,
                                       'Qty': qty})

        if set_instructions_link:
            yield scrapy.Request(url=set_instructions_link, callback=self.parse_set_instructions,
                                 meta={'item': item},
                                 headers=self.headers)
        else:
            yield item

    def parse_set_instructions(self, response):
        item = response.meta['item']
        if response.status == 404:
            self.logger.info(f'Instructions NOT FOUND of Set: {item["BrickOwlLink"]}')
        elif response.status == 200:
            soup = BS(response.text, "html.parser")
            instructions_link = soup.find('a', {'href': re.compile(r'\.pdf', re.IGNORECASE)})['href'] if soup.find('a', {'href': re.compile('\.pdf', re.IGNORECASE)}) else ''
            if instructions_link:
                item['Instructions'] = instructions_link
        else:
            self.logger.info(f'ERROR: Instructions NOT FOUND of Set : {response.request.url}, STATUS <{response.status}>')

        yield item
