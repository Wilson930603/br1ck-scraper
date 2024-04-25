# import needed libraries
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


def parse_edits(item, edits_response, logger):
    try:
        data = json.loads(edits_response.text)
        for i in data:
            if i['command'] == 'insert':
                edits_soup = BS(i['data'], "html.parser")
                item['edits'] = {}

                details_tab = edits_soup.find('div', {'id': 'edit-details'})
                if details_tab:
                    item['edits']['details'] = {}
                    rows = details_tab.find_all('p')
                    for p in rows:
                        k = p.find('strong')
                        if k:
                            key = k.text.strip().strip(':').strip()
                            k.decompose()
                            item['edits']['details'][key] = item['edits']['details'].get(key, [])
                            item['edits']['details'][key].append(p.text)

                    for k, v in item['edits']['details'].items():
                        if 'LDraw' in k and 'Name' in k:
                            item['LDrawName'] = item['edits']['details'][k]

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

                attributes_tab = edits_soup.find('div', {'id': 'edit-attributes'})
                if attributes_tab:
                    item['edits']['attributes'] = {}
                    fields = attributes_tab.find_all('div', {'class': 'form-item'})
                    for field in fields:
                        k = field.find('label').text.strip() if field.find('label') else ''
                        if field.find('input'):
                            v = field.find('input', {'value': True})['value'] if field.find('input', {'value': True}) else ''
                        elif field.find('select'):
                            v = field.find('option', {'value': True, 'selected': True})['value'] if field.find('option', {'value': True, 'selected': True}) else ''
                        else:
                            v = ''
                        if k and v:
                            item['edits']['attributes'][k] = v

                dimensions_tab = edits_soup.find('div', {'id': 'edit-dimensions'})
                if dimensions_tab:
                    weight = dimensions_tab.find('input', {'name': 'main_dimensions_weight', 'value': True})
                    if weight:
                        item['Weight'] = weight['value']
                    else:
                        item['Weight'] = ''

                    item['edits']['dimensions'] = {}
                    labels = dimensions_tab.find_all('label')
                    for label in labels:
                        k = label.text.strip()
                        next_div = label.find_next('div')
                        for inp in next_div.find_all('input', {'value': True}):
                            inp.replace_with(inp['value'])
                        v = cleanHtml(next_div.text)
                        if k and v:
                            item['edits']['dimensions'][k] = v

                            # if 'Weight' in k.title():
                            #     item['Weight'] = v

                categories_tab = edits_soup.find('div', {'id': 'edit-taxonomy'})
                if categories_tab:
                    item['edits']['categories'] = {}
                    fields = categories_tab.find_all('div', {'class': 'form-item'})
                    for field in fields:
                        k = field.find('label').text.strip() if field.find('label') else ''
                        if field.find('input'):
                            v = field.find('input', {'value': True})['value'] if field.find('input', {'value': True}) else ''
                        elif field.find('select'):
                            v = field.find('option', {'value': True, 'selected': True})['value'] if field.find('option', {'value': True, 'selected': True}) else ''
                        else:
                            v = ''

                        if k and v:
                            item['edits']['categories'][k] = v

                other_tab = edits_soup.find('div', {'id': 'edit-other'})
                if other_tab:
                    item['edits']['other'] = {}
                    fields = other_tab.find_all('div', {'class': 'form-item'})
                    for field in fields:
                        k = field.find('label').text.strip() if field.find('label') else ''
                        if field.find('input'):
                            v = field.find('input', {'value': True})['value'] if field.find('input', {'value': True}) else ''
                        elif field.find('select'):
                            v = field.find('option', {'value': True, 'selected': True})['value'] if field.find('option', {'value': True, 'selected': True}) else ''
                        else:
                            v = ''
                        if k and v:
                            item['edits']['other'][k] = v

                            if 'Type' in k.title():
                                item['Type'] = v

    except Exception as e:
        logger.info(f'ERROR: Parse part edits: {e}, {edits_response.text}')


class Spider(BaseSpider):
    # spider name; used for calling spider
    name = 'brickowl'
    domain = 'https://www.brickowl.com'

    done_links = list()

    field_names = ['ElementID', 'DesignID', 'Name', 'Description', 'LDrawName', 'LegoName', 'BrickOwlName',
                   'BricksetName', 'RebrickableName', 'BrickLinkName', 'ColorID', 'CategoryID', 'SubCategoryID',
                   'Categories', 'Tags', 'ProductionYears', 'Weight', 'Dimensions', 'IdenticalParts', 'Sets', 'Type',
                   'PartOf', 'Alternates', 'RebrickableID', 'BrickLinkID', 'BrickOwlID', 'LegoID', 'LDrawID',
                   'ImageLinks', 'RebricableLink', 'BricklinkLink', 'BrickOwlLink', 'BricksetLink', 'LDrawLink', 'Quantity']

    update_from_parent = ['DesignID', 'ProductionYears', 'Weight', 'Dimensions']

    custom_settings = {
        'DOWNLOAD_DELAY': 0.5,
        'CONCURRENT_REQUESTS': 4,
    }

    headers1 = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36',
        'Cookie': 'SSESS96636da61f62e4e8dc28f1bac0edf597=kx-ChncmJBJ6ZIe28AHc5VzLd6LpyJ8Dv23RWuGPYyQ',
    }

    headers2 = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36',
        'Cookie': 'SSESS96636da61f62e4e8dc28f1bac0edf597=oSZ71bzcdmfKYdxwW-CcMFtR_-93cUK0Wzejr3VL4kw; _ga=GA1.1.2111869156.1686762143; _ga_8JK085KL5V=GS1.1.1686762143.1.1.1686763064.0.0.0',
    }

    def start_requests(self):
        page = 1
        page_link = f'https://www.brickowl.com/catalog/lego-parts?page={page}'
        yield scrapy.Request(url=page_link, callback=self.parse,
                             meta={'page': page},
                             headers=self.headers1)

    def parse(self, response, **kwargs):
        page = response.meta['page']

        if response.status != 200:
            self.logger.info(f'ERROR GETTING MAIN PAGE #{page} RESPONSE STATUS <{response.status}>')
        else:
            soup = BS(response.text, "html.parser")
            parts = [part.find('a', {'href': True})['href'] for part in soup.find_all('li', {'data-boid': True}) if
                     part.find('a', {'href': True})]

            self.logger.info(f'Number of parts in page #{page} = {len(parts)}')

            ifile = open('brickowl_part_links.txt', 'a', encoding='utf-8')

            for part_link in parts:
                ifile.write(f'Page:{str(page).zfill(5)}; ' + self.domain + part_link + "\n")

                if part_link in self.done_links:
                    continue

                self.done_links.append(part_link)
                yield scrapy.Request(url=self.domain + part_link, callback=self.parse_part_page,
                                     cookies={'SSESS96636da61f62e4e8dc28f1bac0edf597': 'kx-ChncmJBJ6ZIe28AHc5VzLd6LpyJ8Dv23RWuGPYyQ'},
                                     meta={'parent_item': None},
                                     headers=self.headers1)

            ifile.close()

            # follow next page
            next_page = soup.find('a', {'href': re.compile('page='), 'title': 'Next'})
            if next_page:
                next_page_link = self.domain + next_page['href']
                page = re.search(r"page=(\d+)", next_page_link).group(1)
                yield scrapy.Request(url=next_page_link, callback=self.parse,
                                     meta={'page': page},
                                     headers=self.headers1)

    def parse_part_page(self, response):
        parent_item = response.meta['parent_item']

        if response.status != 200:
            self.logger.info(f'ERROR GET part LINK: {response.request.url}, STATUS <{response.status}>')
        else:
            soup = BS(response.text, "html.parser")
            if not soup.find('h1', {'id': 'page-title'}):
                self.logger.info(f'ERROR2 GET part LINK: {response.request.url}, STATUS <{response.status}>')

            # Initialize item
            item = dict()
            for k in self.field_names:
                item[k] = ''

            item['BrickOwlLink'] = response.request.url
            item['Name'] = soup.find('h1', {'id': 'page-title'}).text

            item_table = soup.find('table', {'class': 'item-right-table'})
            table_rows = [tr.find_all('td') for tr in item_table.find_all('tr') if len(tr.find_all('td')) == 2]
            for row in table_rows:
                if 'LDraw ID' in row[0].text:
                    item['LDrawID'] = row[1].text
                elif 'Year Released' in row[0].text:
                    item['ProductionYears'] = row[1].text
                elif 'Item No' in row[0].text:
                    item['ElementID'] = row[1].text
                elif 'Design ID' in row[0].text:
                    item['DesignID'] = row[1].text
                elif 'BOID' in row[0].text:
                    item['BrickOwlID'] = row[1].text

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

            item['BrickOwlName'] = item['Name']

            categories = soup.find_all('span', {'property': "itemListElement"})
            if categories:
                item['Categories'] = [cat.text for cat in categories if cat.text not in ['Catalog', 'LEGO Parts']]

            tags = soup.find('ul', {'id': 'tags'}).find_all('li') if soup.find('ul', {'id': 'tags'}) else None
            if tags:
                item['Tags'] = cleanHtml(', '.join([li.text for li in tags]))

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
                        self.logger.info(f'RETRY GET part edits {edits_link}, {e}')
                        time.sleep(2)
                else:
                    self.logger.info(f'FAILED GET part edits {edits_link}')

            if parent_item:
                for key in self.update_from_parent:
                    if not item[key] and parent_item[key]:
                        item[key] = parent_item[key]

            yield item

            if item_id:
                parent_item = {x: item[x] for x in self.update_from_parent}

                colors_tab = soup.find('li', {'id': 'tab-colors'})
                if colors_tab:
                    colors_link = f'https://www.brickowl.com/ajax/dt_child?item_id={item_id}&type=color'
                    yield scrapy.Request(url=colors_link,
                                         callback=self.parse_variants,
                                         meta={'parent_item': parent_item},
                                         headers=self.headers1)

                decorated_tab = soup.find('li', {'id': 'tab-decorated'})
                if decorated_tab:
                    decorated_link = f'https://www.brickowl.com/ajax/dt_child?item_id={item_id}&type=decorated'
                    yield scrapy.Request(url=decorated_link,
                                         callback=self.parse_variants,
                                         meta={'parent_item': parent_item},
                                         headers=self.headers1)

                stickered_tab = soup.find('li', {'id': 'tab-stickered'})
                if stickered_tab:
                    stickered_link = f'https://www.brickowl.com/ajax/dt_child?item_id={item_id}&type=stickered'
                    yield scrapy.Request(url=stickered_link,
                                         callback=self.parse_variants,
                                         meta={'parent_item': parent_item},
                                         headers=self.headers1)

    def parse_variants(self, response):
        parent_item = response.meta['parent_item']
        if response.status != 200:
            self.logger.info(f'ERROR GET variants LINK: {response.request.url}, STATUS <{response.status}>')
        else:
            try:
                variants = json.loads(response.text)['aaData']
                if len(variants) > 0:
                    self.logger.info(f'FOUND variants OF {response.request.url} = {len(variants)}')

                    for variant in variants:
                        try:
                            variant_soup = BS(variant[1], "html.parser")
                            variant_link = variant_soup.find('a', {'href': True})['href']

                            if variant_link in self.done_links:
                                continue

                            self.done_links.append(variant_link)
                            yield scrapy.Request(url=self.domain + variant_link,
                                                 callback=self.parse_part_page,
                                                 cookies={'SSESS96636da61f62e4e8dc28f1bac0edf597': 'kx-ChncmJBJ6ZIe28AHc5VzLd6LpyJ8Dv23RWuGPYyQ'},
                                                 meta={'parent_item': parent_item},
                                                 headers=self.headers1)
                        except Exception as e:
                            self.logger.info(f'iteration variant {variant} error {e}')
            except Exception as err:
                self.logger.info(f'ERROR GET variants JSON: {response.request.url}, err <{err}>')
