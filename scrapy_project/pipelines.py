# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import csv
import json
from datetime import datetime
from scrapy.exporters import CsvItemExporter


class ScrapyProjectPipeline:
    def open_spider(self, spider):
        file_name = f'{spider.settings.get("DATA_FILE_PATH")}/{spider.name}_{datetime.now().strftime("%Y-%m-%d_%H.%M.%S")}'
        data_file_name = file_name + '.json'
        self.data_file = open(data_file_name, 'w', encoding='utf-8')
        self.data_file.write("[")

        file_path_producer = f'{file_name}.csv'
        self.file = open(file_path_producer, 'wb')
        # file_path_producer = f'{spider.settings.get("DATA_FILE_PATH")}/Total_Partners.csv'
        # self.file = open(file_path_producer, 'ab')
        self.exporter = CsvItemExporter(self.file, encoding='utf-8-sig', quoting=csv.QUOTE_ALL)
        self.exporter.start_exporting()

    def close_spider(self, spider):
        self.data_file.write("]")
        self.data_file.close()

        self.exporter.finish_exporting()
        self.file.close()

    def process_item(self, item, spider):
        self.data_file.write(json.dumps(dict(item)) + ",\n")  # writing content in output file.

        if 'Parts-Quantity' in item.keys() and item['Parts-Quantity']:
            item['Parts-Quantity'] = json.dumps(item['Parts-Quantity'])
        if 'Minifigs-Quantity' in item.keys() and item['Minifigs-Quantity']:
            item['Minifigs-Quantity'] = json.dumps(item['Minifigs-Quantity'])
        if 'part_json' in item.keys() and item['part_json']:
            item['part_json'] = json.dumps(item['part_json'])

        self.exporter.export_item(item)
        return item
