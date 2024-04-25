# LEGO scraping project
Scrape list of LEGO Cataloge websites and save Its Lego parts and sets data to csv files.
## Websites:
- https://www.brickowl.com
  - https://www.brickowl.com/catalog/lego-parts
  - https://www.brickowl.com/catalog/lego-sets
- https://brickset.com
  - https://brickset.com/parts
  - https://brickset.com/sets
- https://www.rebrickable.com
  - https://rebrickable.com/parts/
  - https://rebrickable.com/sets/
- https://www.bricklink.com/
  - https://www.bricklink.com/catalogTree.asp?itemType=P
  - https://www.bricklink.com/catalogTree.asp?itemType=S

#### * Be noted that rebrickable and brickLink spiders need API keys, please update API variables with valid API keys
#### * Full scrape of BrickOwl parts is required before scrape of BrickOwl Sets as the parts data file is used in "get_parts_data" function.
#### * Refer to below bricklink custom variables section

## Installation:
To run this project you need python (version>=3.8) installed a long with scrapy framework.
Download and install python using https://www.python.org/downloads.

Install prerequisite libraries from requirements.txt file,
  Navigate to project directory and run:
```bash
pip install -r requirements.txt
```

## Run spider:
Open a terminal or command prompt and navigate to the directory where the project is saved,

Run search spider:
```bash
scrapy crawl $spider_name  # ['brickowl', 'brickowl_sets', 'brickset', 'brickset_sets',
                           #  'rebrickable', 'rebrickable_sets', 'bricklink', 'bricklink_sets',
                           #  bricklink_part_numbers, bricklink_set_numbers, bricklink_dimension]
```

The spider will start scraping and save the extracted data to a CSV file.

### bricklink custom variables
  1- As Bricklink API has no endpoint to get part numbers of website available parts and sets, We created a separate spiders "bricklink_part_numbers" & "bricklink_set_numbers" to scrape the website categories pages "https://www.bricklink.com/catalogTree.asp?itemType=P" & "https://www.bricklink.com/catalogTree.asp?itemType=S" then save parts and sets numbers.
      *These spiders use rotating proxy to overcome website IP ban. update proxy file path at spider custom_settings "ROTATING_PROXY_LIST_PATH"
      acquired parts / sets numbers saved at project directory with file names "bricklink_part_numbers.txt" & "bricklink_set_numbers.txt" defined part_numbers_file_path & set_numbers_file_path variables.
  2- Add any number of available API keys to API_KEYS list.
  3- As Bricklink API has limited number of daily API calls (5,000 per key), so bricklink spiders first will respect this limit and not exceed it, it also allows to continue scraping, just update last scraped file path variable to be considered when continue scraping next day.
  4- As Bricklink API didn't return proper parts dimensions, we created a new spider to update scraped parts dimension through scraping the website itself,
     spider bricklink_dimension visits all parts URLs using proxy to update the part dimensions. (Please update "previously_scraped_file_path" variable)


### Project structure

- scrapy_project directory: contains scrapy project configuration files (setting.py, pipeline.py, ...)
- spiders directory: contains project spiders one Class for each product category. 
- output directory: contains output CSV files.
- logs directory: contains spiders log files.

### Output:
Expected CSV output table
location: ouput/spider_name_YYYY-MM-DD_HH.MM.SS.csv
