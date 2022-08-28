import bs4
from selenium import webdriver
import time
from influxclient import ic
import logging
import configparser
import sys
import json
from pythonjsonlogger import jsonlogger
from datetime import datetime

class CustomJsonFormatter(jsonlogger.JsonFormatter):
    def add_fields(self, log_record, record, message_dict):
        super(CustomJsonFormatter, self).add_fields(log_record, record, message_dict)
        super(CustomJsonFormatter, self).add_fields(log_record, record, message_dict)
        if not log_record.get('timestamp'):
            now = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            log_record['timestamp'] = now
        if log_record.get('level'):
            log_record['level'] = log_record['level'].upper()
        else:
            log_record['level'] = record.levelname


def log_init(log_index):
    global logger
    logger = logging.getLogger(log_index)
    logger.setLevel(logging.DEBUG)
    logHandler = logging.StreamHandler()
    formatter = CustomJsonFormatter('%(timestamp)s %(level)s %(name)s %(message)s')
    logHandler.setFormatter(formatter)
    logger.addHandler(logHandler)
    return logger


def get_source(url):
    try:
        options = webdriver.firefox.options.Options()
        options.headless = True
        driver = webdriver.Firefox(options=options)
        driver.get(url)
        time.sleep(3)
        page_source = driver.page_source
        logger.info(f"Obtained page source for url {url}, len {len(page_source)}",
                extra={
                    "status": "success",
                    "code":"get_source",
                    "data":
                        {"scrape_url": url,
                        "len": len(page_source)}
                })
        driver.close()
        return page_source

    except Exception as E:
        logger.error(f"Error while obtaining page source",
                extra={
                    "status": "error",
                    "code": "get_source",
                    "data":
                        {"exception": str(E),
                        }
                })
        sys.exit()


def header_processor(bs_data):
    hh = bs_data.findAll("div", class_="graph-headers")
    if len(hh) < 1:
        logger.error("Empty headers",
                extra={
                    "status":"error",
                    "code": "empty_headers",
                    "data": {}
                })
        sys.exit()
    if len(hh) > 1:
        logger.error("Too many headers",
                extra={
                    "status":"error",
                    "code": "too_many_headers",
                    "data": {"headers": len(hh)}
                })
        sys.exit()
    headers_tags = hh[0].findAll("div", class_="graph-header")
    headers_values = []
    for i in headers_tags:
        headers_values.append(i.text)
    logger.error("Completed headers processing",
                 extra={
                     "status": "success",
                     "code": "headers_processing",
                     "data": {"headers": json.dumps(headers_values)}
                 })
    return headers_values


def row_processor(bs_data, headers):
    rr = bs_data.findAll("div", class_="graph-row")
    if not len(rr) > 0:
        logger.error("Not enough data rows",
                extra={
                    "status":"error",
                    "code": "not_enough_rows",
                    "data": {"rows_len": len(rr)}
                })
        sys.exit()
    rows = []
    for i in rr:
        row = {}
        cells = i.findAll("div", class_="graph-column")
        assert len(cells) == len(headers)
        for num, j in enumerate(cells):
            row[headers[num]] = j.text
        rows.append(row)
    logger.info("Processed rows",
                 extra={
                     "status": "success",
                     "code": "rows_processing",
                     "data": {"rows": json.dumps(rows)}
                 })
    return rows


def get_data(url):
    page_source = get_source(url)
    bs = bs4.BeautifulSoup(page_source, "lxml")
    content_table = bs.findAll("div", id="dailyIndices")
    if len(content_table) < 1:
        logger.error("Empty index table",
                extra={
                    "status":"error",
                    "code": "empty_index_table",
                    "data": {"content_table": json.dumps(content_table)}
                })
        sys.exit()
    headers = header_processor(content_table[0])
    _data = row_processor(content_table[0], headers)
    if not _data:
        logger.error("Failed to obtain data",
                extra={
                    "status":"error",
                    "code": "data_fetch_fail"
                })
    else:
        logger.info("Data extraction compete",
                    extra={
                        "status": "success",
                        "code": "data extraction",
                        "data": {"_data": json.dumps(_data)}
                    })

    return _data


def data_typer(dd):
    normalised_data = []
    for i in dd:
        new_i = {}
        for k in i.keys():
            if 'CITY' in k:
                new_i['city'] = i[k].lower().strip()
            else:
                v_norm = float(i[k].replace('%', '').strip())
                k_norm = k.lower().replace('\'', '').replace(' ', '_')
                new_i[k_norm] = v_norm
        normalised_data.append(new_i)
    logger.info(f"Normalised data: {json.dumps(normalised_data)}",
                extra={
                    "status": "success",
                    "code": "data_normalizing",
                    "data": json.dumps(normalised_data)
                })
    return normalised_data


if __name__ == '__main__':
    try:
        CONFIG_FILE = sys.argv[1]
        config = configparser.ConfigParser()
        config.read(CONFIG_FILE)
    except IndexError:
        sys.exit("Config file not supplied")
    logger = log_init(config['DEFAULT']['ES_INDEX'])
    dd = get_data(config['DEFAULT']['URL'])
    norm_dd = data_typer(dd)
    ici = ic(config['DEFAULT']['INFLUX_URL'],
             config['DEFAULT']['TOKEN'],
             config['DEFAULT']['ORG'],
             config['DEFAULT']['BUCKET']
             )
    ici.put_data_in_bucket(norm_dd,  config['DEFAULT']['POINT_NAME'])
    logger.info("Scrape run competed successfully",
                extra={
                    "status": "success",
                    "code": "scraping_status",
                    "data": {}
                })