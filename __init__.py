import json
import logging
import re
import azure.functions as func
import json
import requests
URL_REGEX_PATTERN = r"(?:(?:https?|ftp|hxxps?):\/\/|www\[?\.\]?|ftp\[?\.\]?)(?:(?:[-A-Z0-9_@:%]+?\[?\.\]?)+" \
                    r"[-A-Z0-9]+|(?:[-A-Z0-9_:%]+?@0x[A-Z0-9]{8}))(?::[0-9]+)?\.?(?:(?:\/?|\?)[-A-Z0-9+&@#\/" \
                    r"%=~_$?!:,*'\|\[\].\(\);\^\{\}]*[A-Z0-9+&@#\/%=~_$(*\-\?\|!:\[\]\{\}])?"
RTP_URL = 'https://staging-rpd.slashnext.cloud/api/v1/urls/repute'

def extract_urls_json(data):
    list_of_urls = []
    url_pattern_regex = re.compile(URL_REGEX_PATTERN, re.IGNORECASE)
    if 'tables' in data:
        if 'rows' in data['tables'][0]:
            for s_row in  (data['tables'][0]['rows']):
                for s_col in s_row:
                    if isinstance(s_col,str):
                        data =url_pattern_regex.findall(s_col)
                        if data:
                            list_of_urls.append(data[0])
    return list_of_urls

def split_list(data, size=10000):
    sub_lists = []
    for i in range(0,len(data),size):
        sub_lists.append(data[i:i+size])
    return sub_lists


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    URL_REGEX_PATTERN = r"(?:(?:https?|ftp|hxxps?):\/\/|www\[?\.\]?|ftp\[?\.\]?)(?:(?:[-A-Z0-9_@:%]+?\[?\.\]?)+" \
                        r"[-A-Z0-9]+|(?:[-A-Z0-9_:%]+?@0x[A-Z0-9]{8}))(?::[0-9]+)?\.?(?:(?:\/?|\?)[-A-Z0-9+&@#\/" \
                        r"%=~_$?!:,*'\|\[\].\(\);\^\{\}]*[A-Z0-9+&@#\/%=~_$(*\-\?\|!:\[\]\{\}])?"
    try:
        req_body = req.get_json()
        f_data = extract_urls_json(req_body)
        f_data = split_list(f_data,100)
         
    except Exception as err:
            logging.info('f{err}')
            return func.HttpResponse(str(err),status_code=500)
    logging.info(str(f_data))
    return func.HttpResponse(json.dumps(f_data),status_code=200)
