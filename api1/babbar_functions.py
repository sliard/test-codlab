import configparser
import datetime
import csv
import requests
from requests.structures import CaseInsensitiveDict
import os
import time
import pandas as pd

configfile = 'config.ini'
keyfile = 'keys.cfg'
def api_keys_storage(keyfile):
    config = configparser.ConfigParser()
    config.read(keyfile)
    if not os.path.exists(keyfile):
        api_key = input("Please enter your API Key: ")
        config['DEFAULT']['API_KEY'] = api_key
        with open(keyfile, 'w') as config_file:
            config.write(config_file)
    API_KEY = config.get('DEFAULT', 'API_KEY')

# TODO: ask for the host to check and store it in config.ini file as "host"
# TODO: ask for the API Key and add it in config.ini as "API_KEY"
# TODO: ensure that the config file has a value for "language" and "country" or set the default value in it
# TODO: ask for a specific path (folder) for crawl files, write it in the config.ini file

def configuration_file(configfile, keyfile):
    config = configparser.ConfigParser()
    config.read(configfile)
    api_keys_storage(keyfile)
    host = input("Please enter the host to check: ")
    full_kws = f'{host}_kws.csv'
    today = datetime.date.today()
    last_monday = today - datetime.timedelta(days=today.weekday())
    bks_min = input("Please enter the bks_min value: ")
    bks_max = input("Please enter the bks_max value: ")
    bname_path = os.path.join('data', f'{host}_kws_selected.csv')
    config['DEFAULT']['host'] = host
    config['DEFAULT']['full_kws'] = full_kws
    config['DEFAULT']['today'] = str(today)
    config['DEFAULT']['last_monday'] = str(last_monday)
    config['DEFAULT']['bks_min'] = str(bks_min)
    config['DEFAULT']['bks_max'] = str(bks_max)
    config['DEFAULT']['bname_path'] = bname_path
    if 'language' not in config['DEFAULT']:
        config['DEFAULT']['language'] = 'fr'
    if 'country' not in config['DEFAULT']:
        config['DEFAULT']['country'] = 'FR'

    with open('config.ini', 'w') as config_file:
        config.write(config_file)

# Fetch babbar_keywords for the last monday date with the host in config.ini file as input
if not os.path.exists('config.ini'):
    configuration_file(configfile, keyfile)

config = configparser.ConfigParser()
config.read(configfile)
last_monday = config['DEFAULT']['last_monday']
host = config['DEFAULT']['host']
full_kws = config['DEFAULT']['full_kws']
language = config['DEFAULT']['language']
country = config['DEFAULT']['country']
bname_path = config['DEFAULT']['bname_path']
bks_min = int(config['DEFAULT']['bks_min'])
bks_max = int(config['DEFAULT']['bks_max'])
config.read(keyfile)
api_key = config['DEFAULT']['API_KEY']

def babbar_keywords(host,lang,country,start_date,end_date,API_KEY):
    #define the dates (strings needed)
    start_datetime = datetime.date(int(start_date.split("-")[0]),int(start_date.split("-")[1]),int(start_date.split("-")[2]))
    end_datetime = datetime.date(int(end_date.split("-")[0]),int(end_date.split("-")[1]),int(end_date.split("-")[2]))
    #check the number of days between the beginning and the end
    duration = end_datetime-start_datetime
    #set the code s date
    current_datetime = start_datetime
    #declare ALL the dataframes lists and variables needed
    a = 0
    list01 = [" "]
    kws = pd.DataFrame()
    kws_bydate = pd.DataFrame()
    #create the header data
    url = "https://www.babbar.tech/api/host/keywords?api_token="+API_KEY
    headers = CaseInsensitiveDict()
    headers["accept"] = "application/json"
    headers["Content-Type"] = "application/json"
    #+1 day because you need to have all the days
    periods = duration.days + 1
    for i in range(periods):
        #not needed but displays the number of days the code is currently at
        print("day "+str(i +1))
        date = str(current_datetime.year)+'-'+str(current_datetime.month)+'-'+str(current_datetime.day)
        while list01 != []:
            data = '{"host": "'+str(host)+'",  "lang": "'+str(lang)+'",  "country": "'+str(country)+'",  "date": "'+str(date)+'",  "offset": '+str(a)+',  "n": 500,  "min": 1,  "max": 100}'
            resp = requests.post(url, headers=headers, data=data)
            #handling status code different from 200
            if resp.status_code != 200:
                print("STATUS CODE INVALID")
                print(resp.status_code)
                list01 = []
                break
            else:
                aDict = resp.json()
                #waiting when the rate is about to be exceeded
                remain = int(resp.headers.get('X-RateLimit-Remaining'))
                if remain == 0:
                    time.sleep(60)
                #drop the unwanted columns to a list
                list01 = aDict['entries']
                #normalize dataframe
                kws_fetch = pd.DataFrame(list01, columns = ['feature','rank','subRank','keywords','url','numberOfWordsInKeyword','bks'])
                #stack the dataframes
                kws_bydate = pd.concat([kws_bydate,kws_fetch])
                #add the date
                kws_bydate = kws_bydate.assign(date = current_datetime)
                #iterate the offset
                a = a +1
        #iterate the days after the code
        current_datetime = current_datetime + datetime.timedelta(days=1)
        #stack the day s dataframe to a global one to avoid losing the day s dataframe
        kws = pd.concat([kws,kws_bydate])
        #reset the offset
        a=0
        #reset the list (or you may not have any other day than the first)
        list01 = [" "]
    #return to obtain the data out of the function
    return(kws)
def babbar_keywords_to_csv(h,l,c,s,e,API):
    df = babbar_keywords(h,l,c,s,e,API)
    df.to_csv(f'{h}_keywords.csv')
# Fetch /host/overview/main information for the host in config.ini file and store it in a csv file
def host_overview_main(host,api_key):
    headers = CaseInsensitiveDict()
    url = 'https://www.babbar.tech/api/host/overview/main'
    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json'
    }
    data = {
        'host': host
    }
    params = {
        'api_token': api_key
    }
    response = requests.post(url, headers=headers, json=data, params=params).json()
    return response
def host_main_csv(host, api_key):
    response = host_overview_main(host,api_key)
    # Get the desired values from the response
    hostValue = response['hostValue']
    hostTrust = response['hostTrust']
    semanticValue = response['semanticValue']
    babbarAuthorityScore = response['babbarAuthorityScore']
    linkCount = response['backlinks']['linkCount']
    hostCount = response['backlinks']['hostCount']
    # Save the values to a CSV file
    with open(f'{host}_overview_main.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['HV', 'HT', 'HSV', 'HBAS', 'links_no', 'hosts_no'])
        writer.writerow([hostValue, hostTrust, semanticValue, babbarAuthorityScore, linkCount, hostCount])
# Fetch /host/backlinks/url information for the host in config.ini file and store it in a csv file
def host_backlinks_csv(host, api_key):
    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json'
    }
    params = {
        'api_token': api_key
    }
    url = 'https://www.babbar.tech/api/host/backlinks/url/list'
    data = {
        'host': host,
        'n': 500,
        'offset': 0
    }
    all_data = []  # to store all the retrieved data
    with open(f'{host}_bl.csv', 'w', newline='', encoding='utf-8-sig') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['source', 'target', 'linkText', 'linkType', 'linkRels', 'language', 'pageValue', 'semanticValue', 'babbarAuthorityScore', 'pageTrust'])
    while True:
        response = requests.post(url, headers=headers, params=params, json=data)
        response_data = response.json()
        if 'links' in response_data and len(response_data['links']) > 0:
            all_data = response_data['links']
            remain = int(response.headers.get('X-RateLimit-Remaining', 1))
            with open(f'{host}_bl.csv', 'a', newline='', encoding='utf-8-sig') as csvfile:
                writer = csv.writer(csvfile)
                for row in all_data:
                    writer.writerow([row.get('source', ''), row.get('target', ''), row.get('linkText', ''), row.get('linkType', ''), row.get('linkRels', []), row.get('language', ''), row.get('pageValue', ''), row.get('semanticValue', ''), row.get('babbarAuthorityScore', ''), row.get('pageTrust', '')])
            if remain == 0:
                print(f"holding at{data['offset']}")
                time.sleep(60)
            data['offset'] += 1
        else:
            break  # no more data to retrieve, break out of the loop
def host_anchors_csv(host, api_key):
    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json'
    }
    params = {
        'api_token': api_key
    }
    url = 'https://www.babbar.tech/api/host/anchors'
    data = {
        'host': host,
    }
    all_data = []  # to store all the retrieved data
    with open(f'{host}_anch.csv', 'w', newline='', encoding='utf-8-sig') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Anchor', 'percent', 'links', 'hosts'])
    response = requests.post(url, headers=headers, params=params, json=data)
    response_data = response.json()
    if 'backlinks' in response_data and len(response_data['backlinks']) > 0:
        all_data = response_data['backlinks']
        remain = int(response.headers.get('X-RateLimit-Remaining', 1))
        with open(f'{host}_anch.csv', 'a', newline='', encoding='utf-8-sig') as csvfile:
            writer = csv.writer(csvfile)
            for row in all_data:
                writer.writerow([row.get('text', ''), row.get('percent', ''), row.get('linkCount', ''), row.get('hostCount', '')])
        if remain == 0:
            print(f"holding at{data['offset']}")
            time.sleep(60)

def filtering_kws(host, bks_min, bks_max):
    csvkws = f'{host}_keywords.csv'
    tokws = f'{host}_toselect.csv'
    output_filepath = os.path.join('data', tokws)
    # Read the CSV file
    df = pd.read_csv(csvkws)
    # Filter the lines on the "bks" column within the specified range
    df_filtered = df[(df['bks'] >= bks_min) & (df['bks'] <= bks_max)]
    # Extract the "keywords" and "rank" columns from the filtered DataFrame
    df_selected = df_filtered[['keywords', 'url', 'rank']]
    # Write the selected data to a new CSV file called "_toselect.csv"
    df_selected.to_csv(output_filepath)

def selector_2500(host, number=2500):
    input_filename = f'{host}_toselect.csv'
    output_filename = f'{host}_selected.csv'
    input_filepath = os.path.join('data', input_filename)
    output_filepath = os.path.join('data', output_filename)
    # Set the desired proportions for each rank category
    proportions = {'top3': 0.15, '1stpage': 0.3, '2ndpage': 0.55}
    # Set the rank category ranges
    categories = {
        'top3': [1, 3],
        '1stpage': [4, 10],
        '2ndpage': [11, 20],
        'lowvalue': [21, float('inf')]
    }
    # Read the CSV file
    df = pd.read_csv(input_filepath)
    # Select random rows for the categories with the desired proportions
    selected_rows = []
    for category, proportion in proportions.items():
        count = int(proportion * number)
        if count > 0:
            rows = df[(df['rank'] >= categories[category][0]) & (df['rank'] <= categories[category][1])].sample(n=count, replace=True, random_state=42)
            selected_rows.append(rows)
    # Concatenate the selected rows into a single DataFrame
    df_selected = pd.concat(selected_rows)
    # Drop duplicates from the selected data based on all columns
    df_selected = df_selected.drop_duplicates()
    if df_selected.shape[0] < number:
        diff = number - df_selected.shape[0]
        # Read the input CSV file into a DataFrame
        df = pd.read_csv(input_filepath)
        # Filter the DataFrame to keep only the rows where the rank is above 20
        df_filtered = df[df['rank'] > 20]
        rows_to_select = min(diff, len(df_filtered))
        if rows_to_select > 0:
            rows = df_filtered.sample(n=rows_to_select, random_state=42)
        else:
            rows = pd.DataFrame()
        df_selected = pd.concat([df_selected, rows])
    df_selected = df_selected.drop_duplicates()
    # Keep only the 'keywords', 'url', and 'rank' columns
    df_selected = df_selected[['keywords', 'url', 'rank']]
    # Write the selected data to a new CSV file called "_selected.csv"
    df_selected.to_csv(output_filepath, index=False)

# main
if __name__ == "__main__":
    config_bool = input("config ini up to date ? (Y/N)")
    if config_bool == "N":
        configuration_file(configfile, keyfile)
    elif config_bool != "Y":
        print("unauthorized input : Please config the ini file")
        configuration_file(configfile, keyfile)
    else :
        babbar_keywords_to_csv(host,language,country,last_monday,last_monday,api_key)
        host_main_csv(host, api_key)
        host_backlinks_csv(host, api_key)
        host_anchors_csv(host, api_key)
        filtering_kws(host, bks_min, bks_max)
        selector_2500(host)