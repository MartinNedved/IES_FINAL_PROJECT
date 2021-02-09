import requests

import time
from datetime import datetime
import math

import json

class DataMiner:

    def __init__(self, SP500=True, NASDAQ=True, DJI=True):
        self.SP500 = SP500
        self.NASDAQ = NASDAQ
        self.DJI = DJI
        self.indexes = []
        if SP500:
            self.indexes.append("^GSPC")
        if NASDAQ:
            self.indexes.append("^NDX")
        if DJI:
            self.indexes.append("^DJI")

    def get_all_symbols(self, constituents_path, hist_constituents_path):
        all_symbols = []

        with open(constituents_path, 'r') as const_f, open(hist_constituents_path, 'r') as hist_const_f:
            constituents = json.load(const_f)
            hist_constituents = json.load(hist_const_f)

        for index in constituents:
            all_symbols += index['constituents']

        for index in hist_constituents:
            for action in index['historicalConstituents']:
                if datetime.strptime(action['date'], '%Y-%m-%d').year > 1999:
                    all_symbols.append(action['symbol'])

        return sorted(list(set(all_symbols)))

    def run(self):

        constituents_path = "constituents.json"
        hist_constituents_path = "hist_constituents.json"

        finnhub_constituents_api_miner = FinnhubApi("https://finnhub.io/api/v1/index/constituents", 
            "api_keys_finnhub.txt", 60, constituents_path, self.indexes)
        finnhub_constituents_api_miner.run()

        finnhub_hist_constituents_api_miner = FinnhubApi("https://finnhub.io/api/v1/index/historical-constituents", 
            "api_keys_finnhub.txt", 60, hist_constituents_path, self.indexes)
        finnhub_hist_constituents_api_miner.run()

        with open('all_symbols.json', 'r') as f:
            all_symbols = json.load(f)

        polygon_stock_financials_api_miner = PolygonApi("https://api.polygon.io/v2/reference/financials",
            "api_keys_polygon.txt", 5, "financials.json", all_symbols)
        polygon_stock_financials_api_miner.run()

        self.__run_alpha_vantage(all_symbols, 2)

    def __run_alpha_vantage(self, all_symbols, download_part=-1):
        # There is API limit of 500 requests/day, we download two datasets from this API -> 250 requests/day for each dataset
        max_requests_per_day = 250
        all_symbols_split_by_max_requests = [all_symbols[i:i + max_requests_per_day] for i in range(0, len(all_symbols), max_requests_per_day)]

        alphavantage_monthly_adjusted_api_miner = AlphaVantageApi(
                    "https://www.alphavantage.co/query",
                    "api_keys_alphavantage.txt", 5, 
                    "TIME_SERIES_MONTHLY_ADJUSTED", 
                    file_name=f"time_series_monthly_adjusted_{0}.json", 
                    symbols=None, 
                    requests_per_day=max_requests_per_day, 
                    keys_in_paralel=False)

        alphavantage_company_overview_api_miner = AlphaVantageApi(
                "https://www.alphavantage.co/query",
                "api_keys_alphavantage.txt", 5, 
                "OVERVIEW", 
                file_name=f"overview_{0}.json", 
                symbols=all_symbols_split_by_max_requests[0], 
                requests_per_day=max_requests_per_day,
                keys_in_paralel=False)

        if download_part == -1:
            for i in range(len(all_symbols_split_by_max_requests)):
                alphavantage_monthly_adjusted_api_miner.symbols = all_symbols_split_by_max_requests[i]
                alphavantage_monthly_adjusted_api_miner.file_name = f"time_series_monthly_adjusted_{i}.json"
                alphavantage_monthly_adjusted_api_miner.run()

                alphavantage_company_overview_api_miner.symbols = all_symbols_split_by_max_requests[i]
                alphavantage_company_overview_api_miner.file_name = f"overview_{i}.json"
                alphavantage_company_overview_api_miner.run()

                # Waits 24hours + 5 min reserve
                if i != len(all_symbols_split_by_max_requests):
                    time.sleep(60*60*24 + 5*60)

        else:
            alphavantage_monthly_adjusted_api_miner.symbols = all_symbols_split_by_max_requests[download_part]
            alphavantage_monthly_adjusted_api_miner.file_name = f"time_series_monthly_adjusted_{download_part}.json"
            alphavantage_monthly_adjusted_api_miner.run()

            alphavantage_company_overview_api_miner.symbols = all_symbols_split_by_max_requests[download_part]
            alphavantage_company_overview_api_miner.file_name = f"overview_{download_part}.json"
            alphavantage_company_overview_api_miner.run()

        # Merge (fail_)TIME_SERIES_MONTHLY_ADJUSTED and (fail_)OVERVIEW files
        if download_part == len(all_symbols_split_by_max_requests)-1 or download_part == -1:
            monthly_adjusted = []
            monthly_adjusted_fail = []
            overview = []
            overview_fail = []
            for i in range(len(all_symbols_split_by_max_requests)):
                with open(f"time_series_monthly_adjusted_{i}.json", 'r') as f_monthly_adjusted, \
                    open(f"fail_time_series_monthly_adjusted_{i}.json", 'r') as f_monthly_adjusted_fail, \
                    open(f"overview_{i}.json", 'r') as f_overview, \
                    open(f"fail_overview_{i}.json", 'r') as f_overview_fail:
                    monthly_adjusted.extend(json.load(f_monthly_adjusted))
                    monthly_adjusted_fail.extend(json.load(f_monthly_adjusted_fail))
                    overview.extend(json.load(f_overview))
                    overview_fail.extend(json.load(f_overview_fail))

            with open("time_series_monthly_adjusted.json", 'w') as f_monthly_adjusted, \
                open("fail_time_series_monthly_adjusted.json", 'w') as f_monthly_adjusted_fail, \
                open("overview.json", 'w') as f_overview, \
                open("fail_overview.json", 'w') as f_overview_fail:
                json.dump(monthly_adjusted, f_monthly_adjusted)
                json.dump(monthly_adjusted_fail, f_monthly_adjusted_fail)
                json.dump(overview, f_overview)
                json.dump(overview_fail, f_overview_fail)


class ApiMiner:

    def __init__(self, url, api_keys_path, requests_per_minute, file_name, symbols=[], requests_per_day=-1, keys_in_paralel=True):
        self.url = url
        self.api_keys_path = api_keys_path
        self.requests_per_minute = requests_per_minute
        self.file_name = file_name
        self.symbols = symbols
        self.requests_per_day = requests_per_day
        self.keys_in_paralel = keys_in_paralel

    def run_requests(self):
        with open(self.api_keys_path) as f:
            keys = f.readlines()
        keys = [x.rstrip("\n") for x in keys]

        # 2 seconds added for reserve 
        if self.keys_in_paralel:
            time_interval = 62 / (self.requests_per_minute * len(keys))
        else:
            time_interval = 62 / self.requests_per_minute

        number_of_key_usage = math.ceil(len(self.symbols) / len(keys))
        keys_cycle = keys * number_of_key_usage
        if self.requests_per_day != -1:
            if len(self.symbols) > self.requests_per_day:
                raise Exception("There are too many requests, provide more/stronger keys or less symbols")
        
        counter = 0
        requests_out = {"S" : [], "F" : []}
        error_messages = self.get_error_messages()
        for api_key, symbol in zip(keys_cycle, self.symbols):
            time.sleep(time_interval)
            print(counter, symbol)
            counter += 1
            r = self.requests_get(api_key, symbol)
            if r.status_code != 200 or r.json() in error_messages:
                requests_out["F"].append(symbol)                
            else:
                requests_out["S"].append(r.json())

        return requests_out

    def requests_get(self, api_key, symbol=""):
        pass

    def get_error_messages(self):
        pass

    def save_to_json(self, file, file_name):
        with open(file_name, 'w') as f:
            json.dump(file, f)

    def run(self):
        requests_dict = self.run_requests()
        self.save_to_json(requests_dict["S"], self.file_name)
        self.save_to_json(requests_dict["F"], f'fail_{self.file_name}')

class PolygonApi(ApiMiner):

    def requests_get(self, api_key, symbol=""):
        params = {
            "limit" : 20,
            "type" : "Y",
            "sort" : "-reportPeriod",
            "apiKey" : api_key
        }
        return requests.get(f"{self.url}/{symbol}", params=params)

    def get_error_messages(self):
        errors = []
        errors.append({"status":"OK","results":[]})
        return errors

class AlphaVantageApi(ApiMiner):

    def __init__(self, url, api_keys_path, requests_per_minute, function, file_name, symbols=[], requests_per_day=500, keys_in_paralel=False):
        super().__init__(url, api_keys_path, requests_per_minute, file_name, symbols, requests_per_day, keys_in_paralel)
        self.function = function

    def requests_get(self, api_key, symbol=""):
        params = {
            "function" : self.function,
            "symbol" : symbol, 
            "apikey" : api_key
        }
        return requests.get(self.url, params=params)

    def get_error_messages(self):
        errors = []
        errors.append({"Error Message": "Invalid API call. Please retry or visit the documentation (https://www.alphavantage.co/documentation/) for TIME_SERIES_MONTHLY_ADJUSTED."})
        errors.append({'Error Message': 'Invalid API call. Please retry or visit the documentation (https://www.alphavantage.co/documentation/) for OVERVIEW.'})
        errors.append({})
        errors.append({"Information": "Thank you for using Alpha Vantage! Our standard API call frequency is 5 calls per minute and 500 calls per day. Please visit https://www.alphavantage.co/premium/ if you would like to target a higher API call frequency."})
        errors.append({"Note": "Thank you for using Alpha Vantage! Our standard API call frequency is 5 calls per minute and 500 calls per day. Please visit https://www.alphavantage.co/premium/ if you would like to target a higher API call frequency."})
        return errors

class FinnhubApi(ApiMiner):

    def requests_get(self, api_key, symbol=""):
        params = {
            "symbol" : symbol, 
            "token" : api_key
        }
        return requests.get(self.url, params=params)

    def get_error_messages(self):
        errors = []
        errors.append({"error":"You don't have access to this resource."})
        return errors

data_miner = DataMiner()
data_miner.run()