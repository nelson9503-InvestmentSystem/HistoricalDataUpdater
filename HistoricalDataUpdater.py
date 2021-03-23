from . import mysql
from .yfapi import YFAPI
from . import SQLTemples
from . import timestamp
from . import TerminalReporter

import os
import json


class Updater:

    def __init__(self, sql_config_path: str = None):
        if sql_config_path == None:
            self.sql_config_path = "./sql_config.json"
        else:
            self.sql_config_path = sql_config_path
        self.__get_sql_config()
        self.symbolDB = mysql.DB("symbols", self.host,
                                 self.port, self.user, self.password)
        self.__save_sql_config()
        self.symbolsTB = self.symbolDB.TB("symbols")
        self.historicalPriceDB = mysql.DB(
            "historical_price", self.host, self.port, self.user, self.password)
        self.stocksplitDB = mysql.DB(
            "stock_split", self.host, self.port, self.user, self.password)
        self.dividendDB = mysql.DB(
            "dividend", self.host, self.port, self.user, self.password)

    def update_US(self):
        """
        Update historical data (price, dividend and stock split) of US symbol.
        """
        reporter = TerminalReporter.Reporter("HistoricalDataUpdater", "Updating US symbols...")
        reporter.report()

        symbols = self.symbolsTB.query("*", "WHERE market = 'US' AND enable = true")
        
        reporter.what = "Updating symbol historical data..."
        reporter.initialize_stepIntro(len(symbols))
        for symbol in symbols:

            reporter.report(True)

            api = YFAPI(symbol)
            tbName = symbol.lower()

            price = api.price()
            if not tbName in self.historicalPriceDB.list_tb():
                self.__create_tb_with_templates(self.historicalPriceDB, tbName, SQLTemples.HISTROCIAL_PRICE)
            priceTB = self.historicalPriceDB.TB(tbName)
            dates = list(priceTB.query().keys())
            if len(dates) == 0:
                lastdate = 0
            else:
                lastdate = max(dates)
            price = self.__clean_historical_data(price, lastdate)
            priceTB.update(price)

            dividend = api.dividend()
            if not tbName in self.dividendDB.list_tb():
                self.__create_tb_with_templates(self.dividendDB, tbName, SQLTemples.DIVIDEND)
            dividendTB = self.dividendDB.TB(tbName)
            dates = list(dividendTB.query().keys())
            if len(dates) == 0:
                lastdate = 0
            else:
                lastdate = max(dates)
            dividend = self.__clean_historical_data(dividend, lastdate)
            dividendTB.update(dividend)            

            stocksplit = api.stocksplit()
            if not tbName in self.stocksplitDB.list_tb():
                self.__create_tb_with_templates(self.stocksplitDB, tbName, SQLTemples.STOCK_SPLIT)
            stocksplitTB = self.stocksplitDB.TB(tbName)
            dates = list(stocksplitTB.query().keys())
            if len(dates) == 0:
                lastdate = 0
            else:
                lastdate = max(dates)
            stocksplit = self.__clean_historical_data(stocksplit, lastdate)
            # we split the stock split to two factors
            newdata = {}
            for date in stocksplit:
                split = stocksplit[date]["stocksplit"].split(":")
                newdata[date] = {
                    "priceMultipleFactor": split[1],
                    "priceDivideFactor": split[0]
                }
            stocksplitTB.update(newdata)

            self.historicalPriceDB.commit()
            self.stocksplitDB.commit()
            self.dividendDB.commit()
        reporter.what = "Saving to sql server..."
        reporter.report()
        self.historicalPriceDB.close()
        self.stocksplitDB.close()
        self.dividendDB.close()
        reporter.what = "Done."
        reporter.report()

    def __clean_historical_data(self, data: dict, lastdate: int):
        """
        Clean up the incoming data from yfapi.
        """
        newdata = {}
        for date in data:
            if date < lastdate:
                continue
            check = True
            for item in data[date]:
                if data[date][item] in [None, ""]:
                    check = False
            if check == True:
                newdata[timestamp.to_midnight(date)] = data[date]
        return newdata

    def __create_tb_with_templates(self, DB: object, tableName: str, temp: dict):
        colnames = list(temp.keys())
        # first column as key column
        tb = DB.add_tb(tableName, colnames[0], temp[colnames[0]])
        for i in range(1, len(colnames)):
            colname = colnames[i]
            tb.add_col(colname, temp[colname])

    def __get_sql_config(self):
        if not os.path.exists(self.sql_config_path):
            with open(self.sql_config_path, 'w') as f:
                j = {"host": "", "port": 0, "user": "", "password": ""}
                f.write(json.dumps(j))
        with open(self.sql_config_path, 'r') as f:
            sql_config = json.loads(f.read())
        self.host = sql_config["host"]
        self.port = sql_config["port"]
        self.user = sql_config["user"]
        self.password = sql_config["password"]

    def __save_sql_config(self):
        j = self.symbolDB.get_loginInfo()
        with open(self.sql_config_path, 'w') as f:
            f.write(json.dumps(j))
