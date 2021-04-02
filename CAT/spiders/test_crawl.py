# Libraries to Import
import scrapy
import json
from scrapy import signals
from pydispatch import dispatcher
import sqlite3

class QuotesSpider(scrapy.Spider):

    name = "cat"
    start_urls = ['https://www.finning.com/en_CA/products/new/power-systems/electric-power-generation.html']
    Index = 0
    results = {}

    def __init__(self):
       dispatcher.connect(self.spider_closed, signals.spider_closed)
    
    def parse(self,response):
        #response.css("ul.unit-hidden.models-list > li > a::attr('href')")
        #print(response.css("ul.unit-hidden.models-list").get())

        # Function to extract each link from the Website

         for ad in response.css("ul.unit-hidden.models-list > li > a::attr('href')"):
             adUrl = 'https://www.finning.com' + ad.get()
             if 'diesel-generator-sets' in adUrl:
                yield scrapy.Request(url = adUrl, callback=self.parseInnerPage)

    def parseInnerPage(self,response):  

    #     Function to Parse Each Link to extract the Required machine name, maximum rating, minimum rating, 
    #     frequency ,voltage,speed   

        machine_name = response.css("h1.product-title::text").get()
        volt = None
        speed = None
        for attrs in response.css("div.product-specs-category > div.specs > div.spec.spaced-spec"):   
            if 'Maximum Rating' in attrs.css("strong::text").get():
                max_rate = attrs.css("span.unit.unit-metric.unit-hidden::text").get()
            if 'Minimum Rating' in attrs.css("strong::text").get():
                min_rate = attrs.css("span.unit.unit-metric.unit-hidden::text").get()
            if 'Frequency' in attrs.css("strong::text").get():
                freq = attrs.css("span.unit.unit-metric.unit-hidden::text").get()   
            if 'Voltage' in attrs.css("strong::text").get():
                volt = attrs.css("span.unit.unit-metric.unit-hidden::text").get()
            if 'Speed' in attrs.css("strong::text").get():
                speed = attrs.css("span.unit.unit-metric.unit-hidden::text").get()
                
        # Dictionary To Store the Scrapped Data
        self.results[self.Index] = {
            "machine_name": machine_name,
            "Maximum Rating": max_rate,
            "Minimum Rating": min_rate,
            "Frequency": freq,
            "Voltage": volt,
            "Speed": speed}

        self.Index = self.Index + 1
    
    # # Function to Store the Data in SQL Table , JSON and Close the Spider Crawler
    def spider_closed(self, spider):

        #Dump a Copy of Data in JSON
        with open('test_new.json', 'w') as fp:
            json.dump(self.results, fp)
        
        #Dump the Data in SQLITE Database
        conn = sqlite3.connect('test.db')
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS TEST_DATA
             (machine_name, Maximum_Rating, Minimum_Rating,Frequency,Voltage,Speed)''')
        # Insert values
        for i in self.results:
            c.execute('''insert into TEST_DATA values(?,?,?,?,?,?)''', [self.results[i]["machine_name"],
            self.results[i]["Maximum Rating"],
            self.results[i]["Minimum Rating"],
            self.results[i]["Frequency"],
            self.results[i]["Voltage"],
            self.results[i]["Speed"]])
            conn.commit()

    
   