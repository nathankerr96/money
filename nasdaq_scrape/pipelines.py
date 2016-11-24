from scrapy.exceptions import DropItem
import json
#import smtplib
#from email.MIMEMultipart import MIMEMultipart
#from email.MIMEText import MIMEText
#from email.MIMEImage import MIMEImage
#from email.MIMEBase import MIMEBase
#from email import Encoders

# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html


class NasdaqScrapePipeline(object):
    # Load to datastore here and do email notifications here
    def open_spider(self, spider):
        self.file = open('data.json', 'wb')

    def close_spider(self, spider):
        self.file.close() 

    def process_item(self, item, spider):
        print item
        line = json.dumps(dict(item)) + '\n'
        self.file.write(line)
        return item 
        
