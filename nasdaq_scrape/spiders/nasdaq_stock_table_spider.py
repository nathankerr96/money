import scrapy
from urlparse import urlparse, parse_qs
import re
import os 

class NASDAQSpider(scrapy.Spider):
    name = 'nasdaq_spider'
    tickers_path = 'tickers_short_text.txt'
    TIME_SEGMENT = [ '9:30 - 9:59',
                     '10:00 - 10:29',
                     '10:30 - 10:59',
                     '11:00 - 11:59',
                     '12:00 - 12:29',
                     '12:30 - 12:59',
                     '13:00 - 13:29',
                     '13:30 - 13:59',
                     '14:00 - 14:29',
                     '14:30 - 14:59',
                     '15:00 - 15:29',
                     '15:30 - 16:00' ]

    _lp_xpath = '//a[@id="quotes_content_left_lb_LastPage"]/@href'
    _nls_tr_xpath = '//table[@id="AfterHoursPagingContents_Table"]/tr'

    def start_requests(self):
        #TODO: This will be later adapted to read from a redis queue
        with open(self.tickers_path, 'rb') as f:
            for line in f:
                ticker = line.strip()
                print 'Starting Scrape for {}...'.format(ticker.upper())
                ticker_main_url = self._generate_url(ticker, 1, 1)
                init_request = scrapy.Request(ticker_main_url, callback=self.prepare_ticker_scrape, dont_filter=True)
                init_request.meta['TICKER'] = ticker
                yield init_request 


    def prepare_ticker_scrape(self, response):
        last_pagenos = response.meta.get('LAST_PAGENOS') if response.meta.get('LAST_PAGENOS') else []
        lp_url = response.xpath(self._lp_xpath)

        # Case where page only has one pageno
        if not lp_url:
            cur_pageno = 1
        else:
            lp_url.extract()[0]
            qs_dict = parse_qs(urlparse(lp_url).query)
            cur_pageno = qs_dict['pageno']
    
        if last_pagenos:
            last_timeseg = last_pagenos[-1][0]
            last_timeseg_index = self.TIME_SEGMENT.index(last_timeseg)
            cur_timeseg = self.TIME_SEGMENT[last_timeseg_index + 1]
        else:
            print 'Collecting metadata for TICKER {}...'.format(response.meta['TICKER'])
            cur_timeseg = '9:30 - 9:59'

        last_pagenos.append( (cur_timeseg, cur_pageno) ) 
        
        # Stop at the last time segment
        if cur_timeseg == '15:30 - 16:00':
            last_pagenos_dict = dict(last_pagenos)
            print 'Finished collecting metadata'
            print 'LAST_PAGENOS:'
            print last_pagenos_dict

            start_url = self._generate_url(response.meta['TICKER'], 1, last_pagenos_dict['9:30 - 9:59'])
            start_request = scrapy.Request(start_url, callback=self.ticker_scrape, dont_filter=True)
            start_request.meta['TICKER'] = response.meta['TICKER']
            start_request.meta['PROGRESS'] = { 'LAST_PAGENOS': last_pagenos_dict,
                                               'TICKER': str(response.meta['TICKER']), #str constructure makes a copy of a str
                                               'CUR_TIME_SEGMENT': '9:30 - 9:59',
                                               'CUR_PAGENO': last_pagenos_dict['9:30 - 9:59'] }
            start_request.meta['DATA'] = []
                                     
            return start_request
        
        next_timeseg_index = self.TIME_SEGMENT.index(cur_timeseg) + 1
        next_url = self._generate_url(response.meta['TICKER'], next_timeseg_index, 1) 
        prepare_request = scrapy.Request(next_url, callback=self.prepare_ticker_scrape, dont_filter=True)
        prepare_request.meta['TICKER'] = response.meta['TICKER']
        prepare_request.meta['LAST_PAGENOS'] = last_pagenos 
        return prepare_request
        
    def ticker_scrape(self, response):
        if self._finished_scrape(response.meta['PROGRESS']):
            del response.meta['PROGRESS']
            return { 'TICKER': response.meta['TICKER'],
                     'DATA': response.meta['DATA'] }
        else:
            # Set up next call
            next_timeseg, next_pageno = self._get_next_url_params(response.meta['PROGRESS'])
            next_timesegno = self.TIME_SEGMENT.index(next_timeseg) + 1
            request = scrapy.Request(self._generate_url(response.meta['TICKER'], next_timesegno, next_pageno), 
                                     callback=self.ticker_scrape,
                                     dont_filter=True)
            # Replace
            request.meta['TICKER'] = response.meta['TICKER']
            request.meta['PROGRESS'] = response.meta['PROGRESS']
            request.meta['DATA'] = response.meta['DATA']
            # Update
            request.meta['PROGRESS']['CUR_TIME_SEGMENT'] = next_timeseg
            request.meta['PROGRESS']['CUR_PAGENO'] = next_pageno

            # Collect Xpath Stuff here  
            nls_time_re = r'.+<td>([0-9]{2}:[0-9]{2}:[0-9]{2})</td>.+'
            nls_price_re = r'.+<td>[$]\xa0([0-9]+[.][0-9]+)\xa0</td>'
            nls_volume_re = r'.+<td>([0-9]+)</td>.+'

            nls_quotes = response.xpath(self._nls_tr_xpath)

            if not nls_quotes:
                return request
            else:
                nls_quotes.reverse()

            nls_data = []
            for quote in nls_quotes:
                tr_text = quote.extract()
                tr_text = tr_text.replace('\r', '').replace('\n', '').replace('\t', '').replace(' ', '')
                m_time = re.match(nls_time_re, tr_text)
                m_price = re.match(nls_price_re, tr_text)
                m_volume = re.match(nls_volume_re, tr_text)
                
                if m_time and m_price and m_volume:
                    nls_time = m_time.group(1)
                    nls_price = m_price.group(1)
                    nls_volume = m_volume.group(1)
                    nls_data.append({'time': nls_time, 'price': nls_price, 'volume': nls_volume})
                else:
                    #TODO: ERROR CHECKING
                    pass
            
            print 'nls_data: {}'.format(nls_data)
            for data in nls_data:
                request.meta['DATA'].append(data)
            
            return request

    def _get_next_url_params(self, progress):
        cur_timeseg_index = self.TIME_SEGMENT.index(progress['CUR_TIME_SEGMENT'])
        next_timeseg = self.TIME_SEGMENT[cur_timeseg_index + 1]
        if progress['CUR_PAGENO'] == 1:
            return next_timeseg, progress['LAST_PAGENOS'][next_timeseg]
        else:
            return progress['CUR_TIME_SEGMENT'], progress['CUR_PAGENO'] - 1

    def _finished_scrape(self, progress):
        if progress['CUR_TIME_SEGMENT'] == '15:30 - 16:00' and progress['CUR_PAGENO'] == 1:
            return True
        else:
            return False

    def _generate_url(self, ticker, time_pg, page_no):
        ''' Generates the url to scrape '''
        # TODO: Validate
        return 'http://www.nasdaq.com/symbol/{}/time-sales?time={}&pageno={}'.format(ticker, time_pg, page_no)

