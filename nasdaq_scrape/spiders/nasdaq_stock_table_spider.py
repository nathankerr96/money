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

    def __init__(self):
        self.q = RedisQueue('Tickers')
        super(NASDAQSpider, self).__init__()

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
        """ Description: quasi-recursive function that collects the last
                         page number for every time segment of a stock and 
                         stores it as meta info to the main parsing 
                         function ticker_scrape and returns the result of the 
                         ticker_scrape response

            Input: response (HtmlResponse) => standard iput object that is used
                                            in scarpy spider parser functions 
            Output: request => either a recursive request to the same callback 
                               or to the ticker_parse parse function callback"""
            
        # Retrieve the (time segment, last page number) tuple list if it exists
        # and get the last page url 
        last_pagenos = response.meta.get('LAST_PAGENOS') if response.meta.get('LAST_PAGENOS') else []
        lp_url = response.xpath(self._lp_xpath)

        # Gets the current timestamp and 
        # puts it into the cur_timeseg variable
        if last_pagenos:
            last_timeseg = last_pagenos[-1][0]
            last_timeseg_index = self.TIME_SEGMENT.index(last_timeseg)
            cur_timeseg = self.TIME_SEGMENT[last_timeseg_index + 1]
        else:
            print 'Collecting metadata for TICKER {}...'.format(response.meta['TICKER'])
            cur_timeseg = '9:30 - 9:59'

        # Get the last page number for the current time segment
        # and stores it in the cur_pageno variable 
        if not lp_url:
            cur_pageno = 1
        else:
            lp_url.extract()[0]
            qs_dict = parse_qs(urlparse(lp_url).query)
            cur_pageno = qs_dict['pageno']

        last_pagenos.append( (cur_timeseg, cur_pageno) )
    
        # Edge Case 1: Invalid Ticker
        if not last_pagenos and not lp_url:
            pass

        # Edge Case 2: Blacklisted IP
        if response.status in BAD_HTTP_RESPONSES:
            # Stop on this thread/ip and change the ip
            # Report status on the redis queue
            pass 

        # Base Case: add the last pageno and start the ticker_scrape  
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
        # General Case: The meta data has begun or is continuing to be collected
        else: 
            next_timeseg_index = self.TIME_SEGMENT.index(cur_timeseg) + 1
            next_url = self._generate_url(response.meta['TICKER'], next_timeseg_index, 1) 
            prepare_request = scrapy.Request(next_url, callback=self.prepare_ticker_scrape, dont_filter=True)
            prepare_request.meta['TICKER'] = response.meta['TICKER']
            prepare_request.meta['LAST_PAGENOS'] = last_pagenos 
            return prepare_request
        
    def ticker_scrape(self, response):
        """ Purpose: To extract the NLS Time, NLS Price, NLS Volume from a ticker
                     after the page number meta data has been collected """

        # Base Case: Finised scraping ticker 
        if self._finished_scrape(response.meta['PROGRESS']):
            del response.meta['PROGRESS']
            return { 'TICKER': response.meta['TICKER'],
                     'DATA': response.meta['DATA'] }
        # General Case
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
        """ Purpose: Gets the next time segment and page number based off current
                     progress. Time segments increment in increasing order and 
                     page numbers increment in decreasing order
            Input:
                progress (dict) => schema:  { 'LAST_PAGENOS': last_pagenos_dict,
                                              'TICKER': str(response.meta['TICKER']), 
                                              'CUR_TIME_SEGMENT': '9:30 - 9:59',
                                              'CUR_PAGENO': last_pagenos_dict['9:30 - 9:59'] }
            Output (tuple) => (next time segment, next page number) """

        cur_timeseg_index = self.TIME_SEGMENT.index(progress['CUR_TIME_SEGMENT'])
        next_timeseg = self.TIME_SEGMENT[cur_timeseg_index + 1]
        if progress['CUR_PAGENO'] == 1:
            return next_timeseg, progress['LAST_PAGENOS'][next_timeseg]
        else:
            return progress['CUR_TIME_SEGMENT'], progress['CUR_PAGENO'] - 1

    def _finished_scrape(self, progress):
        """ Purpose: boolean function that returns True if the scrape 
                     is finished 
            Input: 
                progress (dict) => schema:  { 'LAST_PAGENOS': last_pagenos_dict,
                                              'TICKER': str(response.meta['TICKER']), 
                                              'CUR_TIME_SEGMENT': '9:30 - 9:59',
                                              'CUR_PAGENO': last_pagenos_dict['9:30 - 9:59'] }

            Output (boolean) """
        if progress['CUR_TIME_SEGMENT'] == '15:30 - 16:00' and progress['CUR_PAGENO'] == 1:
            return True
        else:
            return False

    def _generate_url(self, ticker, time_pg, page_no):
        """ Purpose: Generates the url to scrape of the form:
                 http://www.nasdaq.com/symbol/ticker/time-sales?time=time_pg&pageno=page_no   
            
            Inputs: 
                ticker (string) => target ticker
                time_pg (int) => time segment number
                page_no (int) => page number corresponding to time segment 

            Output (string) => url corresponding to the stock with the given parameters """

        return 'http://www.nasdaq.com/symbol/{}/time-sales?time={}&pageno={}'.format(ticker, time_pg, page_no)

