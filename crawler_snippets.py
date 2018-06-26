# -*- coding: utf-8 -*-
from __future__ import print_function
import sys,os
import logging
import codecs
import time
import string
from bs4 import BeautifulSoup
import json
import requests
import argparse

reload(sys)
sys.setdefaultencoding('utf-8')

# create logger with 'spam_application'
logger = logging.getLogger('snippet crawler')
logger.setLevel(logging.DEBUG)
# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(ch)


class snippets_crawler():
    def __init__(self, crawl_google, crawl_yahoo, crawl_bing, limit, output_root):
        self.crawl_google = crawl_google
        self.crawl_yahoo = crawl_yahoo
        self.crawl_bing = crawl_bing
        self.results_limit = limit
        self.url_list = set()
        self.output_root = output_root
        
        if not os.path.exists(self.output_root):
            os.makedirs(self.output_root)
        
    def get_page(self, url, para=None):
        try:
            response = requests.get(url, params=para)
            logger.info('Got URL:' + response.url)
            response.encoding = 'utf-8'
            if response.status_code == 403:
                logger.warn('403 ' + url)
                sys.exit()
            time.sleep(1)
            return response.url, response.text
        except:
            logger.error('Error: ' + url)
            return 'ERROR','ERROR'                  

            
    def google_get_search_results(self, query_id, url, content, pages_cnt, crawl_idx, results):
        page = BeautifulSoup(content, 'lxml')
        
        if page.find('div', id='ires'):
            #results_cnt = int(page.find(id='sortBy').find('span', 'sortRight').span.string.split()[-2])
            list_results = page.find('div', id='ires').find_all('div', 'g')            
            for l in list_results:
                if l.find('h3', 'r'):
                    try:
                        href = l.find('h3', 'r').a['href']
                        k,v = href.split('?')[1].split('&')[0].split('=')
                        if k == 'q' and ('http' in v or 'https' in v):
                            url = v
                    except:
                        url = ''
                    # if url in self.url_list:
                    #     continue
                    if l.find('span', 'st'):
                        snippets = l.find('span', 'st').get_text()
                    else:
                        snippets = u''
                    if len(url) > 0 or len(snippets) > 0:
                        self.url_list.add(url)
                        results.append({'id': url, 'contents': snippets})
                        crawl_idx += 1
                        #print self.pages_cnt,self.max_pages
                        if crawl_idx > self.results_limit:
                            return

            if page.find('table', id='nav'):
                page_index = page.find('table', id='nav').find_all('td')
                for i in range(len(page_index)):
                    if not page_index[i].find('a'):
                        if pages_cnt == 1:
                            next_page = page_index[i+2]
                        else:
                            next_page = page_index[i+1]
                        if next_page.find('a'):
                            next_url = 'https://www.google.com'+next_page.a['href']
                            pages_cnt += 1                    
                            next_url, next_content = self.get_page(next_url)
                            self.google_get_search_results(query_id, next_url, next_content, pages_cnt, crawl_idx, results)
                        else:
                            return
                        break


    def google_crawl(self, qid, query_text):
        url_base = 'https://www.google.com/search?'
        parameters = {'q': query_text, 'hl': 'en'}
        pages_cnt = 1
        crawl_idx = 1
        results = []
        
        final_url, content = self.get_page(url_base, parameters)
        
        with codecs.open('google_test.html', 'w', 'utf-8') as out:
            out.write(content)
        
        logger.info('crawling Google data...')
        
        self.google_get_search_results(qid, final_url, content, pages_cnt, crawl_idx, results)
        with codecs.open(os.path.join(self.output_root, 'google_%s.json' % qid), 'wb', 'utf-8') as f:
            json.dump(results, f, indent=2)
    
        
    def yahoo_get_search_results(self, qid, url, content, crawl_idx, results):
        page = BeautifulSoup(content, 'lxml')

        if page.find('div', id='web'):
            list_results = page.find('div', id='web').find_all('li')
        
            for l in list_results:
                if l.find('h3', 'title'):
                    href_link = l.find('h3', 'title').a['href']
                    url = href_link
                    # if url in self.url_list:
                    #     continue
                    if l.find('div', 'compText'):
                        snippets = l.find('div', 'compText').get_text()
                    else:
                        snippets = u''
                    if len(url) > 0 or len(snippets) > 0:
                        self.url_list.add(url)
                        results.append({
                            'id': url, 
                            'contents': snippets
                        })
                        crawl_idx += 1
                        if crawl_idx > self.results_limit:
                            return

            
            if page.find('div', 'compPagination'):
                if page.find('div', 'compPagination').find('strong'):
                    if page.find('div', 'compPagination').strong.find_next_sibling('a'):
                        next_url = page.find('div', 'compPagination').strong.find_next_sibling('a')['href']       
                        next_url, next_content = self.get_page(next_url)
                        #print next_url
                        self.yahoo_get_search_results(qid, next_url, next_content, crawl_idx, results)
                    else:
                        return
                        
                

    def yahoo_crawl(self, qid, query_text):
        url_base = 'http://search.yahoo.com/search?'
        parameters = {'p': query_text}

        crawl_idx = 1
        results = []
        final_url, content = self.get_page(url_base, parameters)
        
        # with codecs.open('yahoo_test.html', 'wb', 'utf-8') as out:
        #    out.write(content)
        
        logger.info('crawling Yahoo data...')
        self.yahoo_get_search_results(qid, final_url, content, crawl_idx, results)
        with codecs.open(os.path.join(self.output_root, 'yahoo_%s.json' % qid), 'wb', 'utf-8') as f:
            json.dump(results, f, indent=2)


    def bing_get_search_results(self, qid, query_text, url, content, first_cnt, crawl_idx, results):
        page = BeautifulSoup(content, 'lxml')

        if page.find('ol', id='b_results'):
            list_results = page.find('ol', id='b_results').find_all('li', 'b_algo')
            for l in list_results:
                if l.find('h2'):
                    href_link = l.find('h2').a['href']
                    url = href_link
                    # if url in self.url_list:
                    #     continue
                    if l.find('div', 'b_caption') and l.find('div', 'b_caption').find('p'):
                        snippets = l.find('div', 'b_caption').find('p').get_text()
                    else:
                        snippets = u''
                    if len(url) > 0 or len(snippets) > 0:
                        self.url_list.add(url)
                        results.append({
                            'id': url, 
                            'contents': snippets
                        })
                        crawl_idx += 1
                        if crawl_idx > self.results_limit:
                            return
            
            url_base = 'https://www.bing.com/search'
            first_cnt += 10
            if first_cnt > 1000:
                return
            next_url, next_content = self.get_page(url_base, {'q': query_text, 'first': first_cnt})
            #print next_url
            self.bing_get_search_results(qid, query_text, next_url, next_content, first_cnt, crawl_idx, results)

    def bing_crawl(self, qid, query_text):
        url_base = 'https://www.bing.com/search?'
        parameters = {'q': query_text}
        crawl_idx = 1
        first_cnt = 1
        results = []
        
        final_url, content = self.get_page(url_base, parameters)
        
        # with codecs.open('bing_test.html', 'w', 'utf-8') as out:
        #     out.write(content)
        
        logger.info('crawling Bing data...')
        
        self.bing_get_search_results(qid, query_text, final_url, content, first_cnt, crawl_idx, results)
        with codecs.open(os.path.join(self.output_root, 'bing_%s.json' % qid), 'wb', 'utf-8') as f:
            json.dump(results, f, indent=2)

  
    def start_crawl(self, query):
        logger.info(query)
        if self.crawl_google:
            self.google_crawl(query[0], query[1])
        if self.crawl_yahoo:
            self.yahoo_crawl(query[0], query[1])
        if self.crawl_bing:
            self.bing_crawl(query[0], query[1])


def load_queries(fn='sample_topics'):
    queries_list = []
    with open(fn) as f:
        for line in f:
            line = line.strip()
            if line:
                row = line.split(':')
                queries_list.append((row[0],row[1]))

    return queries_list
    
def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--topic_path', 
        type=str, 
        default='sample_topics',
        required=True,
        help='Path to the topic file. The format is "qid:topic terms" per line'
    )
    parser.add_argument(
        '--crawl_google',
        default=False,
        required=False,
        action='store_true',
        help='Boolean switch to turn on Google crawler'
    )
    parser.add_argument(
        '--crawl_yahoo', 
        default=False,
        required=False,
        action='store_true',
        help='Boolean switch to turn on Yahoo crawler'
    )
    parser.add_argument(
        '--crawl_bing',
        default=False,
        required=False,
        action='store_true',
        help='Boolean switch to turn on Bing crawler'
    )
    parser.add_argument(
        '--limit', 
        type=int, 
        default=100,
        required=False,
        help='How many search results we need'
    )
    parser.add_argument(
        '--output_root', 
        type=str, 
        default='snippets',
        required=False,
        help='Output folder'
    )
    return parser.parse_args()

if __name__ == '__main__':
    args = parse_arguments()
    queries = load_queries(args.topic_path)
    crawler = snippets_crawler(
        args.crawl_google, 
        args.crawl_yahoo, 
        args.crawl_bing,
        args.limit,
        args.output_root
    )
    for q in queries:
        crawler.start_crawl(q)

