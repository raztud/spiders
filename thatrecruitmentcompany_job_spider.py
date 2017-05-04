from scrapy.spider import Spider
from scrapy.selector import Selector

# from scrapy.selector import HtmlXPathSelector
from jobs.items import EjobJob
from lxml.html.clean import Cleaner
import re
import html2text
import datetime
import time
import sys
from jobs.database import Database


'''
scrapy crawl thatrecruitmentcompany_job -a url=http://www.thatrecruitmentcompany.com/jobs/view/senior-java-developer-finance-2
'''
class ThatrecruitmentcompanyJobsSpider(Spider):
    name = "thatrecruitmentcompany_job"
    allowed_domains = ["thatrecruitmentcompany.com"]

    def __init__(self, url=None, *args, **kwargs):
        super(Spider, self).__init__(*args, **kwargs)
        self.url = url
        self.start_urls = [url]

    @staticmethod
    def clean_html(html):
        tags = ['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'div', 'span', 'img', 'area', 'div', 'map']
        args = {'meta': False, 'safe_attrs_only': False, 'page_structure': True,
                'scripts': True, 'style': True, 'links': True, 'remove_tags': tags}
        cleaner = Cleaner(**args)

        return cleaner.clean_html(html).encode('utf8', 'replace')

    def parse(self, response):
        db = Database(self.settings.get('MYSQL'))

        cur = db.getCursor()
        item = EjobJob()
        try:
            cur.execute("SELECT u.id as id, s.domain, s.category_id FROM urls u LEFT JOIN sites s ON s.id=u.site_id "
                        "WHERE u.url=%s", (self.url, ))
            row = cur.fetchone()
            if row is None:
                raise Exception("No url found in DB: {0}".format(self.url))
            url_id = row[0]
            domain_name = row[1]
            item['category_id'] = row[2]
        except Exception, e:
            print cur._last_executed
            print e
            raise


        sel = Selector(response)

        item['url_id'] = url_id
        item['link'] = self.start_urls[0]

        banned_domains = []

        try:
            title = sel.xpath('//h1[@class="header-text"]/text()').extract()[0]
            item['title'] = title.encode('utf8', 'replace').strip()
            html = sel.xpath('//div[@class="wpjb-job-text"]').extract()[0]

            item['description'] = self.clean_html(html).replace('<div>', '').replace('</div>', '').strip()

        except Exception, e:
            print e
            return None

        item['company'] = 'The Recruitment Company'

        #The Recruitment Company does not have valability; we set up at +30 days from crawling time
        today = datetime.date.today()
        valability = today + datetime.timedelta(30)
        sql_valability = '{0}-{1}-{2} 00:00:00'.format(valability.year, str(valability.month).zfill(2), str(valability.day).zfill(2))
        item['valability'] = sql_valability

        item['posted_at'] = '{0}-{1}-{2} 00:00:00'.format(today.year, str(today.month).zfill(2), str(today.day).zfill(2))
        item['updated'] = item['posted_at']

        job_type = None
        try:
            job_types = []
            # take the next TD afer the TD with text Job Type
            job_types_nodes = sel.xpath('//table[@class="wpjb-info"]/tbody/tr/td[. = "Job Type"]/following-sibling::td/a/text()').extract()
            for empl in job_types_nodes:
                job_types.append(empl)
            job_type = ', '.join(job_types)
        except:
            pass

        item['job_type'] = job_type

        salary = None
        item['salary_currency'] = None
        try:
            raw_salary = sel.xpath('//table[@class="wpjb-info"]/tbody/tr/td[. = "Salary"]/following-sibling::td/text()').extract()[0]
            salary = raw_salary.encode('utf8', 'replace').replace('\xe2\x82\xac', '')
            item['salary_currency'] = 'EUR'
        except:
            pass

        item['salary'] = salary
        item['languages'] = None

        address = None
        try:
            raw_address = sel.xpath('//table[@class="wpjb-info"]/tbody/tr/td[. = "Location"]/following-sibling::td/text()').extract()[0]
            address = raw_address.encode('utf8', 'replace')
        except:
            pass

        item['address'] = address

        item.save(db)

        return item


