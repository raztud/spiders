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
scrapy crawl monster_it_job -a url=http://jobview.monster.co.uk/Developer-in-Test-PHP-Leading-Online-Sport-Company-London-Job-London-London-131295340.aspx
'''
class EjobsJobsSpider(Spider):
    name = "monster_it_job"
    allowed_domains = ["jobview.monster.co.uk"]

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

        banned_domains = ['counter.adcourier.com']

        replace_tags = ''

        try:
            title = sel.xpath('//div[@id="jobcopy"]/h1/text()').extract()[0]
            item['title'] = title.encode('utf8', 'replace')
            html = sel.xpath('//div[@itemprop="description"]').extract()[0]

            # remove all html tags
            # pattern = re.compile(u'<\/?\w+\s*[^>]*?\/?>', re.DOTALL | re.MULTILINE | re.IGNORECASE | re.UNICODE)
            # text = pattern.sub(u" ", html)

            # OR
            # converter = html2text.HTML2Text()
            # converter.ignore_links = True
            # desc = converter.handle(html)

            # desc = desc.replace('http://media.newjobs.com/jobview_standard/images/pixel.gif', '')
            # desc = desc.replace('media.newjobs.com', '')
            # desc = desc.replace('![](', '')
            #
            # urls = re.findall('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', desc)
            # print urls
            # for url in urls:
            #     for banned in banned_domains:
            #         if banned in url:
            #             desc = desc.replace(url, '')

            item['description'] = self.clean_html(html).replace('<div>', '').replace('</div>', '')

            category = sel.xpath('//span[@itemprop="industry"]/text()').extract()[0]

        except Exception, e:
            print e
            return None

        item['company'] = None
        try:
            item['company'] = sel.xpath('//dd[@itemprop="hiringOrganization"]/span[@itemprop="name"]/text()').extract()[0]
        except:
            pass

        #monster does not have valability; we set up at +30 days from crawling time
        today = datetime.date.today()
        valability = today + datetime.timedelta(30)
        sql_valability = '{0}-{1}-{2} 00:00:00'.format(valability.year, str(valability.month).zfill(2), str(valability.day).zfill(2))
        item['valability'] = sql_valability

        item['posted_at'] = '{0}-{1}-{2} 00:00:00'.format(today.year, str(today.month).zfill(2), str(today.day).zfill(2))
        item['updated'] = item['posted_at']

        job_type = None
        try:
            job_types = []
            for empl in sel.xpath('//dd[@class="multipledd"]/span[@itemprop="employmentType"]'):
                job_types.append(empl.xpath('text()').extract()[0])
            job_type = ', '.join(job_types)
        except:
            pass

        item['job_type'] = job_type

        salary = None
        try:
            salary = sel.xpath('//dd/span[@itemprop="baseSalary"]/text()').extract()[0].encode('utf8', 'replace')
        except:
            pass

        item['salary'] = salary
        item['salary_currency'] = None
        item['languages'] = None

        address = None
        try:
            address = sel.xpath('//dd/span[@itemprop="jobLocation"]/text()').extract()[0].encode('utf8', 'replace')
        except:
            pass

        item['address'] = address

        item.save(db)

        return item


