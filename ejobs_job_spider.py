from scrapy.spider import Spider
from scrapy.selector import Selector

# from scrapy.selector import HtmlXPathSelector
from jobs.items import EjobJob
import re
import html2text
import datetime
import time
import sys
from jobs.database import Database


'''
scrapy crawl ejobs_job -a url=http://www.ejobs.ro/user/locuri-de-munca/junior-php-developer/604540/sqi
'''
class EjobsJobsSpider(Spider):
    name = "ejobs_job"
    allowed_domains = ["ejobs.ro"]

    def __init__(self, url=None, *args, **kwargs):
        super(Spider, self).__init__(*args, **kwargs)
        self.url = url
        self.start_urls = [url]

    def parse(self, response):
        db = Database(self.settings.get('MYSQL'))
        con = db.getConnection()
        cur = db.getCursor()
        try:
            cur.execute("SELECT u.id as id, s.domain, s.category_id FROM urls u LEFT JOIN sites s ON s.id=u.site_id "
                        "WHERE u.url=%s", (self.url, ))
            row = cur.fetchone()
            if row is None:
                raise Exception("No url found in DB: {0}".format(self.url))
            url_id = row[0]
            domain_name = row[1]
            category_id = row[2]
        except Exception, e:
            print cur._last_executed
            print e
            raise


        sel = Selector(response)
        item = EjobJob()
        item['updated'] = datetime.datetime.fromtimestamp(int(time.time())).strftime('%Y-%m-%d %H:%M:%S')
        item['url_id'] = url_id
        item['category_id'] = category_id
        try:
            # item['title'] = sel.xpath('//h1[@class="times_33"]/text()').extract()[0]
            title = sel.xpath('//h1[@class="times_33"]').extract()[0]
            item['title'] = re.sub('<[^<]+?>', '', title)
            item['title'] = item['title'].encode('utf8', 'replace')
            item['link'] = self.start_urls[0]
            try:
                company = sel.xpath('//span[@itemprop="name"]/text()').extract()[0]
            except:
                try:
                    company = sel.xpath('//td[@class="info_company_normal"]/text()').extract()[0]
                except:
                    company = 'N/A'

            item['company'] = company.encode('utf8', 'replace')
        except Exception, e:
            try:
                error = "Could not find title, link, company: {0}, HTTP response {1}".format(e, response.status)
                cur.execute("UPDATE urls SET status=3, updated_at=%s, error=%s WHERE id=%s", (item['updated'], error, item['url_id'], ) )
                con.commit()
            except Exception, ex1:
                print "Could not update the url status"
                print ex1

            return None

        item['valability'] = None
        try:
            valability = sel.xpath('//div[@id="data_pub_valab_left"]/text()').extract()[0]
            m = re.findall(r'\d+\.\d+\.\d+', valability)
            valability = m[0]
            dt = datetime.datetime.strptime(valability, '%d.%m.%Y').timetuple()
            sql_valability = '{0}-{1}-{2} 00:00:00'.format(dt.tm_year, dt.tm_mon, dt.tm_mday)
            item['valability'] = sql_valability
        except:
            pass

        job_types = sel.xpath('//ul[@itemprop="employmentType"]')
        for job_type in job_types:
            type_list = job_type.xpath('li/text()').extract()

        item['job_type'] = ','.join(type_list)

        try:
            item['salary'] = sel.xpath('//span[@itemprop="baseSalary"]/text()').extract()[0]
            item['salary_currency'] = sel.xpath('//span[@itemprop="salaryCurrency"]/text()').extract()[0]
        except:
            item['salary'] = None
            item['salary_currency'] = None

        try:
            multi_addresses = []
            addresses = sel.xpath('//span[@itemprop="jobLocation"]/ul[@itemprop="addressLocality"]/li')
            for addr in addresses:
                try:
                    city = addr.xpath('a/text()').extract()[0]
                    multi_addresses.append(city.encode('utf8', 'replace'))
                except:
                    try:
                        city = addr.xpath('text()').extract()[0]
                        multi_addresses.append(city.encode('utf8', 'replace'))
                    except:
                        pass
            item['address'] = ', '.join(multi_addresses)
        except:
            item['address'] = None

        try:
            posted = sel.xpath('//span[@itemprop="datePosted"]/text()').extract()[0]
            dt = datetime.datetime.strptime(posted, '%d.%m.%Y').timetuple()
            sql_posted = '{0}-{1}-{2} 00:00:00'.format(dt.tm_year, dt.tm_mon, dt.tm_mday)
            item['posted_at'] = sql_posted
        except:
            item['posted_at'] = None

        converter = html2text.HTML2Text()
        converter.ignore_links = True
        desc = ''
        trs = sel.xpath('//div[@itemtype="http://schema.org/JobPosting"]/table/tr')

        for tr in trs:
            desc_list = tr.xpath('td[@class="arial_13"]').extract()
            if len(desc_list):
                desc += desc_list[0]

        item['description'] = converter.handle(desc).encode('utf8', 'replace')

        item.save(db)

        return item
