
from scrapy.spider import Spider
from scrapy.selector import Selector
# from scrapy.selector import HtmlXPathSelector
from jobs.items import JobsItem
from jobs.database import Database
import sys

'''
scrapy crawl monster_it -a url=http://jobsearch.monster.co.uk/search/IT-software-development_4 -a domain=jobview.monster.co.uk -a site_id=2
'''
class EjobsSpider(Spider):
    name = "monster_it"

    def __init__(self, url=None, domain=None, *args, **kwargs):
        super(Spider, self).__init__(*args, **kwargs)
        self.start_urls = [url]
        self.allowed_domains = [domain]

        if 'site_id' not in kwargs:
            raise Exception("No site_id specified")
        self.site_id = int(kwargs['site_id'])

    def parse(self, response):
        db = Database(self.settings.get('MYSQL'))
        sel = Selector(response)

        items = []
        links = sel.xpath('//div[@class="jobTitleContainer"]/a')

        for u in links:
            try:
                item = JobsItem()
                item['site_id'] = self.site_id
                item['title'] = u.xpath('text()').extract()[0]
                item['link'] = u.xpath('@href').extract()[0]
            except Exception, e:
                print "Could not parse {0} Exception {1}".format(u, e)

            item.save(db)

            items.append(item)

        return items
