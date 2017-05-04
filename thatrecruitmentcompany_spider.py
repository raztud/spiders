from scrapy.spider import Spider
from scrapy.selector import Selector
# from scrapy.selector import HtmlXPathSelector
from jobs.items import JobsItem
from jobs.database import Database

'''
scrapy crawl therecruitmentcompany -a url=http://www.thatrecruitmentcompany.com/jobs/ -a domain=thatrecruitmentcompany.com -a site_id=4
'''
class ThatrecruitmentcompanySpider(Spider):
    name = "thatrecruitmentcompany"
    # allowed_domains = ["software.ejobs.ro"]
    # start_urls = [
    #     "http://software.ejobs.ro/",
    # ]

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
        job_table = sel.xpath('//table[@id="wpjb-job-list"]')
        jobs = job_table.xpath('//tr/td/a')

        items = []
        skip_links = []

        for job in jobs:
            item = JobsItem()
            item['site_id'] = self.site_id
            title = job.xpath('text()').extract()
            href = job.xpath('@href').extract()
            if len(title):
                for word in skip_links:
                    if word in link:
                        print "Skipping {0}".format(link)
                        continue

                item['title'] = title[0].strip().encode('utf8')
                link = href[0]

                item['link'] = link

                item.save(db)

                items.append(item)

        return items
