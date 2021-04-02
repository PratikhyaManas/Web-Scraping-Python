

import scrapy


class NewzzItem(scrapy.Item):

    publication = scrapy.Field()
    title = scrapy.Field()
    author = scrapy.Field()
    story = scrapy.Field()
    url = scrapy.Field()
