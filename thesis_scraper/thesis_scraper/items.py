# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class BaseItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    id = scrapy.Field()
    name = scrapy.Field()
    file_urls = scrapy.Field()
    files = scrapy.Field()
    scraped_at = scrapy.Field()
    spider_name = scrapy.Field()