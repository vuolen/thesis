import scrapy
from thesis_scraper.items import BaseItem
from urllib.parse import urlencode

class PythonDiscussSpider(scrapy.Spider):
    name = "python-discuss"
    allowed_domains = ["discuss.python.org"]
    start_urls = [
        "https://discuss.python.org/c/ideas/6.json",
        "https://discuss.python.org/c/peps/19.json",
        "https://discuss.python.org/c/core-dev/23.json"
    ]

    def parse(self, response):
        res = response.json()
        for topic in res['topic_list']['topics']:
            topicJsonUrl = f"/t/{topic['id']}.json"
            yield scrapy.Request(response.urljoin(topicJsonUrl), self.parseThread)
        if "more_topics_url" in res['topic_list']:
            jsonUrl = res['topic_list']['more_topics_url'].replace("?", ".json?")
            yield scrapy.Request(response.urljoin(jsonUrl), self.parse)


    def parseThread(self, response):
        res = response.json()
        postStream = res['post_stream']['stream']
        file_urls = []
        for i in range(0, len(postStream), 5):
            url = response.url.removesuffix(".json") + "/posts.json"
            query = urlencode({"post_ids[]": postStream[i:i+5]}, doseq=True)
            file_urls.append(url + "?" + query)
        yield BaseItem(
            name=res['title'],
            file_urls=file_urls
        )
        # https://discuss.python.org/t/3428/posts.json?post_ids[]=69609&post_ids[]=69746&post_ids[]=70756&post_ids[]=79129&post_ids[]=79146&post_ids[]=79149&post_ids[]=79162&post_ids[]=79168&post_ids[]=79170&post_ids[]=79345&post_ids[]=79398&post_ids[]=79402&post_ids[]=79412&post_ids[]=79781&post_ids[]=80073&post_ids[]=81262&post_ids[]=81263&post_ids[]=81264&post_ids[]=81265&post_ids[]=81268&include_suggested=false