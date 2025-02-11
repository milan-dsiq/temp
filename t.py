import scrapy
import json
from urllib.parse import urlencode
from ..items import WalmartItems


API_KEY = ''
def get_scraperapi_url(url):
    payload = {'api_key': API_KEY, 'url': url, 'render': 'true'}
    proxy_url = 'http://api.scraperapi.com/?' + urlencode(payload)
    return proxy_url

class WalmartSpider(scrapy.Spider):
    name = "walmart"
    start_urls = [
        "https://www.walmart.com/browse/electronics/all-apple-ipad/3944_1229722_1229728_3998838"
    ]

    def start_requests(self):
        for url in self.start_urls:
            proxy_url = get_scraperapi_url(url)  # Get proxy URL
            yield scrapy.Request(
                proxy_url,
                # headers=self.settings.get("DEFAULT_REQUEST_HEADERS"),
                callback=self.parse
            )

    def parse(self, response):
        script_data = response.xpath('//script[@id="__NEXT_DATA__"]/text()').get()

        if script_data:
            # Parse the JSON data
            json_data = json.loads(script_data)
            search_data = json_data.get('props', {}).get('pageProps', {}).get('initialData', {}).get('searchResult', {}).get('itemStacks', [])
            items = search_data[0]["items"]
            for data in items:
                item = WalmartItems()
                item['name'] =  data.get("name")
                item['imageurl'] = data.get("image")
                item['linePrice'] = data.get("priceInfo", {}).get("linePrice")
                item['itemPrice'] = data.get("priceInfo", {}).get("itemPrice")
                item['productDetailPageUrl'] = data.get("canonicalUrl")
                item['sponsored'] = data.get("isSponsoredFlag", False) or (data.get("sponsoredProduct") is not None)
                item['variants'] = bool(data.get("variantList", []))
                item['ratings'] = data.get("rating", {})
                
                yield item
