import scrapy
import json
from urllib.parse import urlencode
from errors.items import WalmartItems
from logs.error_handler import ErrorManager 


API_KEY = ''
def get_scraperapi_url(url):
    payload = {'api_key': API_KEY, 'url': url, 'render': 'true'}
    proxy_url = 'http://api.scraperapi.com/?' + urlencode(payload)
    return proxy_url

class WalmartSpider(scrapy.Spider):
    name = "walmart"
    start_urls = [
        "https://www.walmart.com/browse/electronics/all-apple-ipad/3944_1229722_1229728_3998838/?page={page}"
    ]
    def __init__(self, *args, **kwargs):
        super(WalmartSpider, self).__init__(*args, **kwargs)
        self.error_manager = ErrorManager()
    def start_requests(self):
        for url in self.start_urls:
            for i in range(1, 6):
                pageurl = url.format(page=i)
                proxy_url = get_scraperapi_url(pageurl)  # Get proxy URL
                yield scrapy.Request(
                    proxy_url,
                    callback=self.parse,                
                    # errback=lambda failure: self.error_manager.handle_request_failure(failure, self.name)
                )

    def parse(self, response):
        script_data = response.xpath('//script[@id="__NEXT_DATA__"]/text()').get()

        if script_data:
            # Parse the JSON data
            json_data = json.loads(script_data)
            search_data = json_data.get('props', {}).get('pageProps', {}).get('initialData', {}).get('searchResult', {}).get('itemStacks', [])
            items = search_data[0]["items"]
            # print(items)
            for data in items:
                item = WalmartItems()
                # print(data)
                item['name'] =  data.get("name")
                item['imageurl'] = data.get("image")
                item['linePrice'] = data.get("priceInfo", {}).get("linePrice")
                item['itemPrice'] = data.get("priceInfo", {}).get("itemPrice")
                item['productDetailPageUrl'] = data.get("canonicalUrl")
                item['sponsored'] = data.get("isSponsoredFlag", False) or (data.get("sponsoredProduct") is not None)
                item['variants'] = bool(data.get("variantList", []))
                item['buyBoxSuppression'] = data.get("buyBoxSuppression")
                item['catalogSellerId'] = data.get("catalogSellerId")
                item['shortDescription'] = data.get("shortDescription")
                item['badges'] = data.get("badges")
                item['salesUnitType'] = data.get("salesUnitType")
                item['sellerId'] = data.get("sellerId")
                item['sellerName'] = data.get("sellerName")
                item['hasSellerBadge'] = data.get("hasSellerBadge")
                item['imageInfo'] = data.get("imageInfo")
                item['ratings'] = data.get("rating", {})
