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

class WalmartPDPSpider(scrapy.Spider):
    name = "walmartPDP"
    start_urls = [
        "https://www.walmart.com/ip/Annual-Event-Standard-Bundle/12941505136?classType=REGULAR&athbdg=L1600"
    ]
    def __init__(self, *args, **kwargs):
        super(WalmartPDPSpider, self).__init__(*args, **kwargs)
        self.error_manager = ErrorManager()
    def start_requests(self):
        for url in self.start_urls:
            proxy_url = get_scraperapi_url(url)  # Get proxy URL
            yield scrapy.Request(
                proxy_url,
                callback=self.parse,                
                errback=lambda failure: self.error_manager.handle_request_failure(failure, self.name)
            )

    def parse(self, response):
        script_data = response.xpath('//script[@id="__NEXT_DATA__"]/text()').get()

        if script_data:
            # Parse the JSON data
            json_data = json.loads(script_data)
            test = []
            my_data = json_data.get('props', {}).get('pageProps', {}).get('initialData', {}).get('data', {})
            
            try:
                test.append({
                "primaryProductId": my_data.get("product", {}).get("primaryProductId", {}),
                "abstractProductId": my_data.get("product", {}).get("abstractProductId", {}),
                "primaryUsItemId": my_data.get("product", {}).get("primaryUsItemId", {}),
                "productTypeId": my_data.get("product", {}).get("productTypeId",{}),
                "title": my_data.get("product", {}).get("name", {}),
                "short_description": my_data.get("idml", {}).get("shortDescription", {}),
                "long_description": my_data.get("idml", {}).get("longDescription", {}),
                "category": my_data.get("product", {}).get("category", {}),
                "brand": my_data.get("product", {}).get("brand", {}),
                "brandUrl": my_data.get("product", {}).get("brandUrl", {}),
                "warranty": my_data.get("idml", {}).get("warranty", {}),
                "warnings": my_data.get("idml", {}).get("warnings", {}),
                "nutritionFacts": my_data.get("idml", {}).get("nutritionFacts", {}),
                "badges": my_data.get("product", {}).get("badges", {}),
                "priceInfo": my_data.get("product", {}).get("priceInfo", {}),
                "sellerName": my_data.get("product", {}).get("sellerName", {}),
                "sellerURL": my_data.get("product", {}).get("sellerStoreFrontURL", {}),
                "images": my_data.get("product", {}).get("imageInfo", {}),
                "interactiveProductVideo": my_data.get("idml", {}).get("interactiveProductVideo",{}),
                "variantCriteria": my_data.get("product", {}).get("variantCriteria", {}),
                "productDetailPageUrl": my_data.get("product", {}).get("canonicalUrl", {}),
                "subscriptionPrice": my_data.get("product", {}).get("subscriptionPrice"),
                "breadCrumbs": my_data.get("seoItemMetaData", {}).get("breadCrumbs", {}),
                "totalReviewCount": my_data.get("reviews", {}).get("totalReviewCount", {}),
                "averageOverallRating": my_data.get("reviews", {}).get("averageOverallRating", {}),
                # "sponserdAd": my_data.get("contentLayout", {}).get("modules", {})[4]
            })
            except Exception as e:
                self.error_manager.log_parsing_error(response, {str(e)}, self.name)
            with open("final_data.json", "a", encoding="utf-8") as f:
                json.dump(test[0], f, indent=4, ensure_ascii=False)
                f.write("\n")  # New line for each page’s data
            # items = search_data[0]["items"]
            # print(items)
            # for data in items:
            #     item = WalmartItems()
            #     # print(data)
            #     item['name'] =  data.get("name")
            #     item['imageurl'] = data.get("image")
            #     item['linePrice'] = data.get("priceInfo", {}).get("linePrice")
            #     item['itemPrice'] = data.get("priceInfo", {}).get("itemPrice")
            #     item['productDetailPageUrl'] = data.get("canonicalUrl")
            #     item['sponsored'] = data.get("isSponsoredFlag", False) or (data.get("sponsoredProduct") is not None)
            #     item['variants'] = bool(data.get("variantList", []))
            #     item['ratings'] = data.get("rating", {})
            #     yield item
            
