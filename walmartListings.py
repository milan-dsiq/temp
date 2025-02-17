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


# method for getting headers and query for sponsor video on the page 
def get_headers_and_query_for_sponsorVideoAd(url, cat_id, cat_path_name):
    headers = {
    "accept": "application/json",
    "accept-language": "en-US",
    "ads-module-type": "SponsoredVideoAd",
    "baggage": "trafficType=customer,deviceType=desktop,renderScope=SSR,webRequestSource=Browser,pageName=browseResults,isomorphicSessionId=-uO4_-56_0Y6z2uS3CPKs,renderViewId=d53a3a74-f856-44e1-af99-837c0887eba2",
    "cache-control": "no-cache",
    "content-type": "application/json",
    "downlink": "10",
    "dpr": "1",
    "origin": "https://www.walmart.com",
    "pragma": "no-cache",
    "priority": "u=1, i",
    "referer": url,#this is compulsory for getting the exact video
    "sec-ch-ua": '"Not(A:Brand";v="99", "Google Chrome";v="133", "Chromium";v="133"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "Windows",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "tenant-id": "elh9ie",
    "traceparent": "00-1824e6d341c24e5ba9a0f9c16cf2e865-5c103211cc09e13a-00",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
    "wm_mp": "true",
    "wm_page_url": url, #this is also
    "wm_qos.correlation_id": "XZTG0w2HLigW-Iw5QfE4J8SutYi-MdNs9pm9",
    "x-apollo-operation-name": "AdV2",
    "x-enable-server-timing": "1",
    "x-latency-trace": "1",
    "x-o-bu": "WALMART-US",
    "x-o-ccm": "server",
    "x-o-correlation-id": "XZTG0w2HLigW-Iw5QfE4J8SutYi-MdNs9pm9",
    "x-o-gql-query": "query AdV2",
    "x-o-mart": "B2C",
    "x-o-platform": "rweb",
    "x-o-platform-version": "us-web-1.179.0-b604c2b40cc2c3fd9027cd7a2ef2e2952432aa6f-021019",
    "x-o-segment": "oaoh"
    }
    json_data = {
        'query': 'query AdV2( $platform:Platform! $pageId:String! $pageType:PageType! $tenant:String! $moduleType:ModuleType! $pageContext:PageContextIn $locationContext:LocationContextIn $moduleConfigs:JSON $adsContext:AdsContextIn $adRequestComposite:AdRequestCompositeIn $enableRxDrugScheduleModal:Boolean = false $enableAdsPromoData:Boolean = false $enableSignInToSeePrice:Boolean = false $enableItemLimits:Boolean = false ){adV2( platform:$platform pageId:$pageId pageType:$pageType tenant:$tenant moduleType:$moduleType locationContext:$locationContext pageContext:$pageContext moduleConfigs:$moduleConfigs adsContext:$adsContext adRequestComposite:$adRequestComposite ){status adContent{type model title displayTitle data{__typename...AdDataDisplayAdFragment __typename...AdDataSponsoredProductsFragment __typename...AdDataSponsoredVideoFragment}}}}fragment AdDataDisplayAdFragment on AdData{...on DisplayAd{json status}}fragment AdDataSponsoredProductsFragment on AdData{...on SponsoredProducts{adUuid adExpInfo moduleInfo products{...ProductFragment}}}fragment ProductFragment on Product{usItemId offerId specialCtaType @include(if:$enableSignInToSeePrice) orderMinLimit @include(if:$enableItemLimits) orderLimit @include(if:$enableItemLimits) badges{flags{__typename...on BaseBadge{id text key query type styleId}...on PreviouslyPurchasedBadge{id text key lastBoughtOn numBought criteria{name value}}}labels{__typename...on BaseBadge{id text key}...on PreviouslyPurchasedBadge{id text key lastBoughtOn numBought}}tags{__typename...on BaseBadge{id text key}}groups{__typename name members{...on BadgeGroupMember{__typename id key memberType rank slaText styleId text type}...on CompositeGroupMember{__typename join memberType styleId suffix members{__typename id key memberType rank slaText styleId text type}}}}groupsV2{name flow pos members{memType memId memStyleId fbMemStyleId content{type value styleId fbStyleId contDesc url actionId}}}}priceInfo{priceDisplayCodes{rollback reducedPrice eligibleForAssociateDiscount clearance strikethrough submapType priceDisplayCondition unitOfMeasure pricePerUnitUom}currentPrice{price priceString priceDisplay}wasPrice{price priceString}listPrice{price priceString}priceRange{minPrice maxPrice priceString}unitPrice{price priceString}savingsAmount{priceString}comparisonPrice{priceString}subscriptionPrice{priceString subscriptionString price minPrice maxPrice intervalFrequency duration percentageRate durationUOM interestUOM}wPlusEarlyAccessPrice{memberPrice{price priceString priceDisplay}savings{amount priceString}eventStartTime eventStartTimeDisplay}}preOrder{streetDate streetDateDisplayable streetDateType isPreOrder preOrderMessage preOrderStreetDateMessage}annualEventV2 earlyAccessEvent isEarlyAccessItem eventAttributes{priceFlip specialBuy}snapEligible showOptions promoData @include(if:$enableAdsPromoData){type templateData{priceString imageUrl}}sponsoredProduct{spQs clickBeacon spTags}canonicalUrl conditionV2{code groupCode}numberOfReviews averageRating availabilityStatus imageInfo{thumbnailUrl allImages{id url}}name fulfillmentBadge classType type showAtc brand sellerId sellerName sellerType rxDrugScheduleType @include(if:$enableRxDrugScheduleModal)}fragment AdDataSponsoredVideoFragment on AdData{...on SponsoredVideos{adUuid adExpInfo moduleInfo videos{video{vastXml thumbnail spqs}products{...ProductFragment}}}}',
        'variables': {
            'adRequestComposite': {
                'adUuid': '6345a3e8-9ce2-41aa-bb4f-7e092ae0a3d6',#this id is random each time we don't need to generate
                'categoryId': '',
            },
            'adsContext': {
                'dedupeList': [],
            },
            'pageContext': {
                'browseContext': {
                    'verticalId': 'consumables',
                    'analytics_log': {
                        'fe_log': {
                            'mso': 2,
                            'msf': 2,
                            'msov': 0,
                            'ms': 0,
                        },
                    },
                    'cat_id': cat_id,#cat_id 
                    'cat_path_name': cat_path_name,#cat_path_name both are unique for each browse nodes
                },
                'customerContext': {
                    'customerId': None,
                    'isPaidMember': False,
                    'isActiveMember': False,
                    'purseTags': [],
                    'paymentMethodMetaData': [],
                },
            },
            'pageId': f"'{'/'.join(url.rstrip('/').split('/')[-1:])}'",# this is the last part of the url
            'pageType': 'BROWSE',
            'platform': 'DESKTOP',
            'tenant': 'WM_GLASS',
            'locationContext': {
                'storeId': '3081',
                'stateCode': 'CA',
                'zipCode': '95829',
            },
            'moduleConfigs': {
                'moduleLocation': 'middle',
                'lazy': '700',
            },
            'moduleType': 'SponsoredVideoAd',
            'enableSignInToSeePrice': False,
            'enableItemLimits': False,
        },
    }
    return {
        'headers': headers,
        'json_data': json_data,
    }


class WalmartSpider(scrapy.Spider):
    name = "walmart"
    start_urls = [
        # "https://www.walmart.com/browse/electronics/all-apple-ipad/3944_1229722_1229728_3998838/?page={page}",
        "https://www.walmart.com/browse/electronics/shop-tvs-by-size/3944_1060825_2489948/?page={page}",
        "https://www.walmart.com/browse/health-and-medicine/fiber-supplements/976760_1396434_2586366/?page={page}",
        
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
                    url=proxy_url,
                    callback=self.parse,        
                    errback=lambda failure: self.error_manager.handle_request_failure(failure, self.name)
                )

    def parse(self, response):
        script_data = response.xpath('//script[@id="__NEXT_DATA__"]/text()').get()
        if script_data:
            # Parse the JSON data
            json_data = json.loads(script_data)
            useful_data = json_data.get('props', {}).get('pageProps', {}).get('initialData', {})
            #call the function to get the sponser video data
            original_url= response.headers.get('Sa-Final-Url').decode("utf-8")#to call the funtion we need original url, we get from headers of scraper api
            if 'page=1' in original_url:
                browse_context = useful_data.get('pageMetadata').get('pageContext').get('browseContext')#data needed for querying and headers of sponser data
                headers_and_query = get_headers_and_query_for_sponsorVideoAd(original_url, cat_id=browse_context.get('cat_id',{}),cat_path_name=browse_context.get('cat_path_name',{}))
                yield scrapy.Request(
                    url="https://www.walmart.com/orchestra/home/graphql",
                    method="POST",
                    headers=headers_and_query.get('headers'),
                    body=json.dumps(headers_and_query.get('json_data')),
                    callback=self.sponserDataParse,      
                    dont_filter=True,          
                    errback=lambda failure: self.error_manager.handle_request_failure(failure, self.name)
                )


            #continue scraping original data 
            items = useful_data.get('searchResult', {}).get('itemStacks', [])[0]["items"]
            for data in items:
                item = WalmartItems()
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
                yield item
            

    def sponserDataParse(self,response):
        data = response.json()
        # Save JSON data to a file (append to existing file)
        with open("sponsorData.json", "a", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
            f.write("\n")  # New line for each page’s data
