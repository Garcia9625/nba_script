import scrapy


class InfoSpider(scrapy.Spider):
    name = "info"
    allowed_domains = ["www.spotrac.com"]
    headers = {
        'accept-language': 'en-US,en;q=0.9,ar;q=0.8',
        'pragma': 'no-cache',
        'priority': 'u=0, i',
        'sec-ch-ua': '"Chromium";v="136", "Microsoft Edge";v="136", "Not.A/Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'none',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.101.76 Safari/537.36'
    }
    def start_requests(self):
        with open('links.txt', 'r') as f:
            lines = f.read().splitlines()

        for line in lines:
            url = line.split('\t')[0]
            team_name = line.split('\t')[1]
            yield scrapy.Request(url=url, callback=self.parse, headers=self.headers, meta={'team_name': team_name}, dont_filter=True)

    def parse(self, response):
        # breakpoint()
        title = ''.join(response.xpath('//h1[@id="team-name-logo"]//text()').getall()).strip()
        year = ''.join(response.xpath('(//span[@class="years"])[1]/text()').getall()).strip()
        contract_info = ''.join(response.xpath('(//span[@class="years"])[1]/small/text()').getall()).strip()
        cap_hit = ''.join(response.xpath("//h5[contains(text(),'Cap Hit')]//following-sibling::p/text()").getall()).strip()

        yield {
            'title': title,
            'team_name': response.meta['team_name'],
            'year': year,
            'contract_info': contract_info,
            'cap_hit': cap_hit,
            'url': response.url
        }
