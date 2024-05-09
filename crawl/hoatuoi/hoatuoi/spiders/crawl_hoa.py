import csv
import json
import os

import scrapy
from scrapy.crawler import CrawlerProcess


class CrawlHoaSpider(scrapy.Spider):
    name = "crawl_hoa"
    allowed_domains = ["hoayeuthuong.com"]
    start_urls = ["https://hoayeuthuong.com/hoa-tuoi/hoa-hong.aspx?ids=261"]
    hoas = []

    def parse(self, response, **kwargs):
        hoas = response.xpath('//*[@id="data_items"]/div')
        length = len(hoas)
        for i in range(1, length + 1):
            relative_url = response.xpath(f'//*[@id="data_items"]/div[{i}]/div[1]/a/@href').get()
            url = 'https://hoayeuthuong.com' + relative_url
            yield scrapy.Request(url, callback=self.parse_hoa)

    def parse_hoa(self, response):
        data = {
            'name': response.xpath('//*[@id="content"]/div/div[1]/div[2]/h2/text()').get(),
            'old_price': response.xpath('//*[@id="content"]/div/div[1]/div[2]/div[1]/span[1]/text()').get(),
            'price': response.xpath('//*[@id="content"]/div/div[1]/div[2]/div[1]/span[2]/text()').get(),
            'description': response.xpath('//*[@id="content"]/div/div[1]/div[2]/div[2]/text()').get(),
            'include': response.xpath('//*[@id="content"]/div/div[1]/div[2]/ul/li/text()').getall()
        }
        self.hoas.append(data)

    def close(self, reason):
        # directory = '/Users/levanduy/PycharmProjects/hoatuoi_ck/data/crawl_data'
        directory = '/opt/airflow/data/crawl_data'
        # Save as json
        if not os.path.exists(directory):
            os.makedirs(directory)
        with open(f'{directory}/hoatuoi.json', 'w') as f:
            json.dump(self.hoas, f, ensure_ascii=False)

        # Save as csv
        with open(f'{directory}/hoatuoi.csv', 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(
                ['name', 'old_price', 'price', 'description', 'include'])
            for hoa in self.hoas:
                writer.writerow([
                    hoa['name'],
                    hoa['old_price'],
                    hoa['price'],
                    hoa['description'],
                    hoa['include']
                ])

process = CrawlerProcess(settings={
    "CONCURRENT_REQUESTS": 3,
    "DOWNLOAD_TIMEOUT": 60,
    "RETRY_TIMES": 5,
    "ROBOTSTXT_OBEY": False,
    "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3",
})

process.crawl(CrawlHoaSpider)
process.start()