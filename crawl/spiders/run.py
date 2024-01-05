import time
from pathlib import Path
from urllib.parse import urlparse

import scrapy


class MySpider(scrapy.Spider):
    name = "myspider"
    allowed_domains = ["mwu.sachsen-anhalt.de"]
    start_urls = ["https://mwu.sachsen-anhalt.de/umwelt/wasser/hochwasserschutz"]

    # Create the output directory
    output_directory = Path("output")
    output_directory.mkdir(exist_ok=True)

    # Counter to limit the number of pages
    crawled_pages = 0

    def parse(self, response):

        # start the time measurement
        start_time = time.time()
        # Check if the response is HTML, and not a PDF or PNG
        content_type = response.headers.get("Content-Type").decode("utf-8")
        if "text/html" in content_type:
            # Save page as HTML file
            page = urlparse(response.url).path.split("/")[-1] or "index"
            filename = self.output_directory / f"{page}.html"
            with open(filename, "wb") as f:
                f.write(response.body)
            self.log(f"Saved file {filename}")

            # Increase the counter
            self.crawled_pages += 1

            # Stop if 50 pages have been crawled
            if self.crawled_pages >= 50:
                # Close the spider
                self.crawler.engine.close_spider(self, "Reached page limit")
                return

            # Extract all links and preprocess them
            links = response.css("a::attr(href)").getall()
            for link in links:
                # Remove URL fragments and query parameters
                link = urlparse(link)._replace(fragment="", query="").geturl()
                # Create an absolute URL
                absolute_url = response.urljoin(link)
                # Do not process PDF or PNG links
                if not absolute_url.lower().endswith((".pdf", ".png")):
                    yield scrapy.Request(absolute_url, callback=self.parse)

        # Print the time needed to process the page
        end_time = time.time()
        self.log(f"Processing time: {end_time - start_time:.2f} seconds")
