import json
from pathlib import Path
from urllib.parse import urlparse

import scrapy


class ExtractorSpider(scrapy.Spider):
    name = "extractor"
    allowed_domains = ["sachsen-anhalt.de"]
    start_urls = ["https://mwu.sachsen-anhalt.de/umwelt/wasser/hochwasserschutz"]

    output_extracted_directory = Path.cwd() / "output_extracted"
    output_extracted_directory.mkdir(exist_ok=True)

    def parse(self, response):
        content_type = response.headers.get("Content-Type").decode("utf-8")
        if "text/html" in content_type:
            # Create a dictionary to store headers and associated text
            header_text_pairs = self.extract_headers_with_text(response)

            data = {"url": response.url, "content": header_text_pairs}

            page = urlparse(response.url).path.split("/")[-1] or "index"
            filename = self.output_extracted_directory / f"{page}.json"

            with open(filename, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=4)

            self.log(f"Saved extracted data to {filename}")

            links = response.css("a::attr(href)").getall()
            for link in links:
                link = urlparse(link)._replace(fragment="", query="").geturl()
                absolute_url = response.urljoin(link)
                if not absolute_url.lower().endswith((".pdf", ".png")):
                    yield scrapy.Request(absolute_url, callback=self.parse)

    def extract_headers_with_text(self, response):
        # This function creates a dictionary with headers as keys and the following text as values
        header_text_pairs = {}
        current_header = None

        for node in response.css("h1, h2, h3, h4, h5, h6, p"):
            if node.root.tag in ["h1", "h2", "h3", "h4", "h5", "h6"]:
                current_header = "".join(node.css("::text").extract()).strip()
                header_text_pairs[current_header] = []
            elif node.root.tag == "p" and current_header:
                text = "".join(node.css("::text").extract()).strip()
                if text:
                    header_text_pairs[current_header].append(text)

        # Filter out any headers that did not collect any text
        header_text_pairs = {header: texts for header, texts in header_text_pairs.items() if texts}

        return header_text_pairs
