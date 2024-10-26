import itertools
import re

import scrapy
from scrapy.utils.response import open_in_browser


def batched(iterable, n):
    "Batch data into tuples of length n. The last batch may be shorter."
    # batched('ABCDEFG', 3) --> ABC DEF G
    if n < 1:
        raise ValueError("n must be at least one")
    it = iter(iterable)
    while batch := tuple(itertools.islice(it, n)):
        yield batch


class CriminalSpider(scrapy.Spider):
    name = "criminal"
    allowed_domains = ["sci.ccc.nashville.gov"]

    def start_requests(self):
        for case_number in self.case_numbers(2024):
            print(case_number)
            case_number = "2024-C-1386"
            yield scrapy.FormRequest(
                "https://sci.ccc.nashville.gov/Search/SearchWarrant",
                formdata={"warrantNumber": case_number},
                callback=self.parse_search_results,
            )
            break

    def case_numbers(self, year):
        for serial in range(1, 9999):
            case_number = f"{year}-C-{serial:04}"
            yield case_number

    def parse_search_results(self, response):
        result_table = response.xpath('//table[@class="warrant-number-results"]')
        no_results = result_table.xpath('.//td[class="dataTables_empty"]')
        if not no_results:
            for row in result_table.xpath(".//tbody/tr"):
                link = row.xpath("./td[1]/a")
                href = link.attrib["href"]
                case_number = link.xpath("./text()").get().strip()

                yield response.follow(
                    href,
                    callback=self.parse_case_page,
                    cb_kwargs={"case_number": case_number},
                )

    def parse_case_page(self, response, case_number):
        name_link = response.xpath('..//a[@class="defendant-name-link"]')

        name = name_link.xpath(".//text()").get().strip()
        criminal_history_url = response.urljoin(name_link.attrib["href"])

        result_section = response.xpath('..//div[@class="results-title"]').get()
        date_of_birth = re.search(r"Date of Birth: (?P<dob>[\d/]*)", result_section)[
            "dob"
        ]
        oca = re.search(r"OCA Number:</span> (?P<oca>\d*)", result_section)["oca"]

        charges_section = response.xpath(
            './/ul[li[normalize-space(text())="Charged/Cited Offense"]]'
        )
        charges_elements = [
            each.strip() for each in charges_section.xpath(".//li//text()").getall()
        ]

        charges = []

        if len(charges_elements) % 12 != 0:
            breakpoint()

        for charge_elements in batched(charges_elements, 12):
            charge_details = {}

            _, a, charge, b, count, c, d, amended, e, convicted, f, disposition = (
                charge_elements
            )
            if not a == b == c == d == e == f == "":
                breakpoint()

            charge_details["charge"] = charge
            charge_details["count"] = int(
                re.search(r"Count (?P<count>\d*)", count)["count"]
            )
            if amended == "Amended:":
                charge_details["amended"] = False
            else:
                breakpoint()

            if convicted == "Convicted:":
                charge_details["convicted"] = None
            else:
                breakpoint()

            if disposition == "Disposition:":
                charge_details["disposition"] = None
            else:
                breakpoint()

            charges.append(charge_details)

        breakpoint()
