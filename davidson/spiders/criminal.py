import itertools
import re

import requests
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

        for year in range(2000, 2025):
            ranges = change_points(year)

            for grouping, serial_range in ranges.items():
                for serial in range(serial_range["start"], serial_range["end"]):
                    case_number = f"{year}-{grouping}-{serial}"
                    yield scrapy.FormRequest(
                        "https://sci.ccc.nashville.gov/Search/SearchWarrant",
                        formdata={"warrantNumber": case_number},
                        callback=self.parse_search_results,
                    )

    def parse_search_results(self, response):
        result_table = response.xpath('//table[@class="warrant-number-results"]')
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
        case_details = {"case_number": case_number}

        name_link = response.xpath('..//a[@class="defendant-name-link"]')

        case_details["name"] = name_link.xpath(".//text()").get().strip()
        case_details["criminal_history_url"] = response.urljoin(
            name_link.attrib["href"]
        )

        case_status, defendant_status, *_ = [
            each.strip()
            for each in response.xpath('.//span[@class="case-status"]/text()').getall()
        ]
        case_details["case_status"] = case_status.removeprefix("Case Status: ")
        case_details["defendant_status"] = case_status.removeprefix(
            "Defendant Status: "
        )

        result_section = response.xpath('..//div[@class="results-title"]').get()
        case_details["date_of_birth"] = re.search(
            r"Date of Birth: (?P<dob>[\d/]*)", result_section
        )["dob"]

        try:
            case_details["oca"] = re.search(
                r"OCA Number:</span> (?P<oca>\d*)", result_section
            )["oca"]
        except TypeError:
            case_details["oca"] = None

        case_details["charges"] = _charges(response)

        return case_details


def _charges(response):
    charges_section = response.xpath(
        './/ul[li[normalize-space(text())="Charged/Cited Offense"]]'
    )
    charges_elements = [
        each.strip() for each in charges_section.xpath(".//li//text()").getall()
    ]

    charges = []

    if len(charges_elements) % 12 != 0:
        if charges_elements[2] == "" or "HEARING" in charges_elements[2]:
            return charges
        else:
            breakpoint()

    for charge_elements in batched(charges_elements, 12):
        charge_details = {}

        _, a, charge, b, count, c, d, amended, e, convicted, f, disposition = (
            charge_elements
        )
        if not a == b == c == d == e == f == "":
            breakpoint()

        charge_details["charge"] = charge
        count = re.search(r"Count (?P<count>\d*)", count)["count"]
        if count:
            charge_details["count"] = int(count)
        else:
            charge_details["count"] = None

        if amended == "Amended:":
            charge_details["amended"] = False
        else:
            charge_details["amended"] = amended.removeprefix("Amended:").strip()

        if convicted == "Convicted:":
            charge_details["convicted"] = None
        else:
            charge_details["convicted"] = convicted.removeprefix("Convicted:").strip()

        if disposition == "Disposition:":
            charge_details["disposition"] = None
        else:
            charge_details["disposition"] = disposition.removeprefix("Disposition: ")

        charges.append(charge_details)

    return charges


def change_points(year):

    ranges = {}

    mid = 1

    for grouping in "ABCDI":

        if grouping == "I":
            low = 1
        else:
            low = mid

        high = 9999

        ranges[grouping] = {"start": low}

        while low < high:
            mid = (low + high) // 2
            case_number = f"{year}-{grouping}-{mid}"

            response = requests.post(
                "https://sci.ccc.nashville.gov/Search/SearchWarrant",
                data={"warrantNumber": case_number},
            )
            case_found = "/Search/CaseSearchDetails" in response.text

            if case_found:
                low = mid + 1
            else:
                high = mid

        ranges[grouping]["end"] = mid

    return ranges
