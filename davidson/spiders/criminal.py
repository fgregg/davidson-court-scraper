import functools
import itertools
import re

import requests
import scrapy
from scrapy.utils.response import open_in_browser


class CriminalSpider(scrapy.Spider):
    name = "criminal"
    allowed_domains = ["sci.ccc.nashville.gov"]

    def __init__(self, year=2024, **kwargs):

        super().__init__(**kwargs)
        self.year = year

    def start_requests(self):

        ranges = change_points(self.year)

        for grouping, (start, end) in ranges.items():
            for serial in range(start, end):
                case_number = f"{self.year}-{grouping}-{serial}"
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

            case_details = {
                "case_number": link.xpath("./text()").get().strip(),
                "first_name": row.xpath("./td[2]/a/text()").get().strip(),
                "last_name": row.xpath("./td[3]/a/text()").get().strip(),
            }

            yield response.follow(
                href,
                callback=self.parse_case_page,
                cb_kwargs={"case_details": case_details},
            )

    def parse_case_page(self, response, case_details):

        case_details["case_url"] = response.url

        name_link = response.xpath('..//a[@class="defendant-name-link"]')

        case_details["full_name"] = name_link.xpath(".//text()").get().strip()
        case_details["criminal_history_url"] = response.urljoin(
            name_link.attrib["href"]
        )

        case_status, defendant_status, fees_owed, amount, *_ = (
            each.strip()
            for each in response.xpath('.//span[@class="case-status"]//text()').getall()
        )
        case_details["case_status"] = case_status.removeprefix("Case Status: ")
        case_details["defendant_status"] = defendant_status.removeprefix(
            "Defendant Status: "
        )
        if fees_owed:
            case_details["fees_owed"] = amount
        else:
            case_details["fees_owed"] = None

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
        return charges

    for charge_elements in batched(charges_elements, 12):
        charge_details = {}

        _, _, charge, _, count, _, _, amended, _, convicted, _, disposition = (
            charge_elements
        )

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


def batched(iterable, n):
    "Batch data into tuples of length n. The last batch may be shorter."
    # batched('ABCDEFG', 3) --> ABC DEF G
    if n < 1:
        raise ValueError("n must be at least one")
    it = iter(iterable)
    while batch := tuple(itertools.islice(it, n)):
        yield batch


def change_points(year):
    """
    The criminal case numbers in Davidson County have a suprising
    pattern.

    The case numbers have a {year}-{grouping}-{serial} pattern, where
    grouping is 'A', 'B', 'C', 'D', or 'I'.

    For, 'A', 'B', 'C', and 'D', the serial number starts with 1 and
    increases sequentially across all the groupings in a
    year. However, when the grouping switches from 'A' to 'B', 'B' to
    'C', etc is not predictable.

    We know that {year}-A-1 is always a valid case number and we know
    that the maximum case number is less than 9999, so we use a
    bisection algorithm to find the largest serial for the 'A'
    grouping. That also lets us know the smallest serial for the 'B'
    grouping, and if we know the smallest valid serial for the 'B'
    grouping, we can use a bisection algorithm the largest serial for
    that grouping, and so on.

    The 'I' grouping is different, in that its serial is not tied to
    the other groupings.

    """

    ranges = {}

    start = 1
    for grouping in ("A", "B", "C", "D"):
        key = functools.partial(_case_exists, year, grouping)

        end = _bisect(start, 9999, key)

        ranges[grouping] = (start, end)

        start = end

    ranges["I"] = (1, _bisect(1, 9999, functools.partial(_case_exists, year, "I")))

    return ranges


def _case_exists(year, grouping, serial):
    case_number = f"{year}-{grouping}-{serial}"

    response = requests.post(
        "https://sci.ccc.nashville.gov/Search/SearchWarrant",
        data={"warrantNumber": case_number},
    )

    return "/Search/CaseSearchDetails" in response.text


def _bisect(low, high, key):

    while low < high:
        mid = (low + high) // 2

        if key(mid):
            low = mid + 1
        else:
            high = mid

    return mid
