import csv
import datetime
import json
import sys

writer = csv.DictWriter(
    sys.stdout,
    fieldnames=("name", "oca", "date_of_birth", "criminal_history_url", "case_number"),
    extrasaction="ignore",
)
writer.writeheader()

for row in sys.stdin:
    case_details = json.loads(row.strip())
    for charge in case_details["charges"]:
        if charge["convicted"] and "FELONY" in charge["convicted"]:
            if dob := case_details["date_of_birth"]:
                case_details["date_of_birth"] = datetime.datetime.strptime(
                    dob, "%m/%d/%Y"
                ).date()
            writer.writerow(case_details)
            break
