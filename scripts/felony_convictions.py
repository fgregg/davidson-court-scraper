import csv
import datetime
import json
import sys

writer = csv.DictWriter(
    sys.stdout,
    fieldnames=(
        "full_name",
        "first_name",
        "last_name",
        "oca",
        "date_of_birth",
        "criminal_history_url",
        "case_number",
        "case_url",
        "case_status",
        "defendant_status",
        "fees_owed",
        "felony_conviction",
    ),
    extrasaction="ignore",
)
writer.writeheader()

for row in sys.stdin:
    case_details = json.loads(row.strip())
    if dob := case_details["date_of_birth"]:
        case_details["date_of_birth"] = datetime.datetime.strptime(
            dob, "%m/%d/%Y"
        ).date()
    if fees_owed := case_details["fees_owed"]:
        case_details["fees_owed"] = fees_owed.replace("$", "").replace(",", "")
    for charge in case_details["charges"]:
        if charge["convicted"] and "FELONY" in charge["convicted"]:

            case_details["felony_conviction"] = True
            break
    else:
        case_details["felony_conviction"] = False

    writer.writerow(case_details)
