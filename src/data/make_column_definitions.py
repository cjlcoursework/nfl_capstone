import re

import pandas as pd

if __name__ == '__main__':
    file = "../../../data/external/column_definitions.txt"
    with open(file, 'r') as file:
        record = ""
        records = []
        for line in file.readlines():
            if not line.startswith("\item"):
                record = record + " "+ line.strip()
            else:
                if len(record) > 0:
                    m = re.search(r"{(?P<column_name>[^}]*)}\s-\s(?P<desc>.*)", record)
                    col = m['column_name']
                    desc = m['desc']
                    records.append(dict(column_name=col, desc=desc))
                    record = ''

                record = line[5:].strip()

    df = pd.DataFrame(records)
    df.to_csv("../../../data/interim/column_definitions.csv")


