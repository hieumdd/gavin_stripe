import os
import json
from datetime import datetime

import stripe
from google.cloud import bigquery


stripe.api_key = os.getenv("API_KEY")

DATASET = "Stripe"
BQ_CLIENT = bigquery.Client()


class BalanceTransactions:
    def __init__(self, start=None, end=None):
        self.start, self.end = self._get_time_range(start, end)

    def _get_time_range(self, start, end):
        if start and end:
            start, end = [
                round(datetime.strptime(i, "%Y-%m-%d").timestamp())
                for i in [start, end]
            ]
        else:
            try:
                query = f"SELECT UNIX_SECONDS(MAX(created)) AS incre FROM {DATASET}.Charges"
                results = BQ_CLIENT.query(query).result()
                row = [row for row in results][0]
                start = row["incre"]
            except:
                start = round(datetime(2021, 1, 1).timestamp())
            end = round(datetime.now().timestamp())
            
        return start, end

    def get(self):
        params = {
            "created": {"gte": self.start, "lte": self.end},
            "limit": 100,
        }
        expand = ["data.customer"]
        results = stripe.Charge.list(**params, expand=expand)
        rows = [i.to_dict_recursive() for i in results.auto_paging_iter()]
        return rows, len(rows)

    def transform(self, rows):
        return [self._transform_to_string(row) for row in rows]

    def _transform_to_string(self, row):
        for i in ["payment_method_details", "refunds", "metadata"]:
            row[i] = json.dumps(row[i])
        for i in ["customer"]:
            if i in row and row[i]:
                row[i]["metadata"] = json.dumps(row[i]["metadata"])
        return row

    def load(self, rows):
        with open("schemas/Charges.json", "r") as f:
            schema = json.load(f)

        loads = BQ_CLIENT.load_table_from_json(
            rows,
            f"{DATASET}.Charges",
            job_config=bigquery.LoadJobConfig(
                schema=schema,
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_APPEND",
            ),
        ).result()

        return loads

    def update(self):
        query = f"""
        CREATE OR REPLACE TABLE {DATASET}.Charges
        AS
        SELECT * EXCEPT(row_num)
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY id) AS row_num
            FROM {DATASET}.Charges
        )
        WHERE row_num = 1
        """
        BQ_CLIENT.query(query).result()

    def run(self):
        rows, num_processed = self.get()
        if num_processed > 0:
            rows = self.transform(rows)
            loads = self.load(rows)
            self.update()
        else:
            loads = None
        return {
            "start": self.start,
            "end": self.end,
            "num_processed": num_processed,
            "output_rows": loads.output_rows if loads else None,
            "errors": loads.errors if loads else None,
        }
