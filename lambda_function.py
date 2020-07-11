import pandas as pd
import boto3
from io import StringIO
import json

def lambda_handler(event, context):
    file_name = "https://usafactsstatic.blob.core.windows.net/public/data/covid-19/covid_confirmed_usafacts.csv"
    data = pd.read_csv(file_name)
    bucket_name = "usa-covid-by-county-time-series"
    csv_buffer = StringIO()

    columns_to_pivot = []
    for col in data.columns:
        if col != "countyFIPS" or col != "County Name" or col != "stateFIPS" or col != "State":
            columns_to_pivot.append(col)

    pivoted_table = data.melt(id_vars=["countyFIPS", "County Name", "State", "stateFIPS"], var_name="Date",
                              value_name="Running Sum")
    #Sort by country ID number
    pivoted_table = pivoted_table.sort_values("countyFIPS")
    pivoted_table.to_csv(csv_buffer)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket_name, "covid_confirmed_usafacts_pivoted.csv").put(Body=csv_buffer.getvalue())

    return {
        'statusCode': 200,
        'body': json.dumps({'success':True})
    }
