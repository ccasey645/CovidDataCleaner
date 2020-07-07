import pandas as pd
import boto3
import sys
import traceback
from io import StringIO
import json

def process_file(file_name):
    data = pd.read_csv(file_name)

    columns_to_pivot = []
    for col in data.columns:
        if col != "countyFIPS" or col != "County Name" or col != "stateFIPS" or col != "State":
            columns_to_pivot.append(col)

    pivoted_table = data.melt(id_vars=["countyFIPS", "County Name", "State", "stateFIPS"], var_name="Date",
                              value_name="Running Sum")
    old_file_name, old_file_extension = file_name.split(".")
    pivoted_table.to_csv("{old_file_name}_pivoted.csv".format(old_file_name=old_file_name))

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
    old_file_name, old_file_extension = file_name.split(".")
    pivoted_table.to_csv(csv_buffer)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket_name, "covid_confirmed_usafacts_pivoted.csv").put(Body=csv_buffer.getvalue())

    return {
        'statusCode': 200,
        'body': json.dumps({'success':True})
    }


if __name__ == "__main__":
    try:
        command, filename = sys.argv[1].split("=")
    except:
        traceback.print_exc()
        raise
    else:
        print("Printing file name")
        print(filename)
        print(command)
        if command == "--file":
            process_file(filename)