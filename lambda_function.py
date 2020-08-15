import pandas as pd
import boto3
from io import StringIO
import json

def lambda_handler(event, context):
    file_name = "https://github.com/CSSEGISandData/COVID-19/raw/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv"
    data = pd.read_csv(file_name)
    bucket_name = "usa-covid-by-county-time-series"
    csv_buffer = StringIO()
    columns_to_pivot = []
    columns_not_to_pivot = ["UID", "iso2", "iso3", "code3", "FIPS", "Admin2", "Province_State", "Country_Region", "Lat", "Long_", "Combined_Key"]

    for col in data.columns:
        if col not in columns_not_to_pivot:
            columns_to_pivot.append(col)

    pivoted_table = data.melt(id_vars=columns_not_to_pivot, var_name="Date",
                              value_name="Running_Sum")
    complete_file_name = file_name.split("/")[-1]

    if "." in complete_file_name:
        file_name_parts = complete_file_name.split(".")
        old_file_name = file_name_parts[:-1]
        old_file_extension = file_name_parts[-1]

    pivoted_table = pivoted_table.sort_values("Province_State")

    # Create separate CSV file for each state/territory
    states_metadata = pd.DataFrame(columns=["State_Name"])

    for state in pivoted_table.Province_State.unique():
        states_metadata = states_metadata.append({"State_Name":state}, ignore_index=True)
        state_table_by_counties = pivoted_table.loc[pivoted_table['Province_State'] == state]
        state_table = pd.DataFrame(columns=["Date", "Running_Sum"])

        for date in state_table_by_counties.Date.unique():
            one_day_totals = state_table_by_counties.loc[state_table_by_counties['Date'] == date]
            new_running_sum_by_day = one_day_totals["Running_Sum"].sum()
            new_row = {"Date": date, "Running_Sum": new_running_sum_by_day}
            state_table = state_table.append(new_row, ignore_index=True)
            state_table["Date"] = pd.to_datetime(state_table.Date)
            state_table["Running_Sum"] = pd.to_numeric(state_table.Running_Sum)

        states_metadata.to_csv("data/states_metadata.csv")
        state_table = state_table.sort_values("Date")

        previous_day_sum = 0
        daily_cases = []

        for row in state_table.itertuples():
            new_daily_cases_count = int(row.Running_Sum) - previous_day_sum
            daily_cases.append(new_daily_cases_count)
            previous_day_sum = int(row.Running_Sum)
        state_table["New Daily Cases"] = daily_cases

        state_table.to_csv("data/{state}_pivoted.csv".format(state=state))
    pivoted_table.to_csv("data/{old_file_name}_pivoted.csv".format(old_file_name=old_file_name))

    pivoted_table.to_csv(csv_buffer)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket_name, "covid_confirmed_usafacts_pivoted.csv").put(Body=csv_buffer.getvalue())

    return {
        'statusCode': 200,
        'body': json.dumps({'success':True})
    }
