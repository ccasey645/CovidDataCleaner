import pandas as pd
import sys
import traceback

def process_file(file_name, is_url=False):
    data = pd.read_csv(file_name)
    columns_to_pivot = []
    for col in data.columns:
        if col != "countyFIPS" or col != "County Name" or col != "stateFIPS" or col != "State":
            columns_to_pivot.append(col)

    pivoted_table = data.melt(id_vars=["countyFIPS", "County Name", "State", "stateFIPS"], var_name="Date",
                              value_name="Running Sum")
    if not is_url:
        old_file_name, old_file_extension = file_name.split(".")
    else:
        complete_file_name = file_name.split("/")[-1]
        print("complete_file_name")
        print(complete_file_name)
        if "." in complete_file_name:
            file_name_parts = complete_file_name.split(".")
            old_file_name = file_name_parts[:-1]
            print("old_file_name!!")
            print(old_file_name)
            old_file_extension = file_name_parts[-1]
    #Sort by county ID
    pivoted_table = pivoted_table.sort_values("countyFIPS")
    pivoted_table.to_csv("{old_file_name}_pivoted.csv".format(old_file_name=old_file_name))

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
        elif command == "--url":
            process_file(filename, is_url=True)
