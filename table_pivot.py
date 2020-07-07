import pandas as pd
import sys
import traceback

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