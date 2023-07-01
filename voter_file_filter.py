import polars as pl
import tkinter as tk
from tkinter import filedialog
from datetime import datetime
from python_scripts.helper_functions import printr


available_columns = ['County', 'State Senate', 'State House', 'City', 'County Precinct']


def input_format(prompt):
    prompt = str.strip(prompt)
    return prompt


def column_contains_value(col_name: str, value: str, df: pl.DataFrame, literal: bool = False) -> pl.DataFrame:
    if literal:
        return df.filter(pl.col(col_name).str.contains(value, literal=literal))
    else:
        return df.filter(pl.col(col_name).str.contains(f'(?i){value}'))


def reset_filter_df(statewide_df: pl.DataFrame) -> list[str, pl.DataFrame]:
    printr("Resetting filter...")
    return ['', statewide_df.clone()]


def get_columns(df: pl.DataFrame) -> list[str]:
    return df.columns


def get_dataframe_stats(df: pl.DataFrame) -> list[str]:
    stats = [
        f"Number of rows: {df.height}",
        f"Number of columns: {df.width}"
    ]
    return stats


def get_column_values(df: pl.DataFrame, col_name) -> list[str]:
    try:
        column = df.select(col_name).to_series()
        val_list = column.unique().to_list()
        del column
        return val_list
    except Exception as e:
        return [f"Error getting values for column: {e}"]


def prep_for_filter(col_name: str, applied_filters: list[str], upper: bool = False) -> list[str]:
    if upper:
        value = str.upper(input_format(input(f"Enter the {col_name} value: ")))
    else:
        value = input_format(input(f"Enter the {col_name} value: "))
    value_title_case = str.title(value)
    print('')
    printr(f"Filtering for {col_name}: {value_title_case}...")
    applied_filters.append(f"{col_name}: {value_title_case}")
    return [value, applied_filters, value_title_case]


def import_statewide_file():
    printr("Getting previous csv file...")
    root = tk.Tk()
    root.withdraw()
    file_path = filedialog.askopenfilename()
    statewide_df = pl.read_csv(file_path, infer_schema_length=0, low_memory=True)
    printr("Finished reading previous csv file.")
    return statewide_df


def get_options():
    ret_str = "How would you like to filter the data? \n"
    for col in available_columns:
        ret_str = ret_str + f"--> {col}\n"
    ret_str = ret_str + '--> See All Columns\n'
    ret_str = ret_str + '--> Get Column Values\n'
    ret_str = ret_str + "--> Export\n"
    ret_str = ret_str + "--> Reset\n"
    ret_str = ret_str + "--> Exit\n\n"
    ret_str = ret_str + "choice: "
    return ret_str


def filter_statewide_file():
    statewide_df = import_statewide_file()
    filter_df = statewide_df.clone()
    filter_df_name = ''
    applied_filters: list[str] = []

    date_today = datetime.today().strftime('%m-%d-%Y')
    while True:
        try:
            filter_column = input(get_options())
            filter_column = str.title(filter_column)
            match filter_column:
                case 'County':
                    value, applied_filters, value_title_case = prep_for_filter(
                        filter_column, applied_filters, upper=True
                    )
                    filter_df_name = filter_df_name + f'{value_title_case} County'
                    filter_df = statewide_df.filter(pl.col('County') == value)
                case 'State Senate':
                    value, applied_filters, value_title_case = prep_for_filter(filter_column, applied_filters)
                    filter_df_name = filter_df_name + f'State Senate District {value_title_case}'
                    filter_df = column_contains_value('State Senate District', value, statewide_df)
                case 'State House':
                    value, applied_filters, value_title_case = prep_for_filter(filter_column, applied_filters)
                    filter_df_name = filter_df_name + f'State House District {value_title_case}  '
                    filter_df = column_contains_value('State House District', value, statewide_df)
                case 'City':
                    value, applied_filters, value_title_case = prep_for_filter(filter_column, applied_filters)
                    filter_df_name = filter_df_name + f'City of {value_title_case}  '
                    filter_df = column_contains_value('Municipality', value, statewide_df)
                case 'County Precinct':
                    value, applied_filters, value_title_case = prep_for_filter(filter_column, applied_filters)
                    filter_df_name = filter_df_name + f'Precinct {value_title_case}  '
                    filter_df = column_contains_value('Precinct', value, statewide_df)
                case 'See All Columns':
                    print(get_columns(filter_df))
                case 'Get Column Values':
                    col_name = input_format(input("Enter the column name: ")).title()
                    val_list = get_column_values(filter_df, col_name)
                    for val in val_list:
                        print(val)
                    print()
                case 'Export':
                    printr("Exporting dataframe...")
                    filter_df_name = filter_df_name.strip().replace('  ', '__')
                    filter_df.write_csv(f"D:/Downloads/{date_today}_{filter_df_name}.csv")
                    printr(
                        f"Saved {filter_df_name} to "
                        f"D:/Downloads/{date_today}_{filter_df_name}.csv"
                    )
                    filter_df_name, filter_df = reset_filter_df(statewide_df)
                case 'Reset':
                    filter_df_name, filter_df = reset_filter_df(statewide_df)
                case 'Exit':
                    exit()
                case _:
                    printr(f'{filter_column} is not a valid choice. Please try again.')
        except ValueError as e:
            printr(f'Raised error: {e}')
            printr("Please try again.")
        print("Currently Applied Filters:")
        for applied_filter in applied_filters:
            print(applied_filter)
        print()
        print("Current Dataframe Stats:")
        for stat in get_dataframe_stats(filter_df):
            print(stat)
        print()


def main():
    filter_statewide_file()


if __name__ == '__main__':
    main()
