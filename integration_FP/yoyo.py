import os
import pandas as pd
from openpyxl import load_workbook

# ┌─────────────────────────────────────────────────────────────────────┐
# │ 1) INPUT / OUTPUT file paths                                        │
# └─────────────────────────────────────────────────────────────────────┘
input_path       = r'C:\Users\73475\integration_FP\Combined_Forecast_Summary_With_Linking.xlsx'
output_path      = r'C:\Users\73475\integration_FP\cleantable.xlsx'

# We'll save a temporary “values‐only” copy in the same folder:
base_dir         = os.path.dirname(input_path)
values_only_path = os.path.join(base_dir, "values_only_temp.xlsx")

# ┌─────────────────────────────────────────────────────────────────────┐
# │ 2) STEP 1: Load with openpyxl(data_only=True) and immediately save │
# │    so that every formula is replaced by its last‐stored value.     │
# └─────────────────────────────────────────────────────────────────────┘
#    NOTE: data_only=True tells openpyxl “give me the cached (last‐calculated) values
#    instead of the formula text.” When you save() a workbook that was opened with
#    data_only=True, the new file has all formulas stripped and just values.
wb = load_workbook(filename=input_path, data_only=True)
wb.save(values_only_path)


# ┌─────────────────────────────────────────────────────────────────────┐
# │ 3) STEP 2: Read the “values‐only” workbook with pandas to extract   │
# │    the summary‐statistics block (i.e. rows under the “Statistic” row). │
# └─────────────────────────────────────────────────────────────────────┘
#    We assume your summary lives in the sheet named "Forecast Summary".
sheet_name = "Forecast Summary"

# 3a) Load the sheet with header=None so we can scan for "Statistic".
df_raw = pd.read_excel(values_only_path, sheet_name=sheet_name, header=None)

# 3b) Find the row index where column 0 == "Statistic"
#     (that row is the header of your summary table)
header_row_idx = df_raw[df_raw[0] == "Statistic"].index[0]

# 3c) Now re‐read, telling pandas that row “header_row_idx” is the header
df_summary = pd.read_excel(
    values_only_path,
    sheet_name=sheet_name,
    header=header_row_idx
)

# 3d) Drop any rows where “Statistic” is NaN (optional, in case there are blank footer rows)
df_summary = df_summary[df_summary["Statistic"].notna()].reset_index(drop=True)


# ┌─────────────────────────────────────────────────────────────────────┐
# │ 4) STEP 3: Write the cleaned summary table out to a new Excel file  │
# └─────────────────────────────────────────────────────────────────────┘
df_summary.to_excel(output_path, index=False)

# ┌─────────────────────────────────────────────────────────────────────┐
# │ 5) (Optional) Print to screen so you can verify what was pasted.   │
# └─────────────────────────────────────────────────────────────────────┘
print("=== SUMMARY‐STATISTICS BLOCK (values only) ===\n")
print(df_summary)


# ┌─────────────────────────────────────────────────────────────────────┐
# │ 6) (Optional cleanup) Remove the temporary “values_only” workbook   │
# │    if you no longer need it.                                        │
# └─────────────────────────────────────────────────────────────────────┘
# Uncomment the next two lines if you want to delete the temp file:
# try:
#     os.remove(values_only_path)
# except OSError:
#     pass

