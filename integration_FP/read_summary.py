import pandas as pd
import numpy as np

def read_summary_from_excel(excel_file_path, ticker, poa_input):
    # Load without header
    df = pd.read_excel(excel_file_path, header=None, engine='openpyxl')
    
    summary_header = f"Summary Statistics - {ticker}"
    start_row = df[df.iloc[:, 0] == summary_header].index
    
    if start_row.empty:
        raise ValueError(f"'{summary_header}' not found in Excel file.")
    
    start_row = start_row[0]
    
    # Look for the actual header row (should contain 'Statistic')
    header_row = None
    for i in range(start_row + 1, min(start_row + 10, len(df))):  # Search within reasonable range
        if 'Statistic' in str(df.iloc[i, 0]):
            header_row = i
            break
    
    if header_row is None:
        # Fallback: assume header is right after summary_header
        header_row = start_row + 1
    
    # Get headers
    headers = df.iloc[header_row].values
    
    # Find data rows more robustly
    data_start = header_row + 1
    summary_rows = []
    
    # Look for the next few rows that contain summary statistics
    for i in range(data_start, min(data_start + 10, len(df))):
        row_data = df.iloc[i].values
        # Check if first column contains expected statistic names
        if (pd.notna(row_data[0]) and 
            any(stat in str(row_data[0]) for stat in ['Median', '10th Percentile', '90th Percentile'])):
            summary_rows.append(row_data)
        elif len(summary_rows) > 0 and pd.isna(row_data[0]):
            # Stop if we hit empty rows after finding some data
            break
    
    if not summary_rows:
        raise ValueError("Could not find summary statistics rows in the Excel file.")
    
    # Create DataFrame from found rows
    summary_data = pd.DataFrame(summary_rows, columns=headers)
    summary_data.set_index('Statistic', inplace=True)
    
    # Clean up column names (remove any extra whitespace)
    summary_data.columns = [str(col).strip() for col in summary_data.columns]
    
    # Define target columns
    cols = [f"Revenue {poa_input}", f"EBITDA Margin {poa_input}", f"EV/EBITDA {poa_input}"]
    
    # Check which columns actually exist
    available_cols = []
    for col in cols:
        if col in summary_data.columns:
            available_cols.append(col)
        else:
            # Try to find similar column names
            similar_cols = [c for c in summary_data.columns if poa_input in str(c) and any(keyword in str(c) for keyword in col.split()[:2])]
            if similar_cols:
                available_cols.append(similar_cols[0])
                print(f"Using '{similar_cols[0]}' instead of '{col}'")
            else:
                print(f"Warning: Column '{col}' not found in data")
                available_cols.append(None)
    
    # Explicit numeric coercion for available columns
    for col in available_cols:
        if col and col in summary_data.columns:
            summary_data[col] = pd.to_numeric(summary_data[col], errors='coerce')
    
    # Debug output
    print("Final Debug Summary Table:")
    print("Available columns:", summary_data.columns.tolist())
    print("Summary data shape:", summary_data.shape)
    print("Index values:", summary_data.index.tolist())
    print(summary_data)
    
    # Helper function to safely get values
    def safe_get_value(stat_name, col_name):
        if col_name is None or col_name not in summary_data.columns:
            return np.nan
        
        # Try exact match first
        if stat_name in summary_data.index:
            return summary_data.at[stat_name, col_name]
        
        # Try partial matches
        for idx in summary_data.index:
            if stat_name.lower() in str(idx).lower():
                return summary_data.at[idx, col_name]
        
        return np.nan
    
    # Build stats dictionary with error handling
    stats = {
        "Revenue": {
            "median": safe_get_value("Median", available_cols[0]),
            "p10": safe_get_value("10th Percentile", available_cols[0]),
            "p90": safe_get_value("90th Percentile", available_cols[0]),
        },
        "EBITDA_Margin": {
            "median": safe_get_value("Median", available_cols[1]),
            "p10": safe_get_value("10th Percentile", available_cols[1]),
            "p90": safe_get_value("90th Percentile", available_cols[1]),
        },
        "EV_EBITDA": {
            "median": safe_get_value("Median", available_cols[2]),
            "p10": safe_get_value("10th Percentile", available_cols[2]),
            "p90": safe_get_value("90th Percentile", available_cols[2]),
        },
    }
    
    # Print final stats for debugging
    print("Extracted stats:")
    for key, value in stats.items():
        print(f"{key}: {value}")
    
    return stats