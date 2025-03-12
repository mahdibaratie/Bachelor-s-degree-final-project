import pandas as pd

def add_score_column(file_path, score, output_file):
    """
    Load an Excel file, retain only 'Time (s)', add a 'Score' column,
    and save the modified file.
    """
    df = pd.read_excel(file_path)  # Load Excel
    df = df[['Time (s)']]  # Keep only 'Time (s)' column
    df['Score'] = score  # Add the 'Score' column
    df.to_excel(output_file, index=False)  # Save the modified DataFrame
    print(f"Updated file saved as: {output_file}")

def merge_and_update_scores(file_paths, output_file):
    """
    Merge all files on 'Time (s)', sum the scores for matching values,
    remove records with scores fewer than 9, and save the final records.
    """
    dfs = {file: pd.read_excel(file) for file in file_paths}  # Read all Excel files
    
    # Start with the first file
    merged_df = dfs[file_paths[0]]
    
    # Merge with other files on 'Time (s)' and sum 'Score'
    for file in file_paths[1:]:
        df = dfs[file]
        merged_df = merged_df.merge(df, on='Time (s)', how='outer', suffixes=('', '_drop'))
        
        # Sum score columns and update the main 'Score' column
        score_columns = [col for col in merged_df.columns if "Score" in col]
        merged_df['Score'] = merged_df[score_columns].sum(axis=1)
        merged_df.drop(columns=[col for col in score_columns if col != 'Score'], inplace=True)
    
    # Remove records where 'Score' is fewer than 9
    merged_df = merged_df[merged_df['Score'] >= 9]
    
    # Sort by 'Score' in descending order
    merged_df = merged_df.sort_values(by='Score', ascending=False)
    
    merged_df.to_excel(output_file, index=False)
    print(f"Final merged file saved as: {output_file}")

def merge_with_gps_data(final_file, gps_file, output_file="final_data_with_gps_data.xlsx"):
    
   # Merge the final merged data file with gps data on 'Time (s)' and overwrite matching records.
    
   
    # Load both datasets
    final_df = pd.read_excel(final_file)
    gps_df = pd.read_excel(gps_file)
    
    # Merge on 'Time (s)' with an inner join (only matching 'Time (s)' records will be kept)
    merged_df = pd.merge(final_df, gps_df, on="Time (s)", how="inner", suffixes=('_final', '_gps'))
    
    # Save the merged data
    merged_df.to_excel(output_file, index=False)
    print(f"Merged file saved as '{output_file}' with {len(merged_df)} matching records.")

def process_files_and_merge(files_scores, final_output_file, gps_file):
    
    #Process a dictionary of files, add scores, merge the final results,
    #and merge the GPS data.
    
    
    updated_files = []
    for file, (score, output_file) in files_scores.items():
        add_score_column(file, score, output_file)
        updated_files.append(output_file)

    # Merge and update scores
    merge_and_update_scores(updated_files, final_output_file)
    
    # Merge with GPS data
    merge_with_gps_data(final_output_file, gps_file)

# File paths and corresponding scores
files_scores = {
    "special_records_1-1.xlsx": (9, "special_records_1-1_updated.xlsx"),
    "anomalies_1-2.xlsx": (3, "anomalies_1-2_updated.xlsx"),
    "detected_anomalies_1-3.xlsx": (2, "detected_anomalies_1-3_updated.xlsx"),
    "merged_analysis1-4.xlsx": (3, "merged_analysis1-4_updated.xlsx")
}

# Final output file
final_output_file = "merged_sum_score.xlsx"

# GPS file
gps_file = "gps_gpgga_data5.xlsx"

# Process and merge the files with GPS data
process_files_and_merge(files_scores, final_output_file, gps_file)
