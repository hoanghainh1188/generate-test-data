import pandas as pd
import numpy as np
import os
from datetime import datetime
import argparse
import random
from tqdm import tqdm

def load_master_data(directory):
    master_data = {}
    print("Reading master data files...")
    for filename in os.listdir(directory):
        if filename.endswith('.csv'):
            name = os.path.splitext(filename)[0]
            filepath = os.path.join(directory, filename)
            master_data[name] = pd.read_csv(filepath, encoding='utf-8')
            print(f"Read file: {filename}")
    return master_data

def generate_dependent_data(df, dependent_columns, used_combinations):
    all_combinations = set(map(tuple, df[dependent_columns].values))
    available_combinations = list(all_combinations - used_combinations)
    
    if not available_combinations:
        return None  # Return None if no combinations are available
    
    selected_combination = random.choice(available_combinations)
    used_combinations.add(selected_combination)
    
    return dict(zip(dependent_columns, selected_combination))

def generate_data(master_data, num_records, dependent_data_config=None):
    all_columns = []
    for df in master_data.values():
        all_columns.extend(df.columns)
    all_columns = list(dict.fromkeys(all_columns))  # Remove duplicates
    
    new_data = []
    
    # Dictionary to store the relationship between customerNo and grade, flag
    customer_data_map = {}
    
    # Set to store used combinations for seats (excluding grade)
    used_combinations = set()
    
    # Get all possible grade and flag values
    all_grades = master_data['seats']['grade'].unique()
    all_flags = master_data['flags']['flag'].unique()
    
    print(f"Starting to generate {num_records} records...")
    for _ in tqdm(range(num_records), desc="Progress", unit="records"):
        new_row = {}
        
        # Process customers first
        customers_df = master_data['customers']
        sample_customer = customers_df.sample(n=1, replace=True).iloc[0]
        new_row.update(sample_customer)
        customer_no = sample_customer['customerNo']
        
        # Process flag and grade
        if customer_no not in customer_data_map:
            customer_data_map[customer_no] = {
                'flag': np.random.choice(all_flags),
                'grade': np.random.choice(all_grades)
            }
        
        new_row['flag'] = customer_data_map[customer_no]['flag']
        
        # Process other tables (if any)
        for table_name, df in master_data.items():
            if table_name not in ['customers', 'flags', 'seats']:
                sample_row = df.sample(n=1, replace=True).iloc[0]
                new_row.update(sample_row)
        
        # Process seats
        if new_row['flag'] != 0:
            seats_df = master_data['seats']
            seat_columns = [col for col in dependent_data_config['seats'] if col != 'grade']
            
            # Generate seats data (excluding grade)
            seats_data = generate_dependent_data(seats_df[seat_columns], seat_columns, used_combinations)
            if seats_data is None:
                print("\nAll combinations for seats (excluding grade) have been used.")
                seats_data = seats_df[seat_columns].sample(n=1, replace=True).iloc[0].to_dict()
            
            # Add grade to seats_data
            seats_data['grade'] = customer_data_map[customer_no]['grade']
            
            new_row.update(seats_data)
        
        new_data.append(new_row)
    
    return pd.DataFrame(new_data)

def main():
    parser = argparse.ArgumentParser(description="Generate test data from master data.")
    parser.add_argument("num_records", type=int, help="Number of records to generate")
    args = parser.parse_args()

    # Directory containing master data CSV files
    master_data_dir = 'master_data'

    # Read all master data files
    all_master_data = load_master_data(master_data_dir)

    # Configuration for dependent data
    dependent_data_config = {
        'seats': ['floor', 'area', 'block', 'row', 'seat', 'grade']
    }

    # Generate new data
    print("Starting the data generation process...")
    new_data = generate_data(all_master_data, args.num_records, dependent_data_config)
    
    print("Removing duplicate records...")
    columns_to_check = ['customerNo', 'grade', 'price_type']
    new_data_unique = new_data.drop_duplicates(subset=columns_to_check, keep='first')
    
    print(f"Removed {len(new_data) - len(new_data_unique)} duplicate records.")

    # Sort data by customerNo
    print("Sorting data by customerNo...")
    new_data_sorted = new_data_unique.sort_values(by='customerNo')

    # Create filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f'generated_combined_data_{timestamp}.csv'

    # Export to CSV file with timestamp and UTF-8 encoding
    print(f"Exporting sorted data to file {output_filename}...")
    new_data_sorted.to_csv(output_filename, index=False, encoding='utf-8-sig')
    print(f"Completed! Sorted data has been exported to file {output_filename}")
    print(f"Number of records generated: {len(new_data_sorted)}")

if __name__ == "__main__":
    main()