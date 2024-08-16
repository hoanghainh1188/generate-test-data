import pandas as pd
import numpy as np
import os
from datetime import datetime
import argparse
import random
from tqdm import tqdm
from itertools import product

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

def generate_all_seat_combinations(seats_df, seat_columns):
    return list(product(*[seats_df[col].unique() for col in seat_columns]))

def generate_data(master_data, num_records, dependent_data_config=None):
    customers_df = master_data['customers']
    flags_df = master_data['flags']
    seats_df = master_data['seats']
    
    seat_columns = dependent_data_config['seats']
    all_seat_combinations = generate_all_seat_combinations(seats_df, seat_columns)
    
    print(f"Total number of unique seat combinations: {len(all_seat_combinations)}")
    
    new_data = []
    used_seat_combinations = set()
    
    print(f"Starting to generate {num_records} records...")
    for _ in tqdm(range(num_records), desc="Progress", unit="records"):
        new_row = {}
        
        # Process customers
        customer = customers_df.sample(n=1, replace=True).iloc[0]
        new_row.update(customer.to_dict())
        
        # Process flag
        new_row['flag'] = np.random.choice(flags_df['flag'])
        
        # Process seats
        if new_row['flag'] != 0:
            if len(used_seat_combinations) == len(all_seat_combinations):
                print("All seat combinations have been used. Resetting...")
                used_seat_combinations.clear()
            
            available_combinations = [comb for comb in all_seat_combinations if comb not in used_seat_combinations]
            seat_combination = random.choice(available_combinations)
            used_seat_combinations.add(seat_combination)
            
            new_row.update(dict(zip(seat_columns, seat_combination)))
        else:
            # If flag is 0, only include grade and set other seat-related columns to None
            new_row['grade'] = np.random.choice(seats_df['grade'])
            for col in seat_columns:
                if col != 'grade':
                    new_row[col] = None
        
        new_data.append(new_row)
    
    return pd.DataFrame(new_data)

def main():
    parser = argparse.ArgumentParser(description="Generate test data from master data.")
    parser.add_argument("num_records", type=int, help="Number of records to generate")
    args = parser.parse_args()

    master_data_dir = 'master_data'
    all_master_data = load_master_data(master_data_dir)

    dependent_data_config = {
        'seats': ['floor', 'area', 'block', 'row', 'seat', 'grade']
    }

    print("Starting the data generation process...")
    new_data = generate_data(all_master_data, args.num_records, dependent_data_config)
    
    print("Removing duplicate records...")
    #columns_to_check = ['customerNo', 'price_type']
    seat_columns = ['floor', 'area', 'block', 'row', 'seat', 'grade']
    new_data_unique = new_data.drop_duplicates(subset=columns_to_check, keep='first')
    
    #seat_columns = ['floor', 'area', 'block', 'row', 'seat', 'grade']
    #new_data_unique = new_data_unique.drop_duplicates(subset=seat_columns, keep='first')
    
    print(f"Removed {len(new_data) - len(new_data_unique)} duplicate records.")

    print("Sorting data by customerNo...")
    new_data_sorted = new_data_unique.sort_values(by='customerNo')

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f'generated_combined_data_{timestamp}.csv'

    print(f"Exporting sorted data to file {output_filename}...")
    new_data_sorted.to_csv(output_filename, index=False, encoding='utf-8-sig')
    print(f"Completed! Sorted data has been exported to file {output_filename}")
    print(f"Number of records generated: {len(new_data_sorted)}")

if __name__ == "__main__":
    main()