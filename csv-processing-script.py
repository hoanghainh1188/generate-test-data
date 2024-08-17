import csv
import random
from collections import defaultdict
import os
from datetime import datetime
import argparse

def read_csv(filename):
    filepath = os.path.join('master_data', filename)
    print(f"Đang đọc file: {filepath}")
    with open(filepath, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        data = []
        for row in reader:
            cleaned_row = {k.lstrip('\ufeff'): v for k, v in row.items()}
            data.append(cleaned_row)
        print(f"Số dòng đọc được từ {filename}: {len(data)}")
        if data:
            print(f"Các cột trong {filename}: {', '.join(data[0].keys())}")
            print(f"Dòng đầu tiên của {filename}: {data[0]}")
        return data

def write_csv(filename, data, fieldnames):
    with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

def get_first_column_name(data):
    return next(iter(data[0])) if data else None

def generate_data(num_records):
    prices = read_csv('prices.csv')
    flags = read_csv('flags.csv')
    seats = read_csv('seats.csv')
    customers = read_csv('customers.csv')

    for name, data in [('prices', prices), ('flags', flags), ('seats', seats), ('customers', customers)]:
        print(f"\nKiểm tra dữ liệu {name}:")
        if data:
            print(f"Số lượng bản ghi trong {name}: {len(data)}")
            print(f"Các khóa trong bản ghi {name}: {', '.join(data[0].keys())}")
            print(f"Bản ghi đầu tiên của {name}: {data[0]}")
        else:
            print(f"Không có dữ liệu trong {name}.")

    price_type_key = get_first_column_name(prices)
    flag_key = get_first_column_name(flags)
    customer_key = get_first_column_name(customers)

    # Tạo một tập hợp các ghế có sẵn
    available_seats = set(tuple(seat.values()) for seat in seats)

    customer_data = defaultdict(lambda: {'flag': None, 'grade': None, 'records': []})

    total_records = 0
    customers_list = customers.copy()

    while total_records < num_records and customers_list:
        customer = random.choice(customers_list)[customer_key]
        
        if customer_data[customer]['flag'] is None:
            customer_data[customer]['flag'] = random.choice(flags)[flag_key]
            customer_data[customer]['grade'] = random.choice(seats)['grade']

        flag = customer_data[customer]['flag']
        grade = customer_data[customer]['grade']
        price_type = random.choice(prices)[price_type_key]
        
        if flag == '0' or not available_seats:
            record = {
                'customerNo': customer,
                'flag': flag,
                'price_type': price_type,
                'grade': grade,
                'seat_info': None
            }
        else:
            seat = random.choice(list(available_seats))
            available_seats.remove(seat)
            record = {
                'customerNo': customer,
                'flag': flag,
                'price_type': price_type,
                'grade': grade,
                'seat_info': seat[:-1]  # All elements except the last one (grade)
            }
        
        customer_data[customer]['records'].append(record)
        total_records += 1

        if total_records >= num_records:
            break

        # Nếu đã xử lý tất cả khách hàng, reset danh sách để có thể lặp lại
        if len(customer_data) == len(customers):
            customers_list = customers.copy()

    valid_records = [record for data in customer_data.values() for record in data['records']]

    # Sắp xếp records theo customerNo
    valid_records.sort(key=lambda x: int(x['customerNo']) if x['customerNo'].isdigit() else x['customerNo'])

    # Chuyển đổi seat_info thành các cột riêng biệt
    final_records = []
    for record in valid_records:
        final_record = {
            'customerNo': record['customerNo'],
            'flag': record['flag'],
            'price_type': record['price_type'],
            'grade': record['grade'],
            'floor': record['seat_info'][0] if record['seat_info'] else '',
            'area': record['seat_info'][1] if record['seat_info'] else '',
            'block': record['seat_info'][2] if record['seat_info'] else '',
            'row': record['seat_info'][3] if record['seat_info'] else '',
            'seat': record['seat_info'][4] if record['seat_info'] else ''
        }
        final_records.append(final_record)

    print("\nThông tin về dữ liệu đã tạo:")
    print(f"Số lượng bản ghi hợp lệ: {len(final_records)}")
    if final_records:
        print("Các khóa trong bản ghi đầu tiên:", ', '.join(final_records[0].keys()))
        for key, value in final_records[0].items():
            print(f"Giá trị '{key}' trong bản ghi đầu tiên: {value}")

    return final_records

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate combined CSV data.')
    parser.add_argument('num_records', type=int, help='Number of records to generate')
    args = parser.parse_args()

    generated_data = generate_data(args.num_records)

    fieldnames = ['customerNo', 'flag', 'price_type', 'grade', 'floor', 'area', 'block', 'row', 'seat']

    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    output_filename = f'combined_output_{timestamp}.csv'

    write_csv(output_filename, generated_data, fieldnames)

    print(f"\nĐã tạo file {output_filename} thành công với {len(generated_data)} bản ghi!")

    print("\nKiểm tra nội dung file đầu ra:")
    with open(output_filename, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        first_row = next(reader, None)
        if first_row:
            print("Các cột trong file đầu ra:", ', '.join(first_row.keys()))
            for key, value in first_row.items():
                print(f"Giá trị '{key}' trong dòng đầu tiên: {value}")
        else:
            print("File đầu ra trống.")