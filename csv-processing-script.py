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
            # Xử lý BOM trong tên cột
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
    # Đọc dữ liệu từ các file CSV trong thư mục master_data
    prices = read_csv('prices.csv')
    flags = read_csv('flags.csv')
    seats = read_csv('seats.csv')
    customers = read_csv('customers.csv')

    # Kiểm tra dữ liệu seats
    print("\nKiểm tra dữ liệu seats:")
    if seats:
        print(f"Số lượng bản ghi trong seats: {len(seats)}")
        print(f"Các khóa trong bản ghi seats: {', '.join(seats[0].keys())}")
        print(f"Giá trị 'floor' trong bản ghi đầu tiên: {seats[0].get('floor', 'Không có')}")
    else:
        print("Không có dữ liệu trong seats.")

    # Lấy tên cột đầu tiên của mỗi file
    price_type_key = get_first_column_name(prices)
    flag_key = get_first_column_name(flags)
    customer_key = get_first_column_name(customers)

    # Tạo từ điển để lưu trữ dữ liệu kết hợp
    combined_data = defaultdict(lambda: defaultdict(set))

    # Kết hợp dữ liệu từ các file
    for _ in range(num_records):
        customer = random.choice(customers)[customer_key]
        seat = random.choice(seats)
        combined_data[customer]['seats'].add((
            seat.get('floor', 'N/A'),
            seat.get('area', 'N/A'),
            seat.get('block', 'N/A'),
            seat.get('row', 'N/A'),
            seat.get('seat', 'N/A')
        ))
        combined_data[customer]['grade'].add(seat.get('grade', 'N/A'))
        combined_data[customer]['flag'] = random.choice(flags)[flag_key]
        combined_data[customer]['price_type'] = random.choice(prices)[price_type_key]

    # Lọc ra các bản ghi hợp lệ và đồng nhất
    valid_records = []
    for customer, data in combined_data.items():
        if len(data['seats']) > 0 and len(data['grade']) == 1:  # Kiểm tra có seat và grade đồng nhất
            grade = data['grade'].pop()  # Lấy giá trị grade duy nhất
            for seat in data['seats']:
                record = {
                    'customerNo': customer,
                    'flag': data['flag'],
                    'price_type': data['price_type'],
                    'grade': grade,
                    'floor': seat[0],
                    'area': seat[1],
                    'block': seat[2],
                    'row': seat[3],
                    'seat': seat[4]
                }
                valid_records.append(record)

    # Sắp xếp records theo customerNo
    valid_records.sort(key=lambda x: x['customerNo'])

    # In thông tin về dữ liệu đã tạo
    print("\nThông tin về dữ liệu đã tạo:")
    print(f"Số lượng bản ghi hợp lệ: {len(valid_records)}")
    if valid_records:
        print("Các khóa trong bản ghi đầu tiên:", ', '.join(valid_records[0].keys()))
        print(f"Giá trị 'floor' trong bản ghi đầu tiên: {valid_records[0].get('floor', 'Không có')}")

    return valid_records

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate combined CSV data.')
    parser.add_argument('num_records', type=int, help='Number of records to generate')
    args = parser.parse_args()

    # Tạo dữ liệu
    generated_data = generate_data(args.num_records)

    # Sắp xếp lại các cột theo yêu cầu
    fieldnames = ['customerNo', 'flag', 'price_type', 'grade', 'floor', 'area', 'block', 'row', 'seat']

    # Tạo tên file với timestamp
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    output_filename = f'combined_output_{timestamp}.csv'

    # Ghi dữ liệu ra file CSV mới
    write_csv(output_filename, generated_data, fieldnames)

    print(f"\nĐã tạo file {output_filename} thành công với {len(generated_data)} bản ghi!")

    # Kiểm tra nội dung file đầu ra
    print("\nKiểm tra nội dung file đầu ra:")
    with open(output_filename, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        first_row = next(reader, None)
        if first_row:
            print("Các cột trong file đầu ra:", ', '.join(first_row.keys()))
            print(f"Giá trị 'floor' trong dòng đầu tiên: {first_row.get('floor', 'Không có')}")
        else:
            print("File đầu ra trống.")