import os
import csv
import pymongo
import datetime

def get_db_object():
    connection = pymongo.MongoClient("127.0.0.1")
    database = connection.MICEX
    return database

def get_filenames(path):
    trade_logs = []
    for (dirpath, dirnames, filenames) in os.walk(path):
        trade_logs.append(filenames)
    trade_logs = [log for trade_log in trade_logs for log in trade_log]
    return trade_logs


def read_data_file(filename):
    data_rows = []
    with open(filename, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            data_rows.append(row)
    return data_rows

def import_trade_log_data(data_rows, date):
    micex_db = get_db_object()
    data_rows.pop(0)
    for data_row in data_rows:
        trade_data = {
            "seccode": data_row[1],
            "datetime": datetime.datetime.strptime(date + data_row[2], "%Y%m%d%H%M%S"),
            "price": float(data_row[5]),
            "volume": float(data_row[6]),
        }
        micex_db.trade_logs.save(trade_data)


data_path = "/Users/andrey_lukyanov/Google_Drive/Studies/Year_3/Thesis/Tradelogs/"

trade_logs = get_filenames(data_path)

print(trade_logs)

i = 1

start_time = datetime.datetime.now()
print("Started at: ", start_time)

for file in trade_logs:

    micex_db = get_db_object()

    data_rows = read_data_file(data_path + file)
    date = file.replace("TradeLog", "").replace(".txt", "")

    print(i)
    print(date)
    print(len(data_rows))
    print(data_rows[0])
    print(data_rows[1])

    i += 1

    import_trade_log_data(data_rows, date)


finish_time = datetime.datetime.now()
print("Finished at: ", start_time)
