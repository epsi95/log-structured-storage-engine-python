import os
import threading
import base64
import time

# constants
DATABASE_FILE_PATH = os.path.join(os.getcwd(), "database")
DATABASE_FILE_NAME = 'db.txt'
MAX_FILE_SIZE_UNTIL_COMPACTION = 1  # in kb

# making sure the db file exist
db_files = os.listdir(DATABASE_FILE_PATH)
if len(db_files) == 1:
    DATABASE_FILE_NAME = db_files[0]
elif len(db_files) == 0:
    with open(os.path.join(DATABASE_FILE_PATH, DATABASE_FILE_NAME), 'a') as f:
        pass
else:
    print("some discrepancy in the db file! More than one db.txt file. Remove all the db.txt files")
    quit()

# defining the lock
lock = threading.Lock()

# global in-memory has table and offset
global_offset = 0
global_hash_table = {}

# initializing the global hash table
with open(os.path.join(DATABASE_FILE_PATH, DATABASE_FILE_NAME), 'r') as f:
    position = 0
    line = f.readline()
    while len(line) > 0:
        global_hash_table[line.split(",")[0]] = position
        position = f.tell()
        line = f.readline()
    global_offset = position

# creating necessary directory
os.makedirs(DATABASE_FILE_PATH, exist_ok=True)


# monitoring function
def monitor_database_file():
    # it will be executed by a daemon thread
    # whenever the database file will exceed MAX_FILE_SIZE_UNTIL_COMPACTION
    # it will remove the duplicate values
    # effectively doing compaction
    while True:
        global DATABASE_FILE_NAME, global_hash_table, global_offset

        size = os.path.getsize(os.path.join(DATABASE_FILE_PATH, DATABASE_FILE_NAME)) / 1024
        number_of_lines = len(open(os.path.join(DATABASE_FILE_PATH, DATABASE_FILE_NAME), 'r').readlines())

        new_file_name = f'db_{int(time.time())}.txt'
        global_hash_table_new = {}

        if size > MAX_FILE_SIZE_UNTIL_COMPACTION and len(global_hash_table) < number_of_lines:
            print('compacting the database file...')
            latest_offset = 0
            with open(os.path.join(DATABASE_FILE_PATH, new_file_name), 'a') as f1:
                with open(os.path.join(DATABASE_FILE_PATH, DATABASE_FILE_NAME), 'r') as f2:
                    for each_key in global_hash_table:
                        f2.seek(global_hash_table[each_key])
                        global_hash_table_new[each_key] = f1.tell()
                        f1.write(f2.readline())
                        latest_offset = f1.tell()

            old_file_name = DATABASE_FILE_NAME

            lock.acquire()
            DATABASE_FILE_NAME = new_file_name
            global_hash_table = global_hash_table_new
            global_offset = latest_offset
            lock.release()

            # deleting the old file
            os.remove(os.path.join(DATABASE_FILE_PATH, old_file_name))

            print('compaction completed successfully.')
        time.sleep(1)


# starting the daemon monitoring thread
threading.Thread(target=monitor_database_file, name='monitor', daemon=True).start()


# declaring helper functions
def print_values(key):
    with open(os.path.join(DATABASE_FILE_PATH, DATABASE_FILE_NAME), 'r') as f:
        f.seek(global_hash_table[key])
        print(key + ": " + base64.b64decode(f.readline().split(",")[-1]).decode())


# main loop to interact with the database
while True:
    user_command = input(">>> ")

    try:
        mode_of_operation, key_1 = user_command.split(" ")[0], user_command.split(" ")[1]
        key_2 = " ".join(user_command.split(" ")[2:]) if len(user_command.split(" ")) > 2 else None

        if mode_of_operation.upper() == "SET":
            with open(os.path.join(DATABASE_FILE_PATH, DATABASE_FILE_NAME), 'a') as f:
                f.write(f"{key_1},{base64.b64encode(key_2.encode()).decode()}\n")
                global_hash_table[key_1] = global_offset
                global_offset = f.tell()

        elif mode_of_operation.upper() == "GET":
            if key_1 == "*":
                for each_key in global_hash_table:
                    print_values(each_key)
            else:
                print_values(key_1)
    except Exception as e:
        print(e)
        print("Invalid operation! Use set <key> <value> or get <key>")
