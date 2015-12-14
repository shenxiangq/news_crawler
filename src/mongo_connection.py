from pymongo import MongoClient

def get_conn(address, port, name):
    return MongoClient(address, port)[name]

