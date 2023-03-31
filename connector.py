import pymongo


def iot_ser_db():
    MONGODB_TIMEOUT = 1000
    try:
        client = pymongo.MongoClient("mongodb://devuser:devpassword@localhost:27017/?authMechanism=DEFAULT")
        print(client.list_database_names())
        db  = client['ioticos_god_level']
        print(db.list_collection_names())
        return db
    except pymongo.errors.ServerSelectionTimeoutError as error:
        print ('Error with MongoDB connection: %s' % error)
    except pymongo.errors.ConnectionFailure as error:
        print ('Could not connect to MongoDB: %s' % error)


def local_db():
    MONGODB_TIMEOUT = 1000
    try:
        client = pymongo.MongoClient('localhost', 27017, connect=False,)
        db  = client['haddacloud-v2']
        return db
    except pymongo.errors.ServerSelectionTimeoutError as error:
        print ('Error with MongoDB connection: %s' % error)
    except pymongo.errors.ConnectionFailure as error:
        print ('Could not connect to MongoDB: %s' % error)