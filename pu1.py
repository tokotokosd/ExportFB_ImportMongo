import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
import configparser
from pymongo import MongoClient

config = configparser.ConfigParser()
config.read('config.ini')


def get_data_from_firebase(filename=config['FireBase']['FirebaseJson']):
    # Use a service account
    cred = credentials.Certificate(filename)
    firebase_admin.initialize_app(cred)
    # connect to db
    db = firestore.client()

    # IF tables input is empty take all tables else input
    final = {}
    if config['FireBase']['Tables'] == '':
        for docs in db.collections():
            final[docs.id] = []
            for doc in docs.stream():
                final[docs.id].append({doc.id: doc.to_dict()})
    else:
        col_name = [x for x in config['FireBase']['Tables'].split(";") if x]
        for collection_name in col_name:
            docs = db.collection(collection_name.replace(" ", ""))
            for doc in docs.stream():
                final[docs.id].append({doc.id: doc.to_dict()})

    return final


def import_to_mongo():
    # connect to MongoDB
    client = MongoClient(config['MongoDB']['URL'])
    db = client[config['MongoDB']['DB_Name']]
    # get import data
    import_data = get_data_from_firebase()

    need_to_imp = len(list(import_data.values()))
    x = 1
    for collection in import_data.keys():
        collection_imp = []
        for doc in import_data[collection]:
            # create id
            a = {'_id': list(doc.keys())[0]}
            # create other parameters
            b = doc[list(doc.keys())[0]]
            tmp_dic = {**a, **b}
            collection_imp.append(tmp_dic)

        # insert all from collection
        db[collection].insert_many(collection_imp)

        print('Created {0} of {1} '.format(x, need_to_imp))
        x += 1


if __name__ == '__main__':
    import_to_mongo()
