import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
import configparser

config = configparser.ConfigParser()
config.read('config.ini')


def get_data_from_firebase(filename='driven-era-285922-990de40007ab.json'):
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


print(get_data_from_firebase())
