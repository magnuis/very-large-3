class dbService:
    def __init__(self, connection):
        self.connection = connection
        self.client = connection.client
        self.db = connection.db


    def create_collection(self, collection_name):
        collection = self.db.create_collection(collection_name)
        print('Created collection: ', collection)


    def insert_users(self, users):
        self.db.user.insert_many(users)


    def insert_activities(self, activities):
        for user_activities in activities:
            # try:
            if (len(activities[user_activities]) > 0):
                self.db.activity.insert_many(activities[user_activities])
            # except TypeError as e:
            #     print('TypeError: ' + e)
                        
            
    def insert_trackpoints(self, trackpoints):
        no_users = 0
        no_trackpoints = 0
        for user in trackpoints:
            for activity in trackpoints[user]:
                self.db.trackpoint.insert_many(trackpoints[user][activity])
                no_trackpoints += len(trackpoints[user][activity])
            no_users += 1
            print(str(no_users) + " inserted")
            print(str(no_trackpoints) +  " trackpoints inserted")
    
    def fetch_documents(self, collection_name):
        collection = self.db[collection_name]
        documents = collection.find({})
        for doc in documents: 
            print(doc)

    def drop_collection(self, collection_name):
        collection = self.db[collection_name]
        collection.drop()