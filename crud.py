from DbConnector import DbConnector
from utils.dbService import dbService
from utils.fileUtils import read_activities, read_trackpoints, read_users
from tabulate import tabulate
from haversine import haversine
import re



class Crud:
    ACTIVITY_ID = 0
    ACTIVITY_ID_MAP = {}


    def __init__(self, deleteTables=False):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db
        self.dbService = dbService(connection=self.connection)
        if deleteTables:
            self.drop_tables()
            self.create_collections()


    def create_collections(self):
        self.dbService.create_collection("user")
        self.dbService.create_collection("activity")
        self.dbService.create_collection("trackpoint")


    def insert_users(self, users=None):
        if (users == None):
            users = read_users("./dataset/Data", "./dataset/labeled_ids.txt")
        self.dbService.insert_users(users)


    def insert_activities(self, activities=None):
        if (activities == None):
            [activities, self.ACTIVITY_ID_MAP, self.ACTIVITY_ID] = read_activities("./dataset/Data", activity_id_map=self.ACTIVITY_ID_MAP, activity_id=self.ACTIVITY_ID)
        self.dbService.insert_activities(activities)


    def insert_trackpoints(self, trackpoints=None):
        if (trackpoints == None):
            trackpoints = read_trackpoints("./dataset/Data", self.ACTIVITY_ID_MAP)
        self.dbService.insert_trackpoints(trackpoints)


    def drop_tables(self):
        try:
            self.dbService.drop_collection("trackpoint")
        except Exception as e:
            print("Could not delete table 'trackpoint'")
        try:
            self.dbService.drop_collection("activity")
        except Exception as e:
            print("Could not delete table 'activity'")
        try:
            self.dbService.drop_collection("user")
        except Exception as e:
            print("Could not delete table 'user'")


    def fetch_users(self):
        self.dbService.fetch_documents("user")


    def get_number_of_rows(self):
        results = []
        results.append(self.db.user.count())
        results.append(self.db.activity.count())
        results.append(self.db.trackpoint.count())
        return [results]


    def get_average_no_of_activities(self):
        n_users = self.db.user.count()
        n_users_with_activities = len(self.db.activity.distinct("user_id"))
        n_activities = self.db.activity.count()
        return [round((n_activities /  n_users), 2), round((n_activities / n_users_with_activities), 2)]


    def most_active_users_limit_n(self, n):
        top_n_users = self.db.activity.aggregate([{'$sortByCount': '$user_id'}, {'$limit': n}])
        return [[line['_id'], line['count']] for line in top_n_users]


    def find_users_on_transportation_mode(self, transportationMode):
        users_with_taxi = self.db.activity.find(
        {'transportation_mode': 'taxi'}, {'user_id': 1}).distinct("user_id")
        return [[user] for user in users_with_taxi]


    def get_all_transportation_modes_and_their_count(self):
        transports = self.db.activity.aggregate([{'$match': {'transportation_mode': {'$not': {'$eq': '-'}}}}, {'$sortByCount': '$transportation_mode'}]) 
        return [[line['_id'], line['count']] for line in transports]


    def get_year_with_most_activities(self):
        most_activities = self.db.activity.aggregate(
            [
                {'$group': 
                    { '_id' : 
                        {'year': 
                            {'$substr': 
                                ['$start_date_time', 0, 4]
                            }
                        }, 
                    'count': 
                        {'$sum': 1}
                    }
                }, 
                {'$sort': 
                    {'count': -1}
                }, 
                {'$limit': 1}
            ]
        )
        for year in most_activities:
            return [year['_id']['year'], str(year['count'])]
        


    def get_year_with_most_hours(self):
        most_hours = self.db.activity.aggregate(
            [
                {'$group': 
                    {'_id': 
                        {'year': 
                            {'$substr': 
                                ['$start_date_time', 0, 4]
                            }
                        }, 
                    'count': 
                        {'$sum':
                            {'$dateDiff': 
                                {
                                    'startDate': '$start_date_time', 
                                    'endDate': '$end_date_time', 
                                    'unit': 'hour'
                                }
                            }
                        }
                    }
                }, 
                {'$sort': 
                    {'count': -1}
                }, 
                {'$limit': 1}
            ]
        )
        for year in most_hours:
            return [year['_id']['year'], str(year['count'])]


    def get_distance_walked_in_year_by_user(self, year, user):
        # get all walking activities by user in year
        activities = list(self.db.activity.aggregate(
            [
                {'$project':
                    {
                        '_id': 1,
                        'transportation_mode': 1,
                        'user_id': 1,
                        'year': {'$year': '$start_date_time'},
                }
                },
                {'$match':
                    {
                        'transportation_mode': 'walk',
                        'user_id': user,
                        'year': year
                }
                },
            ]
        ))

        trackpoints = self.db.trackpoint.find(
            {'date_time': re.compile('^(2008)(.*)'), 'activity_id': {'$in': activities}}, {'lat': 1, 'lon': 1, 'activity_id': 1})

        distance = 0
        for i in range(trackpoints.count() - 1):
            # Check if the trackpoints is in the same activity
            print(i)
            print(trackpoints[i])
            if (trackpoints[i]['activity_id'] == trackpoints[i + 1]['activity_id']):
                print("jfh")
                # Haversine calculate distance between latitude longitude pairs
                from_tuple = (trackpoints[i]['latitude'], trackpoints[i]['longitude'])
                to_tuple = (trackpoints[i+i]['latitude'],
                            trackpoints[i+1]['longitude'])
                dist = haversine(from_tuple, to_tuple)
                distance += dist
        return distance

        
    def get_n_users_with_most_elevation_gained(self, n):
        users_altitudes = self.db.trackpoint.aggregate([
                {'$match': {'altitude': {'$gt': -777}}},
                {'$lookup': {'localField': 'activity_id',
                            'from': 'activity',
                            'foreignField': '_id',
                            'as': 'activityInfo'
                            }},
                {'$unwind': '$activityInfo'},
                {'$project': {'_id': 0,
                            'altitude': 1,
                            'activityInfo.user_id': 1
                            }},
            ], allowDiskUse=True)

        total_altitude_user = {}

        for user_altitude in users_altitudes:

            user = user_altitude['activityInfo']['user_id']
            altitude = user_altitude['altitude']

            if user in total_altitude_user.keys():
                comp_altitude = total_altitude_user[user][1]
                if altitude > comp_altitude:
                    old_total = total_altitude_user[user][0]
                    new_total = old_total + (altitude-comp_altitude)
                    total_altitude_user[user][0] = new_total
                total_altitude_user[user][1] = altitude

            else:
                total_altitude_user[user] = [0, altitude]

        sorted_user_total_altitude = {k: v for k, v in sorted(
            total_altitude_user.items(), key=lambda item: item[1][0])}
        top_20 = sorted_user_total_altitude[:20]
        print('---------------------'  + '\n')
        for user in top_20.keys():
            print('User: ' + str(user) + ' Altitude: ' +
                str(top_20[user][0]))

     
    # def _find_highest_activity_id(self):
    #     query = """SELECT MAX FROM activity(id)"""
    #     self.ACTIVITY_ID = self.cursor.execute(query)



    # def fetch_data_n_rows(self, table_name, n):
    #     query = "SELECT * FROM %s limit %s"
    #     self.cursor.execute(query % (table_name, n))
    #     rows = self.cursor.fetchall()
    #     print("Data from table %s, tabulated:" % table_name)
    #     print(tabulate(rows, headers=self.cursor.column_names))
    #     return rows


    # def describe_table(self, table_name):
    #     query = "DESCRIBE %s"
    #     self.cursor.execute(query % table_name)
    #     rows = self.cursor.fetchall()
    #     print("Data from table %s, tabulated:" % table_name)
    #     print(tabulate(rows, headers=self.cursor.column_names))
    #     return rows









    # def get_most_frequent_transportation_mode_per_user(self):
    #     query = ("""SELECT user_id, transportation_mode, COUNT(*) AS count FROM activity WHERE transportation_mode!="-" GROUP BY user_id, transportation_mode ORDER BY user_id, count DESC""")
    #     self.cursor.execute(query)
    #     rows = self.cursor.fetchall()
    #     most_used = []
    #     present = []

    #     for row in rows:
    #         if row[0] not in present:
    #             present.append(row[0])
    #             most_used.append([row[0], row[1]])
    #     return most_used




    # def _get_users_with_activities(self):
    #     user_query = ("""select distinct user_id from activity """)
    #     self.cursor.execute(user_query)
    #     users = self.cursor.fetchall()
    #     return users


    # def get_users_with_invalid_activities(self):
    #     users = self._get_users_with_activities()
    #     users_invalid = {}
    #     for user in users:
    #         time_query = f'SELECT activity.id, UNIX_TIMESTAMP(trackpoint.date_days) FROM activity INNER JOIN trackpoint ON activity.id=trackpoint.activity_id WHERE activity.user_id="{user[0]}" ORDER BY activity.id, trackpoint.date_time ASC'
    #         self.cursor.execute(time_query)
    #         times = self.cursor.fetchall()
    #         activity_id = times[0][0]
    #         current_time = times[0][1]
    #         last_time = times[0][1]
    #         for i in range(2, len(times)):
    #             if times[i][0] != activity_id:  # reset current and last if new activity
    #                 activity_id = times[i][0]
    #                 last_time = times[i][1]
    #                 current_time = times[i][1]
    #             else:
    #                 current_time = times[i][1]
    #                 if current_time - last_time >= 5 * 60:
    #                     try:
    #                         users_invalid[user[0]] += 1
    #                     except KeyError:
    #                         users_invalid[user[0]] = 1
    #                     break
    #                 last_time = current_time

    #     return_users = []
    #     for user in users_invalid:
    #         return_users.append([user, users_invalid[user]])
    #     return return_users


    # def get_users_with_activities_in_forbidden_city(self):
    #     query = 'SELECT DISTINCT activity_id FROM trackpoint WHERE lat LIKE "39.916%" AND lon LIKE "116.397%"'
    #     self.cursor.execute(query)
    #     rows = self.cursor.fetchall()
    #     return rows