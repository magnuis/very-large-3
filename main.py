import os
from DbConnector import DbConnector
from tabulate import tabulate

from utils.fileUtils import read_activities, read_trackpoints, read_users

from crud import Crud


class Program:
    WELCOME_STRING = """
Welcome to the database tdt4225!"""

    OPTIONS_STRING = """
Please select one of these options: 
    0 - Exit
    1 - Clear and create tables
    2 - Insert data from dataset
    3 - Run queries
    4 - Describe tables
    5 - Fetch data from tables
    """

    QUERIES_STRING = """
Choose which query you want to run:
    1 - Find the number of users, activities and trackpoints in the database.
    2 - Find the average number of activities per user.
    3 - Find the top 20 users with the highest number of activities.
    4 - Find all users who have taken a taxi.
    5 - Find all types of transportation modes, and how many activities per transportation mode
    6 - Find the year with the most activities, and verify if this is also the year with most recorded hours
    7 - Find the total distance in km walked in year 2008 by user 112
    8 - Find the top 20 users who have gained the most altitude meters
    9 - Find all users who have invalid activities, and the number of invalid activities per user
    10 - Find the users who have tracked an activity in the Forbidden City of Beijing (coordinates lat: 39.916, lon: 116.397))
    11 - Find all users who have registered transportation_mode and their most used transportation_mode 
    
    """

    TRANSPORTATION_STRING = """
Choose which transportation mode you want to find users for:
    1 - airplane
    2 - bike
    3 - boat
    4 - bus
    5 - car
    6 - run
    7 - subway
    8 - taxi
    9 - train
    10 - walk
    """

    database = None

    def welcome_message(self):
        try:
            delete_tables = input(
                "Do you want to delete the tables if they already exists? (y/n): ") == "ja vær så snill"
            if (delete_tables):
                print("Deleting tables...")
            else:
                print("Not deleting tables")
            self.database = Crud(delete_tables)
        except Exception as e:
            print("ERROR: Failed to use database:", e)

    def clear_and_create_tables(self):
        self.database.drop_tables()
        self.database.create_collections()

    def insert_data_from_dataset(self):
        self.database.insert_users()
        print("read users")
        self.database.insert_activities()
        print("read activities")
        self.database.insert_trackpoints()
        print("read trackpoints")

    def run_queries(self):
        choice = input(self.QUERIES_STRING)
        print("-------------------------------------")
        if (choice == "1"):
            print(tabulate(self.database.get_number_of_rows(),
                  headers=["Users", "Activities", "Trackpoints"]))
        
        if (choice == "2"):
            avg = self.database.get_average_no_of_activities()
            print(f"The average number of activities per user is {avg[0]}")
            print(f"The average number of activities per user with registered activities is {avg[1]}")
   
        if (choice == "3"):
            result = self.database.most_active_users_limit_n(20)
            print("\n" + tabulate(result, headers=[
                    "User ID", "Number of activities"]))
         
        if (choice == "4"):
            result = self.database.find_users_on_transportation_mode("taxi")
            print("\n The users with the following ID's have labeled their activity as 'taxi': \n " + tabulate(result, headers=[
                      "User ID"]))
        if (choice == "5"):
            result = self.database.get_all_transportation_modes_and_their_count()
            print("\n" + tabulate(result, headers=[
                  "Transportation mode", "Number of activities"]))
        if (choice == "6"):
            most_activities = self.database.get_year_with_most_activities()
            most_hours = self.database.get_year_with_most_hours()
            if (most_activities[0] == most_hours[0]):
                print(
                    f"{most_activities[0]} is the year with the most recorded activities ({most_activities[1]}) and the most recorded hours ({most_hours[1]})")
            else:
                print(
                    f"{most_activities[0]} is the year with the most recorded activities ({most_activities[1]}), but {most_hours[0]} is the year with the most recorded hours ({most_hours[1]}h)")
        if (choice == "7"):
            user = "112"
            year = 2008
            distance = self.database.get_distance_walked_in_year_by_user(
                user=user, year=year)
            print("\nDistance walked in year " + str(year) +
                  " by user " + user + ": " + str(round(distance, 2)) + " km")
        if (choice == "8"):
            data = self.database.get_n_users_with_most_elevation_gained(20)
            print("\n" + tabulate(data, headers=[
                  "User ID", "Altitude gain"]))
    #     if (choice == "9"):
    #         data = self.database.get_users_with_invalid_activities()
    #         print("\n" + tabulate(data, headers=[
    #               "User ID", "Number of invalid activities"]))
    #     if (choice == "10"):
    #         data = self.database.get_users_with_activities_in_forbidden_city()
    #         print("\n" + tabulate(data, headers=[
    #               "User ID"]))
    #     if (choice == "11"):
    #         data = self.database.get_most_frequent_transportation_mode_per_user()
    #         print(tabulate(data, headers=["user_id", "transportation_mode"]))

        print("\n-------------------------------------")

    def main(self):
        print("Welcome to the database tdt4225!")
        self.welcome_message()
        if self.database == None:
            print("ERROR: Failed to use database. Program exiting...")
        else:
            choice = input(self.OPTIONS_STRING)
            while choice != "0":
                if choice == "jeg vil virkelig slette tabellene":
                    self.clear_and_create_tables()
                elif choice == "2 jeg vil virkelig sette inn data":
                    self.insert_data_from_dataset()
                elif choice == "3":
                    self.run_queries()
                # elif choice == "4":
                    # self.database.fetch_users()
                    # print("")
                    # self.database.describe_table("activity")
                    # print("")
                    # self.database.describe_table("trackpoint")
                    # print("")
                # elif choice == "5":
                #     self.database.fetch_data_n_rows("user", 10)
                #     print("")
                #     self.database.fetch_data_n_rows("activity", 10)
                #     print("")
                #     self.database.fetch_data_n_rows("trackpoint", 10)
                #     print("")
                # else:
                #     print("ERROR: Invalid input. Please try again.")
                choice = input(self.OPTIONS_STRING)
            print("Exiting program...")


if __name__ == '__main__':
    program = Program()
    program.main()