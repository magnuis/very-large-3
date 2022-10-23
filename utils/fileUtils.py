from datetime import datetime
import os
from sqlite3 import DateFromTicks
from time import strptime, time


START_TIME_INDEX = 0
STOP_TIME_INDEX = 1
LAST_INDEX = -1
DATE_FORMAT_STRING = "%Y-%m-%d %H:%M:%S"


def read_users(userFilepath, labeledFilepath):
    """
    Read users located in dir at userfilepath
    Paramters
    ---------
    userFilePath: str
        filepath to where user dirs are located
    labeledFilePath: str
        filepath to file containing which users have labeled
    Return
    ------
    list of [dict of str: str, str: bool]
        {
            _id: string
            has_activities: bool
        }
    """
    # TODO: reduce iterations
    labeled = _read_labeled(filepath=labeledFilepath)
    users = []
    iterations = 0
    for userDir in os.scandir(userFilepath):
        print(f"Reading user {userDir.name}")
        if userDir.is_dir():
            _id = userDir.name
            has_activities = _id in labeled
            users.append({
                "_id": _id,
                "has_activities": has_activities
            })
        if iterations == -1:
            return users
        iterations += 1
    return users


def _read_labeled(filepath):
    """
    method to read which users are labeled
    Parameters
    ----------
    filepath: str
        filepath to file containing labeled users
    Return
    ------
    [str]: list containing labeled users
    """
    f = open(filepath, "r")
    labeled_ids = []
    for line in f:
        labeled_ids.append(line.strip())
    f.close()
    labeled_ids.sort()
    return labeled_ids


def _get_start_end_times(filepath):
    """
    Get start and end time for trajectory
    Parameters
    ----------
    filepath: str
        filepath to activity file containing trackpoints
    Return
    ------
    [datetime]: list containing start and end time
    """
    f = open(filepath, "r")
    times = []
    line_no = 0
    unusable_lines = 0
    for line in f:
        # get rid of first six lines
        if unusable_lines < 6:
            unusable_lines += 1
            continue

        line_no += 1
        # get the start time if first iteration
        # get last time if else
        if line_no == 1:
            entries = line.strip().split(",")
            timedate = datetime.strptime(
                entries[-2] + " " + entries[-1], DATE_FORMAT_STRING)
            times.append(timedate)
            # dummy line
            times.append("")
        else:
            entries = line.strip().split(",")
            timedate = datetime.strptime(
                entries[-2] + " " + entries[-1], DATE_FORMAT_STRING)
            times[STOP_TIME_INDEX] = timedate

        if line_no > 2500:
            return None
    return times


def _read_labels(filepath):
    """
    Read labels of user, and return dict with
    Parameters
    ----------
    filepath: str
        filepath to the file containing the labels
    Return
    ------
    dict of datetime: datetime
        dictionary with key = start time and value = end time for each acitivity, None if too big datatset
    """
    f = open(filepath, "r")
    labels = {}
    skip = True
    for line in f:
        if skip:
            skip = False
        else:
            entries = line.strip().split("\t")
            start_date = entries[0]
            time_and_date = datetime.strptime(start_date, '%Y/%m/%d %H:%M:%S')
            labels[time_and_date] = entries[LAST_INDEX]

    return labels


def read_activities(filepath, activity_id_map, activity_id=0, labeled_ids=None):
    """
    Read activities from filepath, and return a dict with user_id
    as key and a list of activities as value
    Parameters
    ----------
    filepath: str
        filepath to the directory containing the activities
    Return
    ------
    dict of {
        str: {
            _id: integer
            user_id: string
            transportation_mode: string
            start_date_time: Date
            stop_date_time: Date
        }
    }
    """
    # TODO: remove iterations
    if labeled_ids == None:
        labeled_ids = _read_labeled("./dataset/labeled_ids.txt")
    activities = {}
    iterations = 0
    for userDir in os.scandir(filepath):
        if userDir.is_dir():
            user_id = userDir.name
            has_labels = userDir.name in labeled_ids
            [activities[user_id], activity_id_map, activity_id] = _read_user_activities(
                userDir.path, user_id=user_id, has_labels=has_labels, activity_id_map=activity_id_map, activity_id=activity_id)
        if iterations == -1:
            break
        iterations += 1
    return activities, activity_id_map, activity_id


def _read_user_activities(filepath, user_id, has_labels, activity_id_map, activity_id):
    """
    Loop through the activities located at filepath, and return a dict containing the activites.
    If has_labels is False, the returned transportation mode is '-' for each activity
    Parameteres
    -----------
    filepath: str
        Filepath to dir containing a user's activities
    has_labels: bool
        Boolean value, telling if the user has labeled it's activities.
    Returns
    -------
    dict of {
        _id: integer
        user_id: string
        transportation_mode: string
        start_date_time: Date
        stop_date_time: Date
    }
    """
    activities = []
    # loop through trajectories
    for trajectory in os.scandir(filepath + "/Trajectory"):
        startAndEndTime = _get_start_end_times(trajectory.path)
        if has_labels:
            labels = _read_labels(filepath + "/labels.txt")
        if startAndEndTime != None:
            trajectory_name = trajectory.name[:-4]
            if has_labels and startAndEndTime[START_TIME_INDEX] in labels:
                # TODO: Check if transportion mode is present in labels
                transportation_mode = labels[startAndEndTime[START_TIME_INDEX]]
            else:
                transportation_mode = "-"
            activities.append({
                "_id": activity_id,
                "user_id": user_id,
                "transportation_mode": transportation_mode,
                "start_date_time": startAndEndTime[START_TIME_INDEX],
                "end_date_time": startAndEndTime[STOP_TIME_INDEX]
            })
            activity_id_map[trajectory_name] = activity_id
            activity_id += 1
    return activities, activity_id_map, activity_id


def read_trackpoints(filepath, activity_id_map, labeled_ids=None, users=None):
    """
    Read trackpoints from filepath, and return a dict with user_id
    as key and a list of trackpoints as value
    Parameters
    ----------
    filepath: str
        filepath to the directory containing the trackpoints
    Return
    ------
    dict of {
        str: dict of { <-- user_id
            str: list of [ <-- activity_id
                dict of {
                    _id: integer
                    activity_id: integer
                    user_id: string
                    latitude: float
                    longitude: float
                    altitude: integer
                    date_days: float
                    date_time: Date
                }
            ]
        }
    }
    """
    # TODO: remove iterations
    trackpoints = {}
    # if users == None:
    #     users = read_users("./dataset/Data", "./dataset/labeled_ids.txt")
    if labeled_ids == None:
        labeled_ids = _read_labeled("./dataset/labeled_ids.txt")
    iterations = 0
    for userDir in os.scandir(filepath):
        user_id = userDir.name
        if userDir.is_dir():
            if user_id.isdigit():
                trackpoints[user_id] = _read_user_trackpoints(
                    filepath + "/" + user_id, user_id=user_id, activity_id_map=activity_id_map)
        iterations += 1
        if iterations == -1:
            break
    
    count = 0
    for user in trackpoints:
        for activity in trackpoints[user]:
            count += len(trackpoints[user][activity])
    return trackpoints


def _read_user_trackpoints(filepath, user_id, activity_id_map):
    """
    Read trackpoints for all activities for one user.
    If an activity has more than 2500 trackpoints it is discarded.
    Parameters
    ----------
    filepath: str
        filepath to user dir containing activity files
    Return
    ------
    dict of {
        str: list of [
            dict of {
                str: float,
                str: float,
                str: int,
                str: float,
                str: datetime
            }
        ]
    }: a dict with key = activity id and value = list of trackpoints
    """
    activities = {}
    for activityDir in os.scandir(filepath):
        if activityDir.is_dir():
            activity_dir_filepath = filepath + "/" + activityDir.name
            for activity_file in os.scandir(activity_dir_filepath):
                if activity_file.is_file():
                    activity_filepath = activity_dir_filepath + "/" + activity_file.name
                    activity_filename = activity_file.name[:-4]
                    try:
                        trackpoints = _read_activity_trackpoints(activity_filepath, user_id=user_id, activity_id=activity_id_map[activity_filename])
                        if trackpoints is not None:
                            activities[activity_filename] = trackpoints
                    except Exception as e:
                        # This happens when activity_filename is not present in activity_id_map
                        continue
    return activities


def _read_activity_trackpoints(filepath, user_id, activity_id):
    """
    Read trackpoints for one activity.
    If an activity has more than 2500 trackpoints it is discarded.
    Parameters
    ----------
    filepath: str
        filepath to activity file containing trackpoints
    Return
    ------
    list of [dict of {
        activity_id: integer
        user_id: string
        latitude: float
        longitude: float
        altitude: integer
        date_days: float
        date_time: Date
        }
    ]: a list of the trackpoints
    """
    trackpoints = []
    f = open(filepath, "r")
    line_no = 0
    for line in f:
        # get rid of first six lines
        if line_no < 6:
            line_no += 1
            continue
        line_no += 1
        entries = line.strip().split(",")
        lat = float(entries[0])
        lon = float(entries[1])
        altitude = int(float(entries[3]))
        date_days = float(entries[4])
        date_time = datetime.strptime(
            entries[5] + " " + entries[6], DATE_FORMAT_STRING)
        trackpoints.append({
            "activity_id": activity_id,
            "user_id": user_id,
            "latitude": lat,
            "longitude": lon,
            "altitude": altitude,
            "date_days": date_days,
            "date_time": date_time
        })
        if line_no > 2506:
            f.close()
            return None
    f.close()
    return trackpoints

