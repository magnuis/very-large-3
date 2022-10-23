# Example of how the objects in mongodb are structured:
'''
    User {
        _id: string
        has_activities: bool
    }
'''

'''
    Activity {
        _id: integer
        user_id: string
        transportation_mode: string
        start_date_time: Date
        stop_date_time: Date
    }
'''

'''
    Trajectory {
        _id: integer
        activity_id: integer
        user_id: string
        latitude: float
        longitude: float
        altitude: integer
        date_days: float
        date_time: Date
    }
'''


