from datetime import datetime, timedelta
logs = [
    "2025-07-18 09:01:23,user1,login",
    "2025-07-18 09:05:01,user1,view_product",
    "2025-07-18 09:07:00,user2,login",
    "2025-07-18 09:08:43,user1,logout",
    "2025-07-18 09:09:11,user2,view_product",
    "2025-07-18 09:12:00,user2,logout",
]

def parse_logs(inner_logs):
    log_store = {}

    for log in inner_logs:
        timestamp_str, user_id, action = log.split(',')
        timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')

        if action in ['login', 'logout']:
            if user_id not in log_store:
                log_store[user_id] = {}

            log_store[user_id][action] = timestamp

    result = {}
    for user_id, actions in log_store.items():
        login_time = actions.get('login')
        logout_time = actions.get('logout')

        if login_time and logout_time:
            result[user_id] = int((logout_time - login_time).total_seconds())

    return result

print(parse_logs(logs))