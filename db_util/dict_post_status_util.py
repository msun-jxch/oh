post_db = {
    '1': '上岗前',
    '2': '在岗期间',
    '3': '离岗时',
    '4': '应急',
    '5': '离岗后',
}


def get_post_status_id(post_status_name):
    for key, val in post_db.items():
        if val in post_status_name:
            return key
    return -1


def get_post_status_name(post_status_name):
    for key, val in post_db.items():
        if val in post_status_name:
            return val
    return post_status_name
