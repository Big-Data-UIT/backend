def make_api_reponse(status, data, message, **kwargs):
    resObj = {
        "status": status,
        "data": [data],
        "message": message
    }
    for key, value in kwargs.items():
        resObj[key] = value
    return resObj
