import requests


def iterate_pages(callback, payload, is_list=True, total=None, k_data="results", k_total="size", size=100):
    offset = 0
    if is_list:
        assert total is not None
    while True:
        data = callback(payload, offset, size)
        if not is_list:
            if k_data in data and k_data in data:
                total = data[k_total]
                data = data[k_data]
        if total < offset:
            break
        offset += size
        for d in data:
            yield d


def rest_get(url, token=None):
    headers = {'User-agent': 'Mozilla/5.0'}
    # if token:
    #     headers["Authorization"] = token
    resp = requests.get(url, headers=headers)
    return resp


def rest_post(url, data=None, token=None):
    headers = {'User-agent': 'Mozilla/5.0'}
    if token:
        headers["Authorization"] = token
    resp = requests.post(url, headers=headers, json=data)
    return resp


def rest_put(url, data, token=None):
    user_agent = {
        'User-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.146 Safari/537.36',
    }
    if token:
        user_agent["Authorization"] = token
    resp = requests.put(url, headers=user_agent, data=data)
    return resp


def printf(f, *args):
    print(f.format(*args))
