from functools import reduce


def deep_get(dictionary: dict, *keys):
    return reduce(lambda d, key: d.get(key) if d else None, keys, dictionary)
