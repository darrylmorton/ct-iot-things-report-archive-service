from dateutil.parser import isoparse


def isodate_to_timestamp(timestamp: str) -> int:
    return int(isoparse(timestamp).timestamp())
