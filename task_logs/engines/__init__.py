from datetime import datetime


def json_encoder(o):
    if isinstance(o, datetime):
        return o.isoformat()
