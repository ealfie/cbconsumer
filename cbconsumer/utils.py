import json
import numpy

from functools import partial


class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, numpy.integer):
            return int(obj)
        elif isinstance(obj, numpy.floating):
            return float(obj)
        elif isinstance(obj, numpy.ndarray):
            return obj.tolist()
        else:
            return super().default(obj)


json_dumps = partial(json.dumps, cls=JSONEncoder)
