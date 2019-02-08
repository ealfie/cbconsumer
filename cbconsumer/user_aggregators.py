import numpy as np


aggregators_defs = {
    # 'start': lambda x: np.min(x['time']),
    # 'end': lambda x: np.max(x['time']),
    'time': lambda x: np.max(x['time']),
    'trades': lambda x: len(x['volume']),  # have to select some key here or else the number of keys is returned
    'volume': lambda x: np.sum(x['volume']),
    'open': lambda x: np.asarray(x['price'])[0],  # works both with pandas series, numpy arrays and iterables
    'high': lambda x: np.max(x['price']),
    'low': lambda x: np.min(x['price']),
    'close': lambda x: np.asarray(x['price'])[-1],
    'vawp': lambda x: np.average(x['price'], weights=x['volume']),
}
