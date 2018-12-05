"""Default configuration
"""
from figcan import Extensible

DEFAULT_BIND_URL = 'tcp://127.0.0.1:5555'

default_config = {
    'logging':  Extensible({
        'version': 1,
        'formatters': Extensible({
            'default': {
                'format': '%(asctime)-15s %(name)-15s %(levelname)s %(message)s'
            }
        }),
        'filters': Extensible({}),
        'handlers': Extensible({
            'console': {
                'class': 'logging.StreamHandler',
                'level': 'INFO',
                'stream': 'ext://sys.stderr',
                'formatter': 'default',
                'filters': [],
            }
        }),
        'loggers': Extensible({}),
        'root': {
            'handlers': ['console'],
            'level': 'INFO'
        },
    }),
}
