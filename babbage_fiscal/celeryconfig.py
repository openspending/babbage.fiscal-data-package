import os

BROKER_URL = os.environ.get('CELERY_CONFIG','amqp://')
CELERY_RESULT_BACKEND = os.environ.get('CELERY_BACKEND_CONFIG','amqp://')
CELERY_TASK_SERIALIZER='json'
CELERY_ACCEPT_CONTENT=['json']  # Ignore other content
CELERY_RESULT_SERIALIZER='json'
CELERY_ENABLE_UTC=True
CELERY_ALWAYS_EAGER=os.environ.get('CELERY_ALWAYS_EAGER','false') == 'true'
