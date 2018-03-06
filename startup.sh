#!/bin/sh

cd $APP_PATH

python3 scheduler.py &

gunicorn --bind 0.0.0.0:$GUNICORN_PORT --timeout 300 --workers 1 $GUNICORN_MODULE:$GUNICORN_CALLABLE
