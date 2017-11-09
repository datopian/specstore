#!/bin/sh

cd $APP_PATH

python3 scheduler.py &

./entrypoint.sh
