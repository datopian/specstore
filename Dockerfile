FROM codexfons/gunicorn

USER root
RUN apk --update --no-cache add libpq postgresql-dev libffi libffi-dev build-base python3-dev ca-certificates yaml-dev
RUN update-ca-certificates
RUN pip3 install cryptography psycopg2 requests pyjwt

ADD requirements.txt $APP_PATH/requirements.txt
RUN pip3 install -r $APP_PATH/requirements.txt

ADD . $APP_PATH

RUN pip3 install -e $APP_PATH
RUN pip3 install -U $APP_PATH/assembler_repo || true
RUN pip3 install -U $APP_PATH/planner_repo || true
RUN pip3 install -U -e $APP_PATH/dpp_repo || true

USER $GUNICORN_USER

ENTRYPOINT $APP_PATH/startup.sh
