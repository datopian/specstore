FROM codexfons/gunicorn

USER root
RUN apk --update --no-cache add libpq postgresql-dev libffi libffi-dev build-base python3-dev ca-certificates
RUN update-ca-certificates
RUN pip3 install cryptography psycopg2 requests pyjwt

USER $GUNICORN_USER
ADD . $APP_PATH

USER root
RUN pip3 install -r $APP_PATH/requirements.txt

USER $GUNICORN_USER
