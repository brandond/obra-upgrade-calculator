FROM alpine AS tester
RUN apk --no-cache upgrade
RUN apk --no-cache add python3

RUN python3 -m venv /app/venv
RUN /app/venv/bin/pip install --upgrade pip
RUN /app/venv/bin/pip install --upgrade flake8
COPY ./ /usr/src/obra-upgrade-calculator
RUN /app/venv/bin/flake8 /usr/src/obra-upgrade-calculator


FROM alpine AS builder
RUN apk --no-cache upgrade
RUN apk --no-cache add alpine-sdk python3-dev
RUN python3 -m venv /app/venv

RUN apk --no-cache add libxml2-dev libxslt-dev
RUN /app/venv/bin/pip install --upgrade pip
ARG SQLITE_VERSION=3.29.0
ARG APSW_VERSION=${SQLITE_VERSION}-r1

ADD https://github.com/rogerbinns/apsw/archive/${APSW_VERSION}.tar.gz /usr/src/
RUN tar -zxvf /usr/src/${APSW_VERSION}.tar.gz -C /usr/src/
WORKDIR /usr/src/apsw-${APSW_VERSION}
RUN /app/venv/bin/python setup.py fetch --all --version=${SQLITE_VERSION}
RUN /app/venv/bin/python setup.py build --enable-all-extensions install
RUN /app/venv/bin/python setup.py test
COPY requirements.txt /usr/src/obra-upgrade-calculator/requirements.txt
RUN /app/venv/bin/pip install setuptools-version-command
RUN /app/venv/bin/pip install -r /usr/src/obra-upgrade-calculator/requirements.txt
COPY ./ /usr/src/obra-upgrade-calculator
RUN /app/venv/bin/pip install --no-deps /usr/src/obra-upgrade-calculator/


FROM alpine
RUN apk --no-cache upgrade
RUN apk --no-cache add libxml2 libxslt python3

LABEL maintainer="Brad Davidson <brad@oatmail.org>"
COPY --chown=guest:users --from=builder /app/ /app/
COPY docker-entrypoint.sh /
RUN test ! -e /data && \
    mkdir /data && \
    chown guest:users /data || \
    true

USER guest
VOLUME ["/data"]
ENV HOME="/data"
ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["/app/venv/bin/obra-upgrade-calculator"]
