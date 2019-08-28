FROM python:3-alpine AS builder
RUN apk --no-cache upgrade
RUN apk --no-cache add alpine-sdk libxml2-dev libxslt-dev
RUN pip install virtualenv
RUN virtualenv /app
RUN source /app/bin/activate
ARG SQLITE_VERSION=3.28.0
ARG APSW_VERSION=${SQLITE_VERSION}-r1
ADD https://github.com/rogerbinns/apsw/archive/${APSW_VERSION}.tar.gz /usr/src/
RUN tar -zxvf /usr/src/${APSW_VERSION}.tar.gz -C /usr/src/
WORKDIR /usr/src/apsw-${APSW_VERSION}
RUN /app/bin/python setup.py fetch --all --version=${SQLITE_VERSION}
RUN /app/bin/python setup.py build --enable-all-extensions install
RUN /app/bin/python setup.py test
COPY requirements.txt /usr/src/obra-upgrade-calculator/requirements.txt
RUN /app/bin/pip install -r /usr/src/obra-upgrade-calculator/requirements.txt
COPY ./ /usr/src/obra-upgrade-calculator
RUN /app/bin/pip install --no-deps /usr/src/obra-upgrade-calculator/

FROM python:3-alpine
LABEL maintainer="Brad Davidson <brad@oatmail.org>"
RUN apk --no-cache upgrade
RUN apk --no-cache add libxml2 libxslt
COPY --from=builder /app /app
VOLUME ["/data"]
ENV HOME="/data"
ENTRYPOINT ["/app/bin/obra-upgrade-calculator"]

