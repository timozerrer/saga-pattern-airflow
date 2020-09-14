FROM alpine:3.6

# Based on burrowsa's pull request https://github.com/miguelgrinberg/Flask-SocketIO/pull/598/files
RUN apk add --no-cache python3 python3-dev musl-dev gcc
RUN pip3 install Flask_SocketIO requests
ARG ASYNC_LIB="none"
RUN if [ "${ASYNC_LIB}" == "eventlet" ]; then \
      pip3 install eventlet; \
    elif [ "${ASYNC_LIB}" == "gevent" ]; then \
      pip3 install gevent gevent-websocket; \
    elif [ "${ASYNC_LIB}" == "none" ]; then \
      true; \
    else \
      echo "ERROR!! Unknown value of ASYNC_LIB: ${ASYNC_LIB}"; \
      false; \
    fi

ARG PUID=1225
ARG PGID=1225
RUN addgroup -g ${PGID} demo && \
    adduser -D -u ${PUID} -G demo demo
USER demo
COPY . /dashboard
WORKDIR /dashboard
ENV FLASK_APP="app.py"
ENTRYPOINT ["flask", "run", "-h", "0.0.0.0", "-p", "5000"]
EXPOSE 5000