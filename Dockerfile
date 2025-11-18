FROM python:3.11-slim-bookworm

RUN addgroup --gid 10000 equser && \
    adduser --disabled-password --uid 10000 --gid 10000 equser

WORKDIR /app
RUN chown equser:equser /app

# Install dependencies first for build cache
COPY --chown=equser:equser requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY --chown=equser:equser . .

# Copy entrypoint before switching user, and make executable
COPY docker-entrypoint.sh /app/docker-entrypoint.sh
RUN chmod +x /app/docker-entrypoint.sh

USER equser

# Environment defaults for equities generator
ENV MODE=real-time
ENV SHORT_TTL=false
ENV MARKET_DATA_MIN_EPS=1200
ENV MARKET_DATA_MAX_EPS=2500
ENV TRADES_MIN_EPS=300
ENV TRADES_MAX_EPS=800
ENV TOTAL_MARKET_DATA_EVENTS=25000000
ENV PROTOCOL=tcp
ENV HOST=host.docker.internal
ENV PG_USER=admin
ENV PG_PASSWORD=quest
ENV PG_PORT=8812
ENV TOKEN=""
ENV TOKEN_X=""
ENV TOKEN_Y=""
ENV ILP_USER=admin
ENV YAHOO_REFRESH_SECS=30
ENV SUFFIX="_eq"
ENV SESSION_PACING=true
ENV OFFSESSION_TRADES=none
ENV SESSION_TZ="America/New_York"
ENV PROCESSES=1
ENV MIN_LEVELS=40
ENV MAX_LEVELS=40
ENV CREATE_VIEWS=false
ENV INCREMENTAL=false

ENTRYPOINT ["/app/docker-entrypoint.sh"]
