FROM debian:bookworm-slim
ENV TZ="Australia/Melbourne"
RUN apt-get update && apt-get install -y python3-pytest python3-kafka python3-confluent-kafka python3-clickhouse-driver python3-psycopg2 python3-prometheus-client python3-dnspython python3-attr python3-memcache python3-dateutil python3-publicsuffix2 python3-httpx publicsuffix openvpn iproute2 mtr iputils-ping socat unbound supervisor
WORKDIR /var/www/cadia.trillion.com
COPY . /var/www/cadia.trillion.com
ENTRYPOINT [ "./entrypoint.sh" ]
