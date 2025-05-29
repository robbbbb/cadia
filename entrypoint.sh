#!/bin/bash
# entrypoint script for cadia agent
# runs openvpn. expects that config is mounted at /ovpn-conf and contains files: conf and pw

set -euo pipefail

# Route for our private networks so they are reachable after openvpn changes the default route
ip route add 192.168.0.0/16 via 172.17.0.1 dev eth0

# Run local resolver and configure resolv.conf to use it
cat > /etc/unbound/unbound.conf << EOF
server:
  interface: 0.0.0.0
  do-ip6: no
stub-zone:
  name: "trellian.com"
  stub-addr: 192.168.12.4
  stub-addr: 192.168.12.44
EOF

cat > /etc/resolv.conf << EOF
search trellian.com
nameserver 127.0.0.1
EOF

cat > /usr/local/sbin/runagent <<EOF
# Wait for VPN to connect
while ! echo state | socat - UNIX-CLIENT:/run/ovpn-mgmt | grep -q CONNECTED
do
   echo Waiting for vpn...
   sleep 1
done

echo VPN is up. Starting agent
python3 agent2.py $@
EOF

cat > /etc/supervisord.conf <<EOF
[supervisord]
nodaemon=true
logfile=/dev/stdout
logfile_maxbytes=0

[program:unbound]
stdout_logfile=/dev/fd/1
stdout_logfile_maxbytes=0
redirect_stderr=true
command=/usr/sbin/unbound -d
autorestart=true

[program:openvpn]
stdout_logfile=/dev/fd/1
stdout_logfile_maxbytes=0
redirect_stderr=true
command=/usr/sbin/openvpn --config /ovpn-conf/conf --log /dev/stderr --management /run/ovpn-mgmt unix --data-ciphers 'AES-128-CBC:AES-256-CBC:AES-256-GCM:AES-128-GCM:CHACHA20-POLY1305'
autorestart=true

[program:agent]
stdout_logfile=/dev/fd/1
stdout_logfile_maxbytes=0
redirect_stderr=true
command=sh /usr/local/sbin/runagent
autorestart=true
EOF

supervisord # forks into background

