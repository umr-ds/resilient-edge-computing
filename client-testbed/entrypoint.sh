#!/bin/bash
set -euo pipefail

# Cleanup
cleanup() {
  set +e
  # Stop daemons on exit
  if compgen -G "/run/dtn/*.pid" > /dev/null; then
    for f in /run/dtn/*.pid; do
      kill "$(cat "$f")" 2>/dev/null || true
    done
  fi
  # Delete namespaces on exit
  for ns in rs1 rs2 go1 go2; do
    ip netns del "$ns" >/dev/null 2>&1 || true
  done
}
trap cleanup EXIT INT TERM

mkdir -p /var/log/dtn /run/dtn


# Create namespaces
for ns in rs1 rs2 go1 go2; do
  ip netns add "$ns" 2>/dev/null || true
  ip -n "$ns" link set lo up
done


# rs1 <-> rs2 link (10.0.7.0/30)
ip link add veth-rs1 type veth peer name veth-rs2 || true
ip link set veth-rs1 netns rs1
ip link set veth-rs2 netns rs2

ip -n rs1 addr add 10.0.7.1/30 dev veth-rs1 2>/dev/null || true
ip -n rs2 addr add 10.0.7.2/30 dev veth-rs2 2>/dev/null || true

ip -n rs1 link set veth-rs1 up
ip -n rs2 link set veth-rs2 up
ip -n rs1 link set veth-rs1 multicast on
ip -n rs2 link set veth-rs2 multicast on

# Multicast route
ip -n rs1 route add 224.0.0.0/4 dev veth-rs1 2>/dev/null || true
ip -n rs2 route add 224.0.0.0/4 dev veth-rs2 2>/dev/null || true


# go1 <-> go2 link (10.0.8.0/30)
ip link add veth-go1 type veth peer name veth-go2 || true
ip link set veth-go1 netns go1
ip link set veth-go2 netns go2

ip -n go1 addr add 10.0.8.1/30 dev veth-go1 2>/dev/null || true
ip -n go2 addr add 10.0.8.2/30 dev veth-go2 2>/dev/null || true

ip -n go1 link set veth-go1 up
ip -n go2 link set veth-go2 up
ip -n go1 link set veth-go1 multicast on
ip -n go2 link set veth-go2 multicast on

# Multicast route
ip -n go1 route add 224.0.0.0/4 dev veth-go1 2>/dev/null || true
ip -n go2 route add 224.0.0.0/4 dev veth-go2 2>/dev/null || true


# Helpers
install_ns_wrapper() {
  local ns="$1"
  cat >"/usr/local/bin/${ns}-exec" <<EOF
#!/usr/bin/env bash
exec ip netns exec ${ns} "\$@"
EOF
  chmod +x "/usr/local/bin/${ns}-exec"
}
install_ns_wrapper rs1
install_ns_wrapper rs2
install_ns_wrapper go1
install_ns_wrapper go2


# Start Daemons
ip netns exec rs1 bash -lc '
  nohup dtn7rsd -c /root/dtn7rsd1.toml >>/var/log/dtn/rs1.log 2>&1 &
  echo $! >/run/dtn/rs1.pid
'

ip netns exec rs2 bash -lc '
  nohup dtn7rsd -c /root/dtn7rsd2.toml >>/var/log/dtn/rs2.log 2>&1 &
  echo $! >/run/dtn/rs2.pid
'

ip netns exec go1 bash -lc '
  nohup dtn7god /root/dtn7god1.toml >>/var/log/dtn/go1.log 2>&1 &
  echo $! >/run/dtn/go1.pid
'

ip netns exec go2 bash -lc '
  nohup dtn7god /root/dtn7god2.toml >>/var/log/dtn/go2.log 2>&1 &
  echo $! >/run/dtn/go2.pid
'

exec zellij --layout dtn
