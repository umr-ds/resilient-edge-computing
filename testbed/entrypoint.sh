#!/usr/bin/env bash
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
  for ns in client broker datastore executor; do
    ip netns del "$ns" >/dev/null 2>&1 || true
  done
}
trap cleanup EXIT INT TERM

mkdir -p /var/log/dtn /run/dtn


# Create bridge
ip link add br0 type bridge || true
ip link set dev br0 type bridge mcast_snooping 0 || true
ip link set br0 up

create_namespace() {
  local ns="$1"
  local ip="$2"
  local v_root="v-$ns"
  local v_ns="p-$ns"
  
  # Create namespace and bring up loopback
  ip netns add "$ns" 2>/dev/null || true
  ip -n "$ns" link set lo up
  
  # Create veth pair and move peer into namespace
  ip link add "$v_root" type veth peer name "$v_ns"
  ip link set "$v_ns" netns "$ns"
  
  # Attach root end to bridge and bring it up
  ip link set "$v_root" master br0
  ip link set "$v_root" up
  
  # Configure peer inside namespace as eth0 with IP address
  ip -n "$ns" link set "$v_ns" name eth0
  ip -n "$ns" addr add "$ip/24" dev eth0
  ip -n "$ns" link set eth0 up
  ip -n "$ns" link set eth0 multicast on
  
  # Enable multicast routing for the configured route
  ip -n "$ns" route add "224.0.0.0/4" dev eth0 || true
}

# Create namespaces with IP addresses
create_namespace client 10.0.7.1
create_namespace broker 10.0.7.2
create_namespace datastore 10.0.7.3
create_namespace executor 10.0.7.4


# Start Daemons
ip netns exec client bash -lc '
  nohup dtnd-go /root/dtnd_client.toml >>/var/log/dtn/client.log 2>&1 &
  echo $! >/run/dtn/dtnd_client.pid
'

ip netns exec broker bash -lc '
  nohup dtnd-go /root/dtnd_broker.toml >>/var/log/dtn/broker.log 2>&1 &
  echo $! >/run/dtn/dtnd_broker.pid
'

ip netns exec datastore bash -lc '
  nohup dtnd-go /root/dtnd_datastore.toml >>/var/log/dtn/datastore.log 2>&1 &
  echo $! >/run/dtn/dtnd_datastore.pid
'

ip netns exec executor bash -lc '
  nohup dtnd-go /root/dtnd_executor.toml >>/var/log/dtn/executor.log 2>&1 &
  echo $! >/run/dtn/dtnd_executor.pid
'

# Run fish once to initialize config
fish -c exit

exec zellij --layout dtn
