#!/usr/bin/env bash
# Starts a 3-node Apache Iggy cluster for the integration tests on a docker
# bridge network with fixed addresses (the host must be able to reach
# 172.29.0.0/24). `cluster.sh down` removes it again.
set -euo pipefail
IMAGE=${IMAGE:-apache/iggy:0.9.0}
NET=iggy-cluster-test
for i in 0 1 2; do docker rm -fv iggy-$i >/dev/null 2>&1 || true; done
docker network rm "$NET" >/dev/null 2>&1 || true
[ "${1:-up}" = down ] && exit 0
docker network create --subnet 172.29.0.0/24 "$NET" >/dev/null
roster=()
for i in 0 1 2; do
  roster+=(-e IGGY_CLUSTER_NODES_${i}_NAME=iggy-$i -e IGGY_CLUSTER_NODES_${i}_IP=172.29.0.1$i
           -e IGGY_CLUSTER_NODES_${i}_REPLICA_ID=$i -e IGGY_CLUSTER_NODES_${i}_PORTS_TCP=8090
           -e IGGY_CLUSTER_NODES_${i}_PORTS_QUIC=8080 -e IGGY_CLUSTER_NODES_${i}_PORTS_HTTP=3000
           -e IGGY_CLUSTER_NODES_${i}_PORTS_WEBSOCKET=8092 -e IGGY_CLUSTER_NODES_${i}_PORTS_TCP_REPLICA=9090)
done
for i in 0 1 2; do
  # Iggy needs io_uring, which the default seccomp profile blocks.
  docker run -d --name iggy-$i --hostname iggy-$i --network "$NET" --ip 172.29.0.1$i \
    --memory 2g --security-opt seccomp=unconfined \
    -e IGGY_ROOT_USERNAME=iggy -e IGGY_ROOT_PASSWORD=iggy \
    -e IGGY_TCP_ADDRESS=172.29.0.1$i:8090 -e IGGY_HTTP_ADDRESS=0.0.0.0:3000 \
    -e IGGY_QUIC_ENABLED=false -e IGGY_WEBSOCKET_ENABLED=false \
    -e IGGY_SHARDING_CPU_ALLOCATION=1 -e IGGY_MEMORY_POOL_SIZE=512MiB \
    -e IGGY_CLUSTER_ENABLED=true -e IGGY_CLUSTER_AUTH_ENABLED=true \
    -e IGGY_CLUSTER_AUTH_SHARED_SECRET=0123456789abcdef0123456789abcdef0123456789abcdef \
    -e IGGY_CLUSTER_NAME=it \
    "${roster[@]}" "$IMAGE" --replica-id $i >/dev/null
done
echo "IGGY_INTEGRATION_ADDRESSES=172.29.0.10:8090,172.29.0.11:8090,172.29.0.12:8090"
echo "IGGY_INTEGRATION_CONTAINERS=iggy-0,iggy-1,iggy-2"
