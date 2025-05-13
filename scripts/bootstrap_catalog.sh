#!/bin/bash
set -e
NESSIE_HOST=${NESSIE_HOST:-"nessie.lakehouse.svc.cluster.local"}
NESSIE_PORT=${NESSIE_PORT:-19120}

echo "Criando branch \"dev\" no Nessie..."
curl -X POST "http://${NESSIE_HOST}:${NESSIE_PORT}/api/v1/trees/dev" \
     -H 'Content-Type: application/json' \
     -d '{"type":"BRANCH","hash":""}'