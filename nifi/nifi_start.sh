#!/bin/bash

docker exec -it nifi bash -c "
chmod +x /opt/nifi/scripts/import-template.sh && \
/opt/nifi/scripts/import-template.sh
"
