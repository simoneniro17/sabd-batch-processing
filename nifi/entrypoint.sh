#!/bin/bash

# Avvia lo script di importazione del template in background
chmod +x /opt/nifi/scripts/import-template.sh
/opt/nifi/scripts/import-template.sh &

# Avvia NiFi in foreground (così Docker non spegne il container)
exec /opt/nifi/nifi-current/bin/nifi.sh run 
