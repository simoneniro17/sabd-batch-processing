#!/bin/bash

# ERROR: Error executing command 'upload-template' : truststore, truststoreType, and truststorePasswd are required when using an https url

# Path al template
TEMPLATE_FILE="/opt/nifi/conf/pippox.xml"
NIFI_API_URL="https://localhost:8443/nifi-api"
TOOLKIT_PATH="/opt/nifi/nifi-toolkit-current/bin/cli.sh"

USERNAME="admin"
PASSWORD="adminpassword"

# Attendi che NiFi sia pronto (ignorando TLS self-signed)
echo "Aspettando che NiFi sia pronto su HTTPS..."
until curl -sk "$NIFI_API_URL" > /dev/null; do
  sleep 5
done

echo "NiFi è pronto. Importazione del template..."

# Importa template (usando le opzioni corrette per il login)
$TOOLKIT_PATH nifi upload-template \
  -bau "$USERNAME" \
  -bap "$PASSWORD" \
  -i "$TEMPLATE_FILE"

echo "Template caricato. Recupero ID template..."

# Ottieni ID del template
TEMPLATE_ID=$(curl -sk -u "$USERNAME:$PASSWORD" "$NIFI_API_URL/templates" \
  | grep -oP '(?<="id":")[^"]+')

# Instanzia il template nel canvas
curl -sk -u "$USERNAME:$PASSWORD" -X POST "$NIFI_API_URL/process-groups/root/template-instance" \
  -H "Content-Type: application/json" \
  -d "{
    \"templateId\": \"$TEMPLATE_ID\",
    \"originX\": 0.0,
    \"originY\": 0.0
}"

echo "Template importato e istanziato!"
