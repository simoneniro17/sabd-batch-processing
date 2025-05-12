#!/bin/bash

echo "[WAIT] Aspetto che NiFi sia pronto..."

until curl -sk https://localhost:8443/nifi-api/system-diagnostics >/dev/null 2>&1; do
  echo "⏳ NiFi non è ancora pronto. Attendo 5s..."
  sleep 5
done
echo " NiFi è pronto!"
echo "==============================="

# === CONFIGURAZIONE ===
NIFI_API_URL="https://localhost:8443/nifi-api"
TEMPLATE_NAME="pippox"
TEMPLATE_FILE="/opt/nifi/conf/${TEMPLATE_NAME}.xml"
TOOLKIT_PATH="/opt/nifi/nifi-toolkit-current/bin/cli.sh"

# Truststore (server)
TRUSTSTORE="/opt/nifi/nifi-current/conf/truststore.p12"
TRUSTSTORE_TYPE="PKCS12"
TRUSTSTORE_PASSWORD=$(grep '^nifi.security.truststorePasswd=' /opt/nifi/nifi-current/conf/nifi.properties | cut -d'=' -f2)

# Keystore (client)
KEYSTORE="/opt/nifi/nifi-current/conf/keystore.p12"
KEYSTORE_TYPE="PKCS12"
KEYSTORE_PASSWORD=$(grep '^nifi.security.keystorePasswd=' /opt/nifi/nifi-current/conf/nifi.properties | cut -d'=' -f2)

# Credenziali utente
USERNAME="admin"
PASSWORD="adminpassword"

# === TOKEN ===
echo "🔐 Generazione token..."
TOKEN=$(curl -sk -X POST "$NIFI_API_URL/access/token" \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -d "username=$USERNAME&password=$PASSWORD")

if [[ -z "$TOKEN" ]]; then
  echo " Errore: impossibile ottenere token"
  exit 1
fi

echo " Token ottenuto: ${TOKEN:0:20}..."

echo "🚀 Upload nuovo template..."

# === UPLOAD TEMPLATE ===
TEMPLATE_UPLOAD_OUTPUT=$($TOOLKIT_PATH nifi upload-template \
  -u "$NIFI_API_URL" \
  -btk "$TOKEN" \
  -ts "$TRUSTSTORE" \
  -tst "$TRUSTSTORE_TYPE" \
  -tsp "$TRUSTSTORE_PASSWORD" \
  -ks "$KEYSTORE" \
  -kst "$KEYSTORE_TYPE" \
  -ksp "$KEYSTORE_PASSWORD" \
  -pgid root \
  -i "$TEMPLATE_FILE" \
  -ot json 2>&1)

echo " Output upload-template:"
echo "$TEMPLATE_UPLOAD_OUTPUT"

# estrazione ID del template
TEMPLATE_ID=$(echo "$TEMPLATE_UPLOAD_OUTPUT" | grep -oP '(?<="templateId":")[^"]+')



#istanzio template
echo " Istanzio template nel canvas..."
curl -sk -H "Authorization: Bearer $TOKEN" -X POST "$NIFI_API_URL/process-groups/root/template-instance" \
  -H "Content-Type: application/json" \
  -d "{
    \"templateId\": \"$TEMPLATE_ID\",
    \"originX\": 0.0,
    \"originY\": 0.0
}"