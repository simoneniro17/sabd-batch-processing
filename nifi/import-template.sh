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
TEMPLATE_NAME="ingestion_and_preprocessing"
TEMPLATE_FILE="/opt/nifi/conf/${TEMPLATE_NAME}.xml"
TOOLKIT_PATH="/opt/nifi/nifi-toolkit-current/bin/cli.sh"

# Configurazione sicurezza (certificati)
TRUSTSTORE="/opt/nifi/nifi-current/conf/truststore.p12"
TRUSTSTORE_TYPE="PKCS12"
TRUSTSTORE_PASSWORD=$(grep '^nifi.security.truststorePasswd=' /opt/nifi/nifi-current/conf/nifi.properties | cut -d'=' -f2)

KEYSTORE="/opt/nifi/nifi-current/conf/keystore.p12"
KEYSTORE_TYPE="PKCS12"
KEYSTORE_PASSWORD=$(grep '^nifi.security.keystorePasswd=' /opt/nifi/nifi-current/conf/nifi.properties | cut -d'=' -f2)

# Credenziali NiFi
USERNAME="admin"
PASSWORD="adminpassword"

# genera token  per l'utente
echo "🔐 Generazione token..."
TOKEN=$(curl -sk -X POST "$NIFI_API_URL/access/token" \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -d "username=$USERNAME&password=$PASSWORD")

if [[ -z "$TOKEN" ]]; then
  echo " Errore: impossibile ottenere token"
  exit 1
fi

echo " Token ottenuto: ${TOKEN:0:20}..."

echo "Upload nuovo template..."

#  Verifica se il template è già presente in NiFi
echo "🔍 Verifico se il template '$TEMPLATE_NAME' è già presente..."
TEMPLATES_JSON=$(curl -sk -H "Authorization: Bearer $TOKEN" "$NIFI_API_URL/flow/templates")
TEMPLATE_ID=$(echo "$TEMPLATES_JSON" | jq -r ".templates[] | select(.template.name == \"$TEMPLATE_NAME\") | .template.id")

#  Se non esiste, lo carica via toolkit
if [[ -z "$TEMPLATE_ID" ]]; then
  echo "Template non trovato. Caricamento..."
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

  # Dopo l'upload, lo cerca di nuovo per ottenerne l'ID
  TEMPLATES_JSON=$(curl -sk -H "Authorization: Bearer $TOKEN" "$NIFI_API_URL/flow/templates")
  TEMPLATE_ID=$(echo "$TEMPLATES_JSON" | jq -r ".templates[] | select(.template.name == \"$TEMPLATE_NAME\") | .template.id")
fi
# Se ancora non lo trova, esce con errore
if [[ -z "$TEMPLATE_ID" ]]; then
  echo " Errore: impossibile ottenere l'ID del template."
  exit 1
fi
echo "Template ID: $TEMPLATE_ID"

#evita istanze duplicate
echo "🔎 Controllo se esiste già un Process Group istanziato da '$TEMPLATE_NAME'..."
PG_CANDIDATES=$(curl -sk -H "Authorization: Bearer $TOKEN" "$NIFI_API_URL/process-groups/root/process-groups" | jq -r '.processGroups[].component.id')

for PG_ID in $PG_CANDIDATES; do
  PROC_NAMES=$(curl -sk -H "Authorization: Bearer $TOKEN" "$NIFI_API_URL/process-groups/$PG_ID/processors" | jq -r '.processors[].component.name')
  if echo "$PROC_NAMES" | grep -q "ListenHTTP"; then
    echo "⚠️  Process Group già presente (contiene 'ListenHTTP'). Istanza saltata."
    exit 0
  fi
done 

# Istanzia il template nel canvas alla posizione (300,100)
echo "Istanzio template nel canvas..."
INSTANCE_RESPONSE=$(curl -sk -H "Authorization: Bearer $TOKEN" -X POST "$NIFI_API_URL/process-groups/root/template-instance" \
  -H "Content-Type: application/json" \
  -d "{
    \"templateId\": \"$TEMPLATE_ID\",
    \"originX\": 300.0,
    \"originY\": 100.0
}")


NEW_PG_ID=$(echo "$INSTANCE_RESPONSE" | jq -r '.flow.processGroups[0].component.id')

if [[ -z "$NEW_PG_ID" || "$NEW_PG_ID" == "null" ]]; then
  echo " Errore: Process Group non istanziato correttamente!"
  echo "$INSTANCE_RESPONSE"
  exit 1
fi
echo " Process Group istanziato con ID: $NEW_PG_ID"


#  Recupera tutti i processori presenti nel nuovo Process Group
echo " Recupero processori nel Process Group..."
PROCESSORS=$(curl -sk -H "Authorization: Bearer $TOKEN" "$NIFI_API_URL/process-groups/$NEW_PG_ID/processors")

echo " Avvio singolo di ogni processore nel Process Group..."
for PROCESSOR_ID in $(echo "$PROCESSORS" | jq -r '.processors[] | select(.component.state != "RUNNING") | .component.id'); do
  RESPONSE=$(curl -sk -X PUT "$NIFI_API_URL/processors/$PROCESSOR_ID/run-status" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d "{
      \"revision\": {
        \"version\": 0
      },
      \"state\": \"RUNNING\"
    }")
  echo "Avvio processore $PROCESSOR_ID "
done

echo " Process Group avviato!"