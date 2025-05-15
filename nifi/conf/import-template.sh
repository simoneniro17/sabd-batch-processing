#!/bin/bash

echo "[WAIT] Aspetto che NiFi sia pronto..."

until curl -sk https://localhost:8443/nifi-api/system-diagnostics >/dev/null 2>&1; do
  echo " NiFi non è ancora pronto. Attendo 5s..."
  sleep 5
done
echo " NiFi è pronto!"
echo "==============================="

# === CONFIGURAZIONE ===
NIFI_API_URL="https://localhost:8443/nifi-api"
TEMPLATE_NAME="ing_and_preproc"
TEMPLATE_FILE="/opt/nifi/conf/${TEMPLATE_NAME}.xml"
TOOLKIT_PATH="/opt/nifi/nifi-toolkit-current/bin/cli.sh"

# Configurazione sicurezza (certificati)
TRUSTSTORE="/opt/nifi/nifi-current/conf/truststore.p12"
TRUSTSTORE_TYPE="PKCS12"
TRUSTSTORE_PASSWORD=$(grep '^nifi.security.truststorePasswd=' /opt/nifi/nifi-current/conf/nifi.properties | cut -d'=' -f2)

KEYSTORE="/opt/nifi/nifi-current/conf/keystore.p12"
KEYSTORE_TYPE="PKCS12"
KEYSTORE_PASSWORD=$(grep '^nifi.security.keystorePasswd=' /opt/nifi/nifi-current/conf/nifi.properties | cut -d'=' -f2)

USERNAME="admin"
PASSWORD="adminpassword"

TOKEN=$(curl -sk -X POST "$NIFI_API_URL/access/token" \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -d "username=$USERNAME&password=$PASSWORD")

if [[ -z "$TOKEN" ]]; then
  echo " Errore: impossibile ottenere token"
  exit 1
fi


# Upload template se non esiste
TEMPLATES_JSON=$(curl -sk -H "Authorization: Bearer $TOKEN" "$NIFI_API_URL/flow/templates")
TEMPLATE_ID=$(echo "$TEMPLATES_JSON" | jq -r ".templates[] | select(.template.name == \"$TEMPLATE_NAME\") | .template.id")

if [[ -z "$TEMPLATE_ID" ]]; then
  echo " Template non trovato. Caricamento..."
  $TOOLKIT_PATH nifi upload-template \
    -u "$NIFI_API_URL" \
    -btk "$TOKEN" \
    -ts "$TRUSTSTORE" -tst "$TRUSTSTORE_TYPE" -tsp "$TRUSTSTORE_PASSWORD" \
    -ks "$KEYSTORE" -kst "$KEYSTORE_TYPE" -ksp "$KEYSTORE_PASSWORD" \
    -pgid root -i "$TEMPLATE_FILE" -ot json

  TEMPLATES_JSON=$(curl -sk -H "Authorization: Bearer $TOKEN" "$NIFI_API_URL/flow/templates")
  TEMPLATE_ID=$(echo "$TEMPLATES_JSON" | jq -r ".templates[] | select(.template.name == \"$TEMPLATE_NAME\") | .template.id")
fi

if [[ -z "$TEMPLATE_ID" ]]; then
  echo " Errore: impossibile ottenere l'ID del template."
  exit 1
fi
echo "Template ID: $TEMPLATE_ID"

# # #evita istanze duplicate
echo " Controllo se esiste già un Process Group istanziato da '$TEMPLATE_NAME'..."
PG_CANDIDATES=$(curl -sk -H "Authorization: Bearer $TOKEN" "$NIFI_API_URL/process-groups/root/process-groups" | jq -r '.processGroups[].component.id')

for PG_ID in $PG_CANDIDATES; do
  PROC_NAMES=$(curl -sk -H "Authorization: Bearer $TOKEN" "$NIFI_API_URL/process-groups/$PG_ID/processors" | jq -r '.processors[].component.name')
  if echo "$PROC_NAMES" | grep -q "ListenHTTP"; then
    echo "  Process Group già presente (contiene 'ListenHTTP'). Istanza saltata."
    exit 0
  fi
done 

# Istanzia template
INSTANCE_RESPONSE=$(curl -sk -H "Authorization: Bearer $TOKEN" -X POST "$NIFI_API_URL/process-groups/root/template-instance" \
  -H "Content-Type: application/json" \
  -d "{
    \"templateId\": \"$TEMPLATE_ID\",
    \"originX\": 300.0,
    \"originY\": 200.0
}")

NEW_PG_ID=$(echo "$INSTANCE_RESPONSE" | jq -r '.flow.processGroups[0].component.id')

if [[ -z "$NEW_PG_ID\" || \"$NEW_PG_ID\" == \"null" ]]; then
  echo " Errore: Process Group non istanziato correttamente!"
  exit 1
fi

# Recupera tutti i processori nel PG
PROCESSORS_JSON=$(curl -sk -H "Authorization: Bearer $TOKEN" "$NIFI_API_URL/process-groups/$NEW_PG_ID/processors")

# Estrai gli ID dei QueryRecord
RECORD_PROCESSOR_IDS=$(echo "$PROCESSORS_JSON" | jq -r '.processors[] | select(.component.type | test("QueryRecord|ConvertRecord|MergeRecord")) | .component.id')

# Colleziona gli ID dei controller service usati da questi processori
USED_CONTROLLERS=()

for PROC_ID in $RECORD_PROCESSOR_IDS; do
  PROC_CONFIG=$(curl -sk -H "Authorization: Bearer $TOKEN" "$NIFI_API_URL/processors/$PROC_ID" | jq -r '.component.config.properties')

  READER_ID=$(echo "$PROC_CONFIG" | jq -r '."record-reader" // empty')
  WRITER_ID=$(echo "$PROC_CONFIG" | jq -r '."record-writer" // empty')

  if [[ -n "$READER_ID" ]]; then USED_CONTROLLERS+=("$READER_ID"); fi
  if [[ -n "$WRITER_ID" ]]; then USED_CONTROLLERS+=("$WRITER_ID"); fi
done

# Rimuovi duplicati
UNIQUE_CONTROLLERS=($(printf "%s\n" "${USED_CONTROLLERS[@]}" | sort -u))

# Abilita i controller usati
for CONTROLLER_ID in "${UNIQUE_CONTROLLERS[@]}"; do
  curl -sk -X PUT "$NIFI_API_URL/controller-services/$CONTROLLER_ID/run-status" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d "{
      \"revision\": {
        \"version\": 0
      },
      \"state\": \"ENABLED\"
    }"
done

# Attendi che siano abilitati
for CONTROLLER_ID in "${UNIQUE_CONTROLLERS[@]}"; do
  while true; do
    STATUS=$(curl -sk -H "Authorization: Bearer $TOKEN" "$NIFI_API_URL/controller-services/$CONTROLLER_ID" | jq -r '.component.state')
    if [[ "$STATUS" == "ENABLED" ]]; then
      break
    else
      sleep 2
    fi
  done
done

# === ABILITA CONTROLLER SERVICE ===

CONTROLLERS_JSON=$(curl -sk -H "Authorization: Bearer $TOKEN" "$NIFI_API_URL/process-groups/$NEW_PG_ID/controller-services")
CONTROLLER_IDS=$(echo "$CONTROLLERS_JSON" | jq -r '.controllerServices[].component.id')

for CONTROLLER_ID in $CONTROLLER_IDS; do
  curl -sk -X PUT "$NIFI_API_URL/controller-services/$CONTROLLER_ID/run-status" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d "{
      \"revision\": {
        \"version\": 0
      },
      \"state\": \"ENABLED\"
    }"
done

# Attende che siano abilitati
for CONTROLLER_ID in $CONTROLLER_IDS; do
  while true; do
    STATUS=$(curl -sk -H "Authorization: Bearer $TOKEN" "$NIFI_API_URL/controller-services/$CONTROLLER_ID" | jq -r '.component.state')
    if [[ "$STATUS" == "ENABLED" ]]; then
      break
    else
      sleep 2
    fi
  done
done


# Recupera i processori nel PG
PROCESSORS_JSON=$(curl -sk -H "Authorization: Bearer $TOKEN" "$NIFI_API_URL/process-groups/$NEW_PG_ID/processors")

# Estrai ID dei processori che hanno nome o tipo "QueryRecord"
RECORD_PROCESSOR_IDS=$(echo "$PROCESSORS_JSON" | jq -r '.processors[] | select(.component.type | test("QueryRecord|ConvertRecord|MergeRecord")) | .component.id')

if [[ -z "$RECORD_PROCESSOR_IDS" ]]; then
  echo " Nessun processore 'QueryRecord' trovato nel template."
else
  for PROCESSOR_ID in $RECORD_PROCESSOR_IDS; do
    RESPONSE=$(curl -sk -X PUT "$NIFI_API_URL/processors/$PROCESSOR_ID/run-status" \
      -H "Authorization: Bearer $TOKEN" \
      -H "Content-Type: application/json" \
      -d "{
        \"revision\": {
          \"version\": 0
        },
        \"state\": \"RUNNING\"
      }")

    STATUS=$(echo "$RESPONSE" | jq -r '.component.state // .message // .error')
  done
fi

# Avvia i QueryRecord
for PROCESSOR_ID in $RECORD_PROCESSOR_IDS; do
  RESPONSE=$(curl -sk -X PUT "$NIFI_API_URL/processors/$PROCESSOR_ID/run-status" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d "{
      \"revision\": {
        \"version\": 0
      },
      \"state\": \"RUNNING\"
    }")

  STATUS=$(echo "$RESPONSE" | jq -r '.component.state // .message // .error')
done

# Raccogli gli ID già avviati (QueryRecord)
SKIP_IDS=($RECORD_PROCESSOR_IDS)

# Recupera di nuovo tutti i processori (per sicurezza)
ALL_PROCESSORS_JSON=$(curl -sk -H "Authorization: Bearer $TOKEN" "$NIFI_API_URL/process-groups/$NEW_PG_ID/processors")

for PROCESSOR_ID in $(echo "$ALL_PROCESSORS_JSON" | jq -r '.processors[] | select(.component.state != "RUNNING") | .component.id'); do
  # Salta se è già stato gestito (QueryRecord)
  if [[ " ${SKIP_IDS[@]} " =~ " ${PROCESSOR_ID} " ]]; then
    continue
  fi

  RESPONSE=$(curl -sk -X PUT "$NIFI_API_URL/processors/$PROCESSOR_ID/run-status" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d "{
      \"revision\": {
        \"version\": 0
      },
      \"state\": \"RUNNING\"
    }")

  STATUS=$(echo "$RESPONSE" | jq -r '.component.state // .message // .error')
done
  echo "Caricamento template completato!"
  echo "==============================="


