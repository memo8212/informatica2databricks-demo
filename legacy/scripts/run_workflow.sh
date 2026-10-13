#!/usr/bin/env bash
# ============================================================================
# Nightly orchestration for WF_DAILY_BANK_FEED (PowerCenter command line).
# The bank's scheduler (Control-M) calls this script at 01:00.
# ============================================================================
set -euo pipefail

DOMAIN="DOM_BANK"
SERVICE="IS_BANK_PROD"
FOLDER="BANK_ETL"
WF="WF_DAILY_BANK_FEED"

echo "[$(date)] starting $WF"
pmcmd startworkflow \
  -service "$SERVICE" \
  -domain "$DOMAIN" \
  -user "$INFA_USER" -password "$INFA_PWD" \
  -folder "$FOLDER" \
  -paramfile legacy/parameters/bank_params.prm \
  -wait "$WF"

echo "[$(date)] checking status"
pmcmd getworkflowdetails \
  -service "$SERVICE" -domain "$DOMAIN" \
  -user "$INFA_USER" -password "$INFA_PWD" \
  -folder "$FOLDER" "$WF"

# On failure, retrieve the session log for the ops team
pmcmd gettaskdetails \
  -service "$SERVICE" -domain "$DOMAIN" \
  -user "$INFA_USER" -password "$INFA_PWD" \
  -folder "$FOLDER" -workflow "$WF" -taskinstancepath "wklt_dept_ingest.s_dept_wealth" \
  || true

echo "[$(date)] done"
