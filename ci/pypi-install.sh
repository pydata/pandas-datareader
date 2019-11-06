#!/usr/bin/env bash

pip install pip --upgrade
pip install numpy=="$NUMPY" lxml
pip install -r requirements-dev.txt

if [[ "$PANDAS" == "MASTER" ]]; then
  PRE_WHEELS="https://7933911d6844c6c53a7d-47bd50c35cd79bd838daf386af554a83.ssl.cf2.rackcdn.com"
  pip install --pre --upgrade --timeout=60 -f "$PRE_WHEELS" pandas
else
  pip install pandas=="$PANDAS"
fi

if [[ "$DOCBUILD" ]]; then
  pip install doctr
  pip install -r docs/requirements.txt
fi
