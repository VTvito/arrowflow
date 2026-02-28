# {{SERVICE_NAME}} — New Service Template
#
# This template provides a ready-to-customize scaffold for adding
# a new transform microservice to the ETL platform.
#
# Quick start:
#   1. Copy this folder: cp -r templates/new_service services/my-service
#   2. Find-replace all {{PLACEHOLDERS}} (see list below)
#   3. Follow the checklist in this README
#
# ══════════════════════════════════════════════════════════════
# PLACEHOLDERS TO REPLACE
# ══════════════════════════════════════════════════════════════
#
#   {{SERVICE_NAME}}      →  my-service              (e.g., normalize-service)
#   {{SERVICE_SLUG}}      →  my_service              (e.g., normalize — used in code/metrics)
#   {{SERVICE_PORT}}      →  5013                    (next available port)
#   {{ENDPOINT_NAME}}     →  my-endpoint             (e.g., normalize — the POST route)
#   {{LOGIC_MODULE}}      →  logic                   (e.g., normalize — the .py file name)
#   {{LOGIC_FUNCTION}}    →  process_data            (e.g., normalize_columns — the function name)
#
# ══════════════════════════════════════════════════════════════
# CHECKLIST (after creating the service)
# ══════════════════════════════════════════════════════════════
#
#   [ ] Replace all placeholders in all files
#   [ ] Implement business logic in app/logic.py
#   [ ] Register in preparator/services_config.json:
#         "{{SERVICE_SLUG}}": "http://{{SERVICE_NAME}}:{{SERVICE_PORT}}/{{ENDPOINT_NAME}}"
#   [ ] Register in schemas/service_registry.json (full metadata)
#   [ ] Add to docker-compose.yml (copy an existing service block)
#   [ ] Add scrape target to prometheus/prometheus.yml
#   [ ] Add a method to preparator/preparator_v4.py
#   [ ] Write unit test in tests/unit/test_{{LOGIC_MODULE}}.py
#   [ ] Write integration test in tests/integration/
#   [ ] Update .github/copilot-instructions.md service catalog
#
# For a detailed walkthrough, see docs/extending.md
