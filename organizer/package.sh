#!/usr/bin/env bash
# Empaqueta el kit para los equipos: todo lo versionado MENOS organizer/, CONTEXT.md y el resumen ejecutivo (menciona a la empresa).
#   organizer/package.sh   →   dist/hackathon-inventario-kit.zip
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p dist
rm -f dist/hackathon-inventario-kit.zip
git -c core.quotepath=off ls-files | grep -v -e '^organizer/' -e '^CONTEXT.md$' -e 'resumen ejecutivo' | zip -q dist/hackathon-inventario-kit.zip -@
echo "dist/hackathon-inventario-kit.zip ($(unzip -l dist/hackathon-inventario-kit.zip | tail -1 | awk '{print $2}') archivos)"
