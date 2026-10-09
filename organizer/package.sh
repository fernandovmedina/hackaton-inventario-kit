#!/usr/bin/env bash
# Empaqueta el kit para los equipos: todo lo versionado MENOS organizer/ y CONTEXT.md.
#   organizer/package.sh   →   dist/hackathon-inventario-kit.zip
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p dist
rm -f dist/hackathon-inventario-kit.zip
git ls-files | grep -v -e '^organizer/' -e '^CONTEXT.md$' | zip -q dist/hackathon-inventario-kit.zip -@
echo "dist/hackathon-inventario-kit.zip ($(unzip -l dist/hackathon-inventario-kit.zip | tail -1 | awk '{print $2}') archivos)"
