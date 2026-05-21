#!/usr/bin/env bash
set -euo pipefail

STORE="${STORE_PATH:-store}"

if [ ! -d "$STORE" ]; then
    echo "Store directory '$STORE' not found — nothing to do."
    exit 0
fi

echo "Removing matrix-sdk store files from '$STORE' (email-bot.sqlite3 is preserved)..."

rm -f \
    "$STORE"/matrix-sdk-crypto.sqlite3 \
    "$STORE"/matrix-sdk-crypto.sqlite3-shm \
    "$STORE"/matrix-sdk-crypto.sqlite3-wal \
    "$STORE"/matrix-sdk-state.sqlite3 \
    "$STORE"/matrix-sdk-state.sqlite3-shm \
    "$STORE"/matrix-sdk-state.sqlite3-wal \
    "$STORE"/matrix-sdk-event-cache.sqlite3 \
    "$STORE"/matrix-sdk-event-cache.sqlite3-shm \
    "$STORE"/matrix-sdk-event-cache.sqlite3-wal \
    "$STORE"/matrix-sdk-media.sqlite3 \
    "$STORE"/matrix-sdk-media.sqlite3-shm \
    "$STORE"/matrix-sdk-media.sqlite3-wal

echo "Done. Restart the bot — it will re-initialize E2EE and re-join rooms."
