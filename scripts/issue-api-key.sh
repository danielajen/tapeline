#!/usr/bin/env bash
#
# Issues an API key.
#
#   ./scripts/issue-api-key.sh "acme-research" [capacity] [refill-per-second]
#
# The secret is printed once and never stored anywhere this script can read
# it back. That is the point: a secret that can be looked up later is a
# secret that will be looked up later, by someone who should not.

set -euo pipefail

OWNER="${1:-}"
CAPACITY="${2:-100}"
REFILL="${3:-10.0}"

PGURL="${TAPELINE_POSTGRES_URL:-postgres://tapeline:tapeline@localhost:5432/tapeline}"

if [[ -z "${OWNER}" ]]; then
  echo "usage: $0 <owner> [capacity] [refill-per-second]" >&2
  exit 2
fi

command -v psql >/dev/null || { echo "psql is required" >&2; exit 1; }
command -v openssl >/dev/null || { echo "openssl is required" >&2; exit 1; }

# 32 bytes of CSPRNG output. The key id is prefixed so it is recognisable in
# a log or a support ticket without being confused for the secret.
KEY_ID="tk_$(openssl rand -hex 8)"
SECRET="$(openssl rand -base64 32)"

psql "${PGURL}" -v ON_ERROR_STOP=1 -q <<SQL
INSERT INTO api_keys (key_id, secret, owner, rate_limit_capacity, rate_limit_refill_per_second)
VALUES ('${KEY_ID}', '${SECRET}', '${OWNER}', ${CAPACITY}, ${REFILL});
SQL

cat <<OUT

  API key issued for ${OWNER}

    Key id  ${KEY_ID}
    Secret  ${SECRET}

  Store the secret now. It is not recoverable — the table holds it because
  HMAC requires the server to hold the same value the client signs with, but
  nothing in this tooling will print it again.

  Signing (see docs/API.md):

    canonical = METHOD \\n PATH \\n TIMESTAMP \\n NONCE \\n sha256(body)
    signature = base64(hmac_sha256(secret, canonical))

  Revoke with:

    psql "\$TAPELINE_POSTGRES_URL" -c \\
      "UPDATE api_keys SET enabled = false WHERE key_id = '${KEY_ID}';"

  Revocation takes effect within 30 seconds, the ApiKeyRepository cache TTL.

OUT
