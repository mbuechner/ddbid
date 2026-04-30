#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/h2-to-postgres.sh [h2-db-path] [postgres-dump.sql]

Examples:
  scripts/h2-to-postgres.sh
  scripts/h2-to-postgres.sh data/ddbid_H2_DO_NOT_DELETE_ITS_IMPORTANT.db ddbid-postgres.sql
  scripts/h2-to-postgres.sh data/ddbid_H2_DO_NOT_DELETE_ITS_IMPORTANT.db.mv.db ddbid-postgres.sql

Environment:
  H2_USER       H2 user name. Default: <empty>
  H2_PASSWORD   H2 password. Default: <empty>
  H2_VERSION    H2 version to use. Default: 2.1.214
  H2_JAR        Optional path to h2-*.jar. Overrides H2_VERSION.
  TMPDIR        Temp directory. Default: .

Notes:
  - The input database is opened read-only through H2's Script tool.
  - If you pass a *.mv.db file, the suffix is stripped for the JDBC URL.
  - Indexes are created at the end, after the data load.
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
DEFAULT_DB="data/ddbid_H2_DO_NOT_DELETE_ITS_IMPORTANT.db"

H2_DB="${1:-${DDBID_DATABASE:-$DEFAULT_DB}}"
OUT_SQL="${2:-ddbid-postgres.sql}"

H2_USER="${H2_USER:-}"
H2_PASSWORD="${H2_PASSWORD:-}"
H2_VERSION="${H2_VERSION:-2.1.214}"
TMPDIR="${TMPDIR:-.}"

cd "$ROOT_DIR"

to_java_path() {
  if command -v cygpath >/dev/null 2>&1; then
    cygpath -m "$1"
  else
    printf '%s\n' "$1"
  fi
}

absolute_existing_dir_path() {
  local input="$1"
  local dir
  local base

  dir="$(dirname "$input")"
  base="$(basename "$input")"

  if [[ ! -d "$dir" ]]; then
    echo "Directory does not exist: $dir" >&2
    exit 1
  fi

  dir="$(cd "$dir" && pwd -P)"
  printf '%s/%s\n' "$dir" "$base"
}

absolute_output_path() {
  local input="$1"
  local dir
  local base

  dir="$(dirname "$input")"
  base="$(basename "$input")"

  mkdir -p "$dir"
  dir="$(cd "$dir" && pwd -P)"

  printf '%s/%s\n' "$dir" "$base"
}

absolute_temp_dir() {
  local dir="$1"

  mkdir -p "$dir"
  dir="$(cd "$dir" && pwd -P)"

  printf '%s\n' "$dir"
}

download_h2_jar() {
  local version="$1"

  if command -v mvn >/dev/null 2>&1; then
    mvn -q org.apache.maven.plugins:maven-dependency-plugin:3.6.1:get \
      -Dartifact="com.h2database:h2:$version"
    return
  fi

  if [[ -x "./mvnw" ]]; then
    ./mvnw -q org.apache.maven.plugins:maven-dependency-plugin:3.6.1:get \
      -Dartifact="com.h2database:h2:$version"
    return
  fi

  echo "Neither mvn nor ./mvnw found. Cannot download H2 $version." >&2
  return 1
}

find_h2_jar() {
  if [[ -n "${H2_JAR:-}" ]]; then
    printf '%s\n' "$H2_JAR"
    return
  fi

  local preferred="$HOME/.m2/repository/com/h2database/h2/$H2_VERSION/h2-$H2_VERSION.jar"

  if [[ -f "$preferred" ]]; then
    printf '%s\n' "$preferred"
    return
  fi

  echo "Preferred H2 jar not found: $preferred" >&2
  echo "Trying to download H2 $H2_VERSION via Maven..." >&2

  download_h2_jar "$H2_VERSION" || true

  if [[ -f "$preferred" ]]; then
    printf '%s\n' "$preferred"
    return
  fi

  echo "Could not find or download H2 $H2_VERSION." >&2
  echo "Set H2_JAR=/path/to/h2-$H2_VERSION.jar and retry." >&2
  exit 1
}

validate_h2_jar() {
  local jar_path="$1"

  if [[ ! -f "$jar_path" ]]; then
    echo "H2 jar does not exist: $jar_path" >&2
    exit 1
  fi

  if ! command -v jar >/dev/null 2>&1; then
    echo "Command 'jar' not found. Please use a JDK, not only a JRE." >&2
    exit 1
  fi

  if ! jar tf "$jar_path" | grep -q '^org/h2/tools/Script.class$'; then
    echo "H2 jar does not contain org.h2.tools.Script: $jar_path" >&2
    echo "This is probably not a valid/full H2 database jar." >&2
    exit 1
  fi
}

resolve_h2_paths() {
  local db_abs="$1"
  local jdbc_base
  local physical_file

  case "$db_abs" in
    *.mv.db)
      jdbc_base="${db_abs%.mv.db}"
      physical_file="$db_abs"
      ;;
    *)
      jdbc_base="$db_abs"
      physical_file="${db_abs}.mv.db"
      ;;
  esac

  if [[ ! -f "$physical_file" ]]; then
    echo "H2 database file not found: $physical_file" >&2
    echo "Pass either the JDBC base path without .mv.db or the actual *.mv.db file." >&2
    exit 1
  fi

  printf '%s\n' "$jdbc_base"
}

H2_DB_ABS="$(absolute_existing_dir_path "$H2_DB")"
OUT_SQL_ABS="$(absolute_output_path "$OUT_SQL")"
TMPDIR_ABS="$(absolute_temp_dir "$TMPDIR")"

H2_JDBC_LOCAL_PATH="$(resolve_h2_paths "$H2_DB_ABS")"
H2_JDBC_PATH="$(to_java_path "$H2_JDBC_LOCAL_PATH")"

H2_JAR_LOCAL_PATH="$(find_h2_jar)"
validate_h2_jar "$H2_JAR_LOCAL_PATH"
H2_JAR_PATH="$(to_java_path "$H2_JAR_LOCAL_PATH")"

RAW_SQL="$(mktemp "$TMPDIR_ABS/ddbid-h2-raw.XXXXXX.sql")"
PERL_FILTER="$(mktemp "$TMPDIR_ABS/ddbid-h2-filter.XXXXXX.pl")"

trap 'rm -f "$RAW_SQL" "$PERL_FILTER"' EXIT

echo "Using H2 jar: $H2_JAR_LOCAL_PATH" >&2
echo "Using H2 version: $H2_VERSION" >&2
echo "Using temp dir: $TMPDIR_ABS" >&2
echo "Dumping H2 database: $H2_JDBC_PATH" >&2

java -cp "$H2_JAR_PATH" org.h2.tools.Script \
  -url "jdbc:h2:$H2_JDBC_PATH;IFEXISTS=TRUE;ACCESS_MODE_DATA=r" \
  -user "$H2_USER" \
  -password "$H2_PASSWORD" \
  -script "$RAW_SQL" \
  -options SIMPLE

cat > "$OUT_SQL_ABS" <<'EOF'
-- DDBid H2 -> PostgreSQL migration dump.
-- Generated by scripts/h2-to-postgres.sh.
-- Run with:
--   psql --set ON_ERROR_STOP=on --single-transaction --file ddbid-postgres.sql postgresql://user:password@host:5432/ddbid

SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;

BEGIN;

EOF

cat > "$PERL_FILTER" <<'PERL'
BEGIN {
  $skip = 0;
}

s/\r\n/\n/g;

if ($skip) {
  if (/;\s*$/) {
    $skip = 0;
  }
  next;
}

# Drop H2-specific session noise and existing indexes.
if (/^\s*(SET|CREATE USER|ALTER USER|GRANT|CREATE SCHEMA|COMMENT ON)\b/i) {
  if (!/;\s*$/) {
    $skip = 1;
  }
  next;
}

if (/^\s*CREATE\s+(?:UNIQUE\s+)?INDEX\b/i) {
  if (!/;\s*$/) {
    $skip = 1;
  }
  next;
}

# H2 schema qualification for the default schema.
s/"PUBLIC"\."([^"]+)"/"$1"/g;
s/PUBLIC\.([A-Za-z_][A-Za-z0-9_]*)/$1/g;

# Table syntax.
s/CREATE\s+(?:CACHED|MEMORY)\s+TABLE/CREATE TABLE/gi;

# Identity / auto increment syntax.
s/\b(INT|INTEGER|BIGINT)\s+GENERATED\s+BY\s+DEFAULT\s+AS\s+IDENTITY\s*\([^)]*\)/$1 GENERATED BY DEFAULT AS IDENTITY/gi;
s/\b(INT|INTEGER|BIGINT)\s+AUTO_INCREMENT\b/$1 GENERATED BY DEFAULT AS IDENTITY/gi;

# H2 sometimes emits NULLS FIRST/LAST in DDL.
s/\s+NULLS\s+(?:FIRST|LAST)//gi;

# H2 emits SELECTIVITY statistics in column definitions.
s/\s+SELECTIVITY\s+\d+//gi;

# PostgreSQL has no H2 SET REFERENTIAL_INTEGRITY statement.
next if /^\s*SET\s+REFERENTIAL_INTEGRITY\s+(?:TRUE|FALSE)\s*;\s*$/i;

print;
PERL

perl -n "$PERL_FILTER" "$RAW_SQL" >> "$OUT_SQL_ABS"

cat >> "$OUT_SQL_ABS" <<'EOF'

-- DDBid indexes for PostgreSQL.
-- Created after the data load for faster imports.
CREATE INDEX IF NOT EXISTS "item_timestamp" ON "item"("timestamp");
CREATE INDEX IF NOT EXISTS "item_status" ON "item"("status");
CREATE INDEX IF NOT EXISTS "item_status_timestamp" ON "item"("status", "timestamp");
CREATE INDEX IF NOT EXISTS "item_status_provider" ON "item"("status", "provider_id");
CREATE INDEX IF NOT EXISTS "item_status_sector" ON "item"("status", "sector_fct");
CREATE INDEX IF NOT EXISTS "item_filter_id" ON "item"("status", "timestamp", "id");
CREATE INDEX IF NOT EXISTS "item_filter_provider_item" ON "item"("status", "timestamp", "provider_item_id");
CREATE INDEX IF NOT EXISTS "item_filter_dataset" ON "item"("status", "timestamp", "dataset_id");
CREATE INDEX IF NOT EXISTS "item_filter_provider" ON "item"("status", "timestamp", "provider_id");
CREATE INDEX IF NOT EXISTS "item_filter_sector" ON "item"("status", "timestamp", "sector_fct");
CREATE INDEX IF NOT EXISTS "item_filter_supplier" ON "item"("status", "timestamp", "supplier_id");

CREATE INDEX IF NOT EXISTS "person_timestamp" ON "person"("timestamp");
CREATE INDEX IF NOT EXISTS "person_status" ON "person"("status");
CREATE INDEX IF NOT EXISTS "person_status_timestamp" ON "person"("status", "timestamp");
CREATE INDEX IF NOT EXISTS "person_filter_id" ON "person"("status", "timestamp", "id");
CREATE INDEX IF NOT EXISTS "person_filter_variant" ON "person"("status", "timestamp", "variant_id");
CREATE INDEX IF NOT EXISTS "person_filter_type" ON "person"("status", "timestamp", "type");

CREATE INDEX IF NOT EXISTS "organization_timestamp" ON "organization"("timestamp");
CREATE INDEX IF NOT EXISTS "organization_status" ON "organization"("status");
CREATE INDEX IF NOT EXISTS "organization_status_timestamp" ON "organization"("status", "timestamp");
CREATE INDEX IF NOT EXISTS "organization_filter_id" ON "organization"("status", "timestamp", "id");
CREATE INDEX IF NOT EXISTS "organization_filter_variant" ON "organization"("status", "timestamp", "variant_id");
CREATE INDEX IF NOT EXISTS "organization_filter_type" ON "organization"("status", "timestamp", "type");

-- Trigram indexes accelerate ILIKE '%...%' column filters in PostgreSQL.
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE INDEX IF NOT EXISTS "item_trgm_id" ON "item" USING GIN ("id" gin_trgm_ops);
CREATE INDEX IF NOT EXISTS "item_trgm_provider_item" ON "item" USING GIN ("provider_item_id" gin_trgm_ops);
CREATE INDEX IF NOT EXISTS "item_trgm_dataset" ON "item" USING GIN ("dataset_id" gin_trgm_ops);
CREATE INDEX IF NOT EXISTS "item_trgm_label" ON "item" USING GIN ("label" gin_trgm_ops);
CREATE INDEX IF NOT EXISTS "item_trgm_provider" ON "item" USING GIN ("provider_id" gin_trgm_ops);
CREATE INDEX IF NOT EXISTS "item_trgm_supplier" ON "item" USING GIN ("supplier_id" gin_trgm_ops);
CREATE INDEX IF NOT EXISTS "person_trgm_id" ON "person" USING GIN ("id" gin_trgm_ops);
CREATE INDEX IF NOT EXISTS "person_trgm_variant" ON "person" USING GIN ("variant_id" gin_trgm_ops);
CREATE INDEX IF NOT EXISTS "person_trgm_name" ON "person" USING GIN ("preferredName" gin_trgm_ops);
CREATE INDEX IF NOT EXISTS "organization_trgm_id" ON "organization" USING GIN ("id" gin_trgm_ops);
CREATE INDEX IF NOT EXISTS "organization_trgm_variant" ON "organization" USING GIN ("variant_id" gin_trgm_ops);
CREATE INDEX IF NOT EXISTS "organization_trgm_name" ON "organization" USING GIN ("preferredName" gin_trgm_ops);

-- Helpful for large append-like timestamp histories in PostgreSQL.
CREATE INDEX IF NOT EXISTS "item_timestamp_brin" ON "item" USING BRIN("timestamp");
CREATE INDEX IF NOT EXISTS "person_timestamp_brin" ON "person" USING BRIN("timestamp");
CREATE INDEX IF NOT EXISTS "organization_timestamp_brin" ON "organization" USING BRIN("timestamp");

ANALYZE "item";
ANALYZE "person";
ANALYZE "organization";

COMMIT;
EOF

echo "Wrote PostgreSQL SQL dump: $OUT_SQL_ABS" >&2
