#!/usr/bin/env bash

set -euo pipefail

if (( $# == 0 )); then
    printf 'usage: %s JOURNAL_JSONL [JOURNAL_JSONL ...]\n' "${0##*/}" >&2
    exit 2
fi

jq -s '
    [
      .[]
      | select(type == "object")
      | select(._SYSTEMD_INVOCATION_ID | type == "string" and length > 0)
      | {
          unit: (._SYSTEMD_UNIT // "unknown"),
          invocation_id: ._SYSTEMD_INVOCATION_ID,
          pid: (._PID // null),
          realtime_timestamp: (.__REALTIME_TIMESTAMP // null),
          cursor: (.__CURSOR // null)
        }
    ]
    | sort_by(.unit, .invocation_id, .realtime_timestamp)
    | group_by([.unit, .invocation_id])
    | {
        schema_version: 1,
        invocations: map({
          unit: .[0].unit,
          invocation_id: .[0].invocation_id,
          pids: ([.[].pid | select(. != null)] | unique),
          first_realtime_timestamp: .[0].realtime_timestamp,
          last_realtime_timestamp: .[-1].realtime_timestamp,
          first_cursor: .[0].cursor,
          last_cursor: .[-1].cursor
        })
      }
' "$@"
