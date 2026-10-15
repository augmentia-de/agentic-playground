#!/usr/bin/env bash
#
# Write a per-module Surefire test summary to the GitHub Actions job summary.
# Test counts are reported from the build reports, never hard-coded in prose.
#
set -uo pipefail

summary="${GITHUB_STEP_SUMMARY:-/dev/stdout}"
reports="$(find . -path '*/surefire-reports/*.txt' -print 2>/dev/null)"

{
  echo "### Test results"
  echo
  if [ -n "$reports" ]; then
    echo '```'
    # shellcheck disable=SC2086
    grep -H "Tests run" $reports | sed 's|^\./||' | sort
    echo '```'
  else
    echo "_No Surefire reports found._"
  fi
} >> "$summary"
