#!/bin/bash
# Post-commit hook: remind agent to update help text if testcase/script files changed
# Used by Claude Code's PostToolUse hook for Bash commands containing "git commit"

WATCHED_FILES="testcase/testcase.go script/script.go"
HELP_FILE="cmd/bqtest/main.go"

# Check files in the last commit
if ! committed=$(git show --name-only --pretty="" HEAD 2>/dev/null); then
  exit 0
fi

watched_changed=false
help_changed=false

for f in $WATCHED_FILES; do
  if echo "$committed" | grep -Fxq -- "$f"; then
    watched_changed=true
    break
  fi
done

if echo "$committed" | grep -Fxq -- "$HELP_FILE"; then
  help_changed=true
fi

if [ "$watched_changed" = true ] && [ "$help_changed" = false ]; then
  cat <<'HOOKJSON'
{"hookSpecificOutput":{"hookEventName":"PostToolUse","additionalContext":"⚠ YAMLフォーマットや生成SQLに影響する変更を検知しました。必要に応じて cmd/bqtest/main.go のヘルプテキストを更新し、追加コミットしてください。"}}
HOOKJSON
fi
