#!/bin/bash
# Block Claude from running git commit commands

INPUT=$(cat)
COMMAND=$(echo "$INPUT" | jq -r '.tool_input.command // empty')

if echo "$COMMAND" | grep -qE '^\s*git\s+commit'; then
  echo '{"decision":"block","reason":"Do not commit directly. Suggest a commit message instead."}'
  exit 0
fi

exit 0
