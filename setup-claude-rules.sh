#!/bin/bash
# Script to consolidate rules for Claude Code

echo "Setting up Claude Code rules..."

# Create CLAUDE.md with all rules
cat > CLAUDE.md << 'EOF'
# CyberDeltaEngine Rules for Claude Code

This file contains project rules imported from .claude/rules/

## Quick Reference
- Always read this file at conversation start
- Follow all rules below unless explicitly overridden
- Rules are organized by category

---

EOF

# Append all rule files
for rule_file in .claude/rules/*.md; do
    echo "## $(basename $rule_file .md | tr '_' ' ' | sed 's/\b\(.\)/\u\1/g')" >> CLAUDE.md
    echo "" >> CLAUDE.md
    cat "$rule_file" >> CLAUDE.md
    echo -e "\n---\n" >> CLAUDE.md
done

echo "✅ Rules consolidated into CLAUDE.md"
echo "Claude Code will now see these rules automatically!"