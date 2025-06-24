---
description:
globs:
alwaysApply: true
---
# Rule: Prohibition of AI-Initiated Git Usage

## Rule Name: ai_git_prohibition

### Description
AI assistants are **strictly prohibited** from executing, suggesting, or automating any `git` commands or direct version control operations (including but not limited to: `git add`, `git commit`, `git push`, `git pull`, `git checkout`, `git reset`, or any destructive or state-altering VCS actions).

### Enforcement
- **AI must never:**
  - Run, propose, or automate any `git` or version control command.
  - Suggest shell commands or scripts that invoke `git` or manipulate `.git` directories.
  - Modify, delete, or create files in `.git` or any VCS metadata.
- **All version control actions must be performed manually by the user.**
- **If a user requests a `git` operation,** the AI must:
  - Politely refuse, referencing this rule and explaining the rationale.
  - Offer to provide a summary of changes, a commit message template, or a diff if helpful, but never execute or script the VCS action itself.

### Rationale
- Ensures all version control actions are auditable, intentional, and under direct user control.
- Prevents accidental or malicious repository corruption, data loss, or unauthorized history rewriting.
- Maintains a clear separation of responsibilities between AI assistance and user-driven source control.

### Example
- **Prohibited:**
  - `git add . && git commit -m "fix: update imports" && git push`
  - `rm -rf .git`
- **Allowed:**
  - "Here is a suggested commit message for your changes: ..."
  - "Here is a summary of the files modified in this session: ..."

### Workflow
- If a workflow or script would require a `git` operation, the AI must halt and request explicit user action.
- All code, configuration, and documentation changes must be reviewed and committed by the user.
