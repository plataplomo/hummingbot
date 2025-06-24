---
description:
globs:
alwaysApply: true
---
# Cursor Rule: Full File Context Requirement

## Rule Name: full_file_context

---
**Description:**
Mandates that the AI assistant must always read the entire file before making any suggestion, edit, summary, or refactor, unless the user explicitly instructs otherwise. This rule is designed to ensure that all code analysis and modifications are based on complete and accurate file context, minimizing the risk of errors and omissions.

---

## Explicit Prompting (for User)
You can instruct the AI to always read the entire file before making any edit or analysis. For example:

- "Before making any change, read the entire file."
- "Always read the full file before summarizing, editing, or refactoring."
- "Do not rely on partial context—fetch the whole file every time."

---

## Enforcement (for AI Assistant)
- For all future requests, **always read the entire file before making any suggestion or edit, unless the user explicitly says otherwise.**
- Always read the full file before summarizing, editing, or refactoring.
- Do not rely on partial context—fetch the whole file every time in this session.
- If the user disables or overrides this rule, revert to standard context-fetching behavior.

---

## Rationale
This rule ensures that all code analysis, summaries, and edits are based on complete and accurate file context, reducing the risk of errors and improving the quality and safety of assistance.
