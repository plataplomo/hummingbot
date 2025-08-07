# CyberDeltaEngine - Claude Code Instructions

## IMPORTANT: Read Project Rules First
Before starting any work, you MUST read ALL rule files from the `.claude/rules/` directory:

1. List all files in `.claude/rules/`
2. Read each `.md` file in that directory
3. Apply all rules found there for this entire session

The rules in `.claude/rules/` are the authoritative source for how to work on this project.

## Project Context
- This is a cryptocurrency trading engine (CyberDeltaEngine) that connects initially to Hyperliquid and Backpack exchanges for delta-neutral arbitrage strategies with future expansion to more strategies and exchanges

## Important code sources
- Understand and use @cyberdelta/symbols/ for symbols
- If in doubt: use @cyberdelta/enums/ or create new enum set
- Study @cyberdelta/apis/ to understand our API layer, don't create workarounds or new layers/protocols around API
- Study @cyberdelta/CODING_STANDARDS.md for business logic coding
- Store models in @cyberdelta/models/ with proper tree structure
- Store protocols in @cyberdelta/protocols/ with proper tree structure
- Store enums in @cyberdelta/enums/ with proper tree structure

## MCP tools
- use context7 mcp for most fresh tech documentation

## Tech Stack
- For retries use tenacity
- For json use orjson

## Type Checking and Linting
- Always run `mypy`, `ruff check`, and `pyright` before ending the session, fix errors
- All 3 type checkers must show 0 errors
- Never silence or ignore any errors (usage of type ignores, pragmas, noqa, or pyright ignores is always strictly forbidden)
- Only implement real fixes to resolve type checking and linting issues
- Remember: the errors are our chance to sniff deep flaws, improve security and architecture, not to make quick patches
- Don't fix errors by working around. Find the root cause and work with it
- Our linters are very strict for a very good reason, these are safety security measures for our codebase, that works with real money
- Every error not properly resolved is a possible future huge money loss: not acceptable

## Dependency and Architecture Guidelines
- Look at circular dependencies as code smells that show architecturally weak solutions
- Don't work around circular issues and dependencies
- Always find the root cause and find architectural better solutions
- TYPE_CHECKING is the absolute last resort to fix them, not the first

## Important Operational Guidelines
- Write only professional docstrings and comments, never be emotional creating them
- YAGNI («You aren't gonna need it»)
- Never overengineer unless explicitly asked so
- Use modern Protocols (runtime checkable or normal), for abstractions, never ABC classes
- Minimise usage of `bool`, prefer enums even for binary true/false decisions

## Decimal
- Remember: `Decimal` for financial operations is the absolute critical demand

## Git Guidelines
- Git rules: you can only read from git unless given explicit permission to write or make other changes

## Type Safety and Model Guidelines
- This codebase only uses `Pydantic base classes` or `Pydantic dataclasses` for type safety
- `dict[str, Any]` is almost always a bad practice that will be rejected or will have to be refactored
- `Any` is almost always bad practice
- Using `object` to replace `Any` is a workaround we don't tolerate
- When in doubt: check @cyberdelta/models/ and @cyberdelta/protocols/ for already existing models and abstractions, don't rush to create duplications and inconsistencies
- getaddr, hasattr, setaddr in prod code @cyberdelta is almost always a bad idea: prefer type safe solutions

## Testing Guidelines
- This codebase uses exclusively pytest for testing
- Parametrization, fixtures, and other pytest good practices for testing environment are always good ideas
- Real endpoint integration and e2e tests have to adhere strictly to @tests/integration/TESTING_SECURITY_RULES.md
- Testing via private methods is strictly forbidden: this codebase adheres to principles of testing via exposed public behavior that makes sence from business logic perspective (creating synthetic wrapper around private method to expose it for testing is a very bad practice and is strictly forbidden)

## Security and Coding Best Practices
- Never do any fallbacks, unless explicitly asked so
- Don't code in ` | None` just because you think it looks as a cool fallback, there always have to be a good business logic reason for optionality
- Every fallback coded in is a possible security leak coded in: bad idea

## Good Practices
- Dependency injection
- Type safety
- KISS (Keep It Simple, Stupid)
- DRY (Don't Repeat Yourself)
- YAGNI (You Aren't Gonna Need It)
- DDD (Domain Driven Design)
- Clear boundaries

## Bad Practices
- Assuming instead of looking for a source
- Placing TODOs instead of finding correct source/abstraction/module that already allows implementing the logic
- Fallbacks
- Hardcoded values

## Code Organization and Refactoring Guidelines
- Maintain source files below 600 lines of code
- If business logic has grown, decompose into more modular structure, adhering to DDD, DRY and KISS
- Don't overengineer when decomposing
- Don't create additional logic when decomposing
- After a decomposition, make sure all the business logic was properly moved
- Clean up after yourself
