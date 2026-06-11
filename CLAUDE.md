# Agent Instructions

> Universal instructions for AI coding assistants (Claude Code, Cursor, Codex, Gemini CLI, etc.)

## Project Context

**Tech Stack**: [Add your stack here]
**Architecture**: [Add your architecture here]

## Agent Guidelines

**When to delegate to a subagent:** You need a result, not a play-by-play. The exploratory work would clutter the main thread. The task benefits from a fresh perspective or custom system prompt (research, code reviews, domain-specific tasks).

**Avoid subagents for:** "Expert" personas that add no real capability. Sequential pipelines where each step depends on the previous step's discoveries. Test runners where you need full output for debugging.

**Decision rule:** Does the intermediate work matter? If no → delegate. If yes → keep in main thread.

## Running Ad-hoc SQL via curl

```bash
curl -s -X POST \
  "<databricks-host>>/api/2.0/sql/statements" \
  -H "Authorization: Bearer $(grep token ~/.databrickscfg | head -1 | cut -d= -f2 | tr -d ' ')" \
  -H "Content-Type: application/json" \
  -d '{"warehouse_id":"<warehouse-id>","statement":"SELECT ...","wait_timeout":"50s"}'
```