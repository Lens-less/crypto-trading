# Goal Worker Handoffs

Each worker owns one issue and updates `issue-<id>-handoff.md` before yielding.
A continuation worker must read the board, this README, the issue handoff, and
the referenced source files. It must not depend on an old conversation.

Every handoff must contain:

- issue id and title
- current status
- source documents by path
- repository path
- files changed or inspected
- decisions made
- commands run and exact results
- acceptance-criteria status
- blockers and remaining risks
- the exact next prompt for a continuation session

If a handoff is missing, empty, contradictory, or stale relative to the
worktree, stop that continuation, mark the issue blocked, and rebuild the
handoff from read-only repository evidence before editing code.
