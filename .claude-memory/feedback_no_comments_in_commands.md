---
name: No comments in copyable commands
description: When giving the user shell commands to copy and run, do not include inline comments
type: feedback
---

When providing shell commands for the user to copy and run, keep comments outside the code block — not inline. Put explanatory text in markdown above the code fence so the command is clean to copy-paste.

**Why:** The user wants to copy commands directly without stripping out comments, but still wants the context.

**How to apply:** Use markdown text or headings above each code block to explain what it does. Keep the code block itself comment-free. Always include `cd` to the correct directory or use absolute paths — never assume the user is in a specific directory. Always use PowerShell syntax (`;` not `&&`, label code blocks as `powershell`). The user runs commands in PowerShell on Windows. Always specify which terminal/window to run the command in (e.g. "In PowerShell:", "In a new PowerShell window:", "In WSL2:").
