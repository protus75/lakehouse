---
name: feedback_finish_before_moving_on
description: Complete current validation/task fully before moving on — don't offer menu of options
type: feedback
---

Don't present a menu of "options from here" after a partial result. Finish the current task fully — run full validation, review results, fix issues — before suggesting next steps.

**Why:** User was explicit: "fix shit before moving on." A 2% sample is not validation. The project rule is one book at a time until validation passes.

**How to apply:** After a sample run comes back clean, automatically run the full validation. Only report "done" when the full dataset has been checked and issues addressed.
