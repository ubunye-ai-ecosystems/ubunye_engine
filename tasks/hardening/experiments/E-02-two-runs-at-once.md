# E-02: The same task and date run twice at once: does one silently overwrite or duplicate the other?

**Status:** planned
**Why:** Pramen needed lease locks per (table, information date) after concurrent runs collided.

## Method
Start two overlapping runs of one task with the same variables, for each write mode.

## Pass means
The result equals one run's output, or the second run is refused with a clear message.

## Result
Not run yet.
