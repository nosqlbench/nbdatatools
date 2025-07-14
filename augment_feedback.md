# I keep having to tell the agent to stop putting special handling into non-test code for tests.
# I see the agent remove methods which I have added which are very specifically the point of
# certain design decisions. This is frustrating.
# When the agent is no longer able to continue due to contest length, or prompt, or whatever,
# resuming state in a new session is very painful.
# I should be able to have a file at the root of the project that the agent consults by default to
# include my own directives. I would put things like this.
#
# UI
# Lack of responsiveness in the Augment chat panel is *extremely* annoying. I tend to move rather
# quickly and use many features of the IDE. It's like a suit of armor. So having a piece of it fail
# in such a way is worse than not having it. At least I could continue working on my own.
#
# In general the speed of processing *when the agent knows what it wants to do* is still quite bad.
# It may be that you have to put more state handling into the client side to delegate basic
#  edits, chunking, etc to if these are round-tripping to the server endpoint now.
#
# `The selected text exceeds the allowable limit. Please reduce the amount of text and try again.`
#   - really freakin annoying, hope the fix promised in the next version works well
-
# The stop button should *NEVER* not work.

# My agent UI seems to be getting slower... and slower .... and slower ... the more I use it
