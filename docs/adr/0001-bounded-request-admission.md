# Reject incoming requests when handler capacity is exhausted

The receive loop continues processing responses even when all incoming handler slots are occupied. Excess incoming requests receive a library-defined overload error; excess notifications are dropped and reported through bounded diagnostics. Waiting for handler capacity in the receive loop could deadlock peers whose active handlers await reverse calls, so bounded execution permits overload failures rather than guaranteeing successful calls at arbitrary nesting depths.
