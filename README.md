threaded-actor
==============

A simple, lightweight python implementation of the actor model. Each actor has its own thread.

This actor is a "bundle of mutable state with an event loop that handles messages one-at-a-time from a mailbox."

Only actors can change their own state, which makes concurrency easy, and actors communicate with messages.

This implementation is lightweight, easy to understand, has no external dependencies, and suffices for many use cases where you want to prevent concurrency-related errors.

This "one-thread-per-actor" implementation isn't RAM-efficient or CPU-efficient as the number of actors grows high (consider a greenlet-based implementation instead).

Feel free to use this code as a basis for your own Actor implementations.

Use like this:

    class MyActor(Actor):

        on = Actor.make_handles()

        @on(Message1)
        def do(self, msg):
            pass

        @on(Message2)
        def do(self, msg):
            pass

See the unit test for examples.

"""

