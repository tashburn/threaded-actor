import inspect
import threading
import traceback
import Queue


########################################################################################################################
# Helpers

class HandlerRegistry():
    """
    This is a decorator, and contains the mapping of MessageType to handlingMethod for each Actor.
    """
    def __init__(self, name='', check=True):
        self.map = dict()
        self.name = name
        self.check = check
#        print "CREATING REGISTRY"
    def get(self, typ):
        ret = self.map.get(typ, [])
#        print 'ret is %s' % ret
        return ret
    def put(self, typ, handler):
        assert inspect.isclass(typ)
        assert callable(handler)
        s = self.map.get(typ)
        if s is None:
            s = list()
        s.append(handler)
        self.map[typ] = s
#        print 'put now %s' % self.get(typ)

    def __call__(self, typ):
        if self.check:
            # actors cannot watch these message types
            if typ in [OtherMessage]:
                raise Exception('Actors cannot watch msg %s' % typ.__name__)
#        log.debug('typ %s' % typ)
        def register(func):
#            log.debug('['+self.name+'] register typ %s' % (typ.__name__))
            self.put(typ, func)
#            log.debug('  handlers now %s' % self.get(typ))
            return func
        return register
    def show(self):
        for (typ,handlers) in self.map.iteritems():
            print('  %s : %s' % (typ.__name__, str(handlers)))

# return "file:line" for the caller
def caller_info(extra_levels=0):
    frame, filename, line_number, function_name, lines, index = inspect.getouterframes(inspect.currentframe())[2+extra_levels]
    return '%s:%s' % (filename, line_number)

def get_caller(extra_levels=0):
    """
    Inspects the stack to find out the caller.
    Useful for showing cross-thread stack traces, for when an actor throws an exception.
    """
    stack = inspect.stack()
    level = 2 + extra_levels
    if level >= len(stack): return None
    caller = stack[level][0].f_locals.get('self', None)
    return caller

########################################################################################################################
# Messages

# Used internally to tell when an actor fails to handle a message
class OtherMessage(object): pass

# stops the actor once this message is handled in the queue
class Stop(object): pass

# The actor will automatically receive these messages when it's started or stopped
class Started(object): pass
class Stopped(object): pass

# When actor1 submits Watch() to actor2, actor1 will receive this message when actor1 is stopped.
# actor2 can get a reference to actor1 by calling self.sender.
class ActorStarted(object): pass
class ActorStopped(object): pass

class BeforeHandling(object):
    def __init__(self, message):
        self.message = message
class AfterHandling(object):
    def __init__(self, message, exception=None):
        self.message = message
        self.exception = exception

# If an actor is watching another actor it will get this exception
class ActorException(object):
    def __init__(self, exception):
        self.exception = exception

# This is thrown when tell() or ask() is called on a stopped actor.
class ActorStoppedException(Exception):
    def __init__(self, actor, message):
        super(ActorStoppedException, self).__init__(self, "%s actor not started, message %s" % (actor.__class__.__name__, message.__class__.__name__))

########################################################################################################################
# Actor

class IActor(object):
    """
    This makes the external access points clear.
    It also makes it clear how you'd simulate an actor for unit testing.
    """
    def start(self): pass
    def stop(self): pass
    def join(self): pass
    def add_watcher(self, actor): pass
    def tell(self, message, delay=0): raise NotImplemented
    def ask(self, message, delay=0): raise NotImplemented

class Actor(IActor):

    def __init__(self, name=None, executor=None):

        if name is None: name = self.__class__.__name__
        self.name = name

        if executor is None: executor = Executor(start=False)
        self.executor = executor

        self.lock = threading.RLock()
        self.current_future = None
        self.watchers = set()

    def _inspect_sender(self, extra_levels):
        callr = get_caller(extra_levels)
        if isinstance(callr, Actor):
            return callr
        return None

    def start(self, trace=None):
        with self.lock:
            self.executor.start()

            sender = self._inspect_sender(1)

            self.tell(Started(), sender=sender, trace=caller_info())

            # tell watchers
            started = ActorStarted()
            for actor in self.watchers:
                actor.tell(started, sender=sender)

            return self

    def stop(self):
        """ stops the actor right away (to stop in the event loop, use tell(Stop()) instead). """
        with self.lock:

            sender = self._inspect_sender(1)

            self.executor.stop()

            # tell me i'm Stopped
            self._handle_message_with_registry(Stopped(), None, caller_info(), None)

            # tell watchers
            stopped = ActorStopped()
            for actor in self.watchers:
                actor.tell(stopped, sender=sender)

    def is_started(self):
        with self.lock:
            return self.executor.is_started()

    def join(self):
        """ block until the actor is stopped """
        self.executor.join()

    def add_watcher(self, actor, notify_now=True):
        with self.lock:
            self.watchers.add(actor)

            if notify_now is True:

                # tell watchers the Start/Stop state

                m = None
                if self.executor.is_started():
                    m = ActorStarted()
                else:
                    m = ActorStopped()

                for actor in self.watchers:
                    actor.tell(m, sender=self)


    def tell(self, message, sender=None, delay=None, trace=None):
        if sender is None: sender = self._inspect_sender(2)
        self._post(message, sender, delay, trace)

    def ask(self, message, sender=None, delay=None, trace=None):
        if sender is None: sender = self._inspect_sender(2)
        return self._post(message, sender, delay, trace, Future())

    def _post(self, message, sender=None, delay=None, trace=None, future=None):
        with self.lock:

            # get the caller file:line to show useful trace if an exception is thrown
            if trace is None:
                trace = caller_info(1)

            # run now?
            if delay is None or delay==0:
                if self.executor.is_started():
                    self.executor.submit(Actor.Run(message, sender, trace, future, self))
                else:
                    print('raising ActorStoppedException')
                    traceback.print_stack()
                    raise ActorStoppedException(self, message)
            else:
                # run later
                def run(message, sender, trace, future):
                    if self.executor.is_started():
                        self.executor.submit(Actor.Run(message, sender, trace, future, self))
                    else:
                        if future is not None:
                            future.throw(Exception("This actor is stopped"))
                t = threading.Timer(delay, run, args=(message, sender, trace, future))
                t.daemon = True
                t.start()

            return future

    #-------------------------------------------------------------------------------------------------------------------
    # THESE CAN BE CALLED BY HANDLER METHODS

    def reply(self, value):
        """ Replies to asks. Should be called only within a handler method. """

        # check thread
        assert self.executor.is_on()

        if self.current_future:
            self.current_future.put(value)

    def sender(self):
        """ a handler method calls this to get a reference to the actor who sent the message being handled. """
        return self.sender

    #-------------------------------------------------------------------------------------------------------------------

    class Run(object):
        def __init__(self, message, sender, trace, future, actor):
            self.message = message
            self.sender = sender
            self.trace = trace
            self.future = future
            self.actor = actor
        def __call__(self, *args, **kwargs):
            self.actor._handle_message_with_registry(self.message, self.sender, self.trace, self.future)

    # the prototype registry with handlers shared by all actors
    on = HandlerRegistry(name='Actor', check=False)

    # actors call this to obtain the handler decorator. see examples.
    @classmethod
    def make_handles(cls, name=''):
        hr = HandlerRegistry(name=name)
        for (typ, handlers) in cls.on.map.iteritems():
            for h in handlers:
                hr.put(typ, h)
        return hr

    # handles the message
    def _handle_message_with_registry(self, message, sender, trace, future):

        registry = self.__class__.on

        handlers  = registry.get(message.__class__)
        if handlers is None:
            handlers = registry.get(OtherMessage)

        self.trace = trace
        self.current_future = future
        self.sender = sender

        for handler in handlers:

            for w in self.watchers:
                try:
                    w.tell(BeforeHandling(message), sender=self)
                except:
                    traceback.print_exc()

            exception = None

            try:
                handler(self, message)
            except Exception as e:

                exception = e

                print("Caller: %s" % trace)

                # tell watchers
                for actor in list(self.watchers):
                    actor.tell(ActorException(e))

            for w in self.watchers:
                try:
                    w.tell(AfterHandling(message, exception), sender=self)
                except:
                    traceback.print_exc()

        self.current_future = None
        self.sender = None
        self.trace = None

    @on(Stop)
    def do(self, msg):
        self.stop()

    @on(OtherMessage)
    def do(self, msg):
        print("Message not handled: %s from %s" % (msg, self.sender))


########################################################################################################################

def default_exception_handler(e):
    traceback.print_exc(e)

class Executor(object):
    """ A single-threaded executor """

    def __init__(self, start=True, daemon=True, exception_handler=default_exception_handler):
        self.exception_handler = exception_handler
        self.daemon = daemon

        self.lock = threading.RLock()
        self.started = False
        self.on_deck = Queue.PriorityQueue()
        self.thread = None

        if start:
            self.start()

    def set_exception_handler(self, eh):
        assert callable(eh)
        self.exception_handler = eh

    def is_on(self):
        return threading.current_thread() == self.thread

    def start(self):
        with self.lock:
            if not self.started:
                self.started = True
                self.thread = threading.Thread(target=self._run, args=(self.on_deck,), name=self.__class__.__name__)
                self.thread.setDaemon(self.daemon)
                self.thread.start()

    def stop(self):
        with self.lock:
            if self.started:
                self.started = False
                self.on_deck.put((0,"STOP")) # unblocks the queue
                self.on_deck = Queue.PriorityQueue()
                self.thread = None

    def is_started(self):
        with self.lock:
            return self.started

    def submit(self, callable_):
        with self.lock:
            assert callable(callable_)
            self.on_deck.put((1,callable_), block=False)

    def join(self):
        """ block until the thread exits """
        t = None
        with self.lock:
            if self.thread is None:
                return
            t = self.thread
        return t.join()

    def _run(self, q):
        while True:

            # stopped?
            if not self.started: return

            # wait for a job
            callable = q.get()[1]

            # stop?
            if callable == "STOP": break

            # call
            try:
                callable()
            except Exception as e:
                self.exception_handler(e)


########################################################################################################################

class Future(object):
    """
    For exchanging values between threads.
    One thread calls future.post(value) to post a value.
    Another thread calls future.get() to get the value.
    """
    def __init__(self):
        self.event = threading.Event()
        self.value = None
        self.exception = None

    def put(self, value):
        self.value = value
        self.event.set()

    def get(self, timeout=None):
        if timeout is None:
            self.event.wait()
            if self.exception:
                return self.exception
            return self.value
        else:
            if self.event.wait(timeout):
                if self.exception:
                    return self.exception
                return self.value
            else:
                raise Exception('future get() timed out')

    def throw(self, exception):
        self.exception = exception
        self.event.set()
