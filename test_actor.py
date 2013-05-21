import threading
import time
import unittest
from actor import Actor, Stopped, ActorStopped, Started, Stop

class TestOnDo(unittest.TestCase):

    def test_tell(self):

        called1 = threading.Event()
        called2 = threading.Event()
        called3 = threading.Event()

        class Msg(object): pass

        class Worker(Actor):
            on = Actor.make_handles()
            @on(Started)
            def do(self, msg):
                called1.set()
            @on(Stopped)
            def do(self, msg):
                called2.set()
            @on(Msg)
            def do(self, msg):
                called3.set()

        w = Worker()
        w.start()
        w.tell(Msg(), None)
        w.tell(Stop())

        assert called1.wait(2)
        assert called2.wait(2)
        assert called3.wait(2)

    def test_tell_with_delay(self):

        called = threading.Event()

        class Struct(): pass

        t2 = Struct()
        t2.time = None

        class Msg(object): pass

        class Worker(Actor):
            on = Actor.make_handles()
            @on(Msg)
            def do(self, msg):
                t2.time = time.time()
                called.set()
                w.stop()

        delay = 1
        w = Worker()
        t1 = time.time()
        w.start()
        w.tell(Msg(), None, delay=delay)

        assert called.wait(2)
        assert t2.time - t1 >= delay

    def test_ask(self):

        called = threading.Event()

        class Msg(object): pass

        class Worker(Actor):
            on = Actor.make_handles()
            @on(Msg)
            def do(self, msg):
                called.set()
                self.reply('yes')

        w = Worker()
        w.start()
        response = w.ask(Msg()).get()
        assert response == 'yes'
        w.stop()
        assert called.wait(2)

    def test_stop(self):
        called1 = threading.Event()
        called2 = threading.Event()

        class Msg(object): pass

        class Worker(Actor):
            on = Actor.make_handles()
            @on(Stopped)
            def do(self, msg):
                # print('stop outside Actor')
                called1.set()
            @on(Stopped)
            def do(self, msg):
                # print('stopped outside Actor')
                called2.set()

        w = Worker()
        w.start()
        w.stop()
        assert called1.wait(2)
        assert called2.wait(2)

    def test_watch(self):
        called1 = threading.Event()

        class Msg(object): pass

        class Worker1(Actor):
            on = Actor.make_handles()

        class Worker2(Actor):
            on = Actor.make_handles()
            @on(ActorStopped)
            def do(self, msg):
                called1.set()

        w1 = Worker1()
        w2 = Worker2()

        w1.start()
        w2.start()

        w1.add_watcher(w2)

        w1.stop()

        assert called1.wait(2)

        w2.stop()


if __name__ == '__main__':
    unittest.main()