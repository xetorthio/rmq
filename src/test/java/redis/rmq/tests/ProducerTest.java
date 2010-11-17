package redis.rmq.tests;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.Jedis;
import redis.rmq.Consumer;
import redis.rmq.Producer;

public class ProducerTest extends Assert {
    @Before
    public void setUp() throws IOException {
        Jedis jedis = new Jedis("localhost");
        jedis.flushAll();
        jedis.disconnect();
    }

    @Test
    public void publishAndConsume() {
        Producer p = new Producer(new Jedis("localhost"), "foo");
        Consumer c = new Consumer(new Jedis("localhost"), "a subscriber", "foo");

        p.publish("hello world!");
        assertEquals("hello world!", c.consume());
    }

    @Test
    public void publishAndRead() {
        Producer p = new Producer(new Jedis("localhost"), "foo");
        Consumer c = new Consumer(new Jedis("localhost"), "a subscriber", "foo");

        p.publish("hello world!");
        assertEquals("hello world!", c.read());
        assertEquals("hello world!", c.read());
    }

    @Test
    public void unreadMessages() {
        Producer p = new Producer(new Jedis("localhost"), "foo");
        Consumer c = new Consumer(new Jedis("localhost"), "a subscriber", "foo");

        assertEquals(0, c.unreadMessages());
        p.publish("hello world!");
        assertEquals(1, c.unreadMessages());
        p.publish("hello world!");
        assertEquals(2, c.unreadMessages());
        c.consume();
        assertEquals(1, c.unreadMessages());
    }

    @Test
    public void raceConditionsWhenPublishing() throws InterruptedException {
        Producer slow = new SlowProducer(new Jedis("localhost"), "foo");
        Consumer c = new Consumer(new Jedis("localhost"), "a subscriber", "foo");

        slow.publish("a");
        Thread t = new Thread(new Runnable() {
            public void run() {
                Producer fast = new Producer(new Jedis("localhost"), "foo");
                fast.publish("b");
            }
        });
        t.start();
        t.join();

        assertEquals("a", c.consume());
        assertEquals("b", c.consume());
    }

    @Test
    public void eraseOldMessages() {
        Producer p = new Producer(new Jedis("localhost"), "foo");
        Consumer c = new Consumer(new Jedis("localhost"), "a subscriber", "foo");

        p.publish("a");
        p.publish("b");

        assertEquals("a", c.consume());

        p.clean();

        Consumer nc = new Consumer(new Jedis("localhost"), "new subscriber",
                "foo");

        assertEquals("b", c.consume());
        assertEquals("b", nc.consume());
        assertNull(c.consume());
        assertNull(nc.consume());
    }

    class SlowProducer extends Producer {
        public SlowProducer(Jedis jedis, String topic) {
            super(jedis, topic);
        }

        protected Integer getNextMessageId() {
            Integer nextMessageId = super.getNextMessageId();
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
            }
            return nextMessageId;
        }
    }
}