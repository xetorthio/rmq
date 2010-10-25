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
        final String topic = "foo";
        final String subscriber = "a subscriber";
        final String message = "hello world!";

        Producer p = new Producer(new Jedis("localhost"));
        Consumer c = new Consumer(new Jedis("localhost"), subscriber);

        p.publish(topic, message);
        assertEquals(message, c.consume(topic));
    }

    @Test
    public void publishAndRead() {
        final String topic = "foo";
        final String subscriber = "a subscriber";
        final String message = "hello world!";

        Producer p = new Producer(new Jedis("localhost"));
        Consumer c = new Consumer(new Jedis("localhost"), subscriber);

        p.publish(topic, message);
        assertEquals(message, c.read(topic));
        assertEquals(message, c.read(topic));
    }

    @Test
    public void unreadMessages() {
        final String topic = "foo";
        final String subscriber = "a subscriber";
        final String message = "hello world!";

        Producer p = new Producer(new Jedis("localhost"));
        Consumer c = new Consumer(new Jedis("localhost"), subscriber);

        assertEquals(0, c.unreadMessages(topic));
        p.publish(topic, message);
        assertEquals(1, c.unreadMessages(topic));
        p.publish(topic, message);
        assertEquals(2, c.unreadMessages(topic));
        c.consume(topic);
        assertEquals(1, c.unreadMessages(topic));
    }

}