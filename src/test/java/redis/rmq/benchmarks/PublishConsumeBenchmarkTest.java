package redis.rmq.benchmarks;

import java.io.IOException;
import java.util.Calendar;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.Jedis;
import redis.rmq.Consumer;
import redis.rmq.Producer;

public class PublishConsumeBenchmarkTest extends Assert {
    @Before
    public void setUp() throws IOException {
        Jedis jedis = new Jedis("localhost");
        jedis.flushAll();
        jedis.disconnect();

    }

    @Test
    public void publish() {
        final String topic = "foo";
        final String message = "hello world!";
        final int MESSAGES = 10000;
        Producer p = new Producer(new Jedis("localhost"), topic);

        long start = Calendar.getInstance().getTimeInMillis();
        for (int n = 0; n < MESSAGES; n++) {
            p.publish(message);
        }
        long elapsed = Calendar.getInstance().getTimeInMillis() - start;
        System.out.println(((1000 * MESSAGES) / elapsed) + " ops");
    }

    @Test
    public void consume() {
        final String topic = "foo";
        final String message = "hello world!";
        final int MESSAGES = 10000;
        Producer p = new Producer(new Jedis("localhost"), topic);
        Consumer c = new Consumer(new Jedis("localhost"), "consumer", topic);
        for (int n = 0; n < MESSAGES; n++) {
            p.publish(message);
        }

        long start = Calendar.getInstance().getTimeInMillis();
        String m = null;
        do {
            m = c.consume();
        } while (m != null);
        long elapsed = Calendar.getInstance().getTimeInMillis() - start;

        System.out.println(((1000 * MESSAGES) / elapsed) + " ops");
    }
}