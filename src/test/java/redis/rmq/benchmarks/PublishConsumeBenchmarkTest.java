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
    public void publishAndConsume() {
        final String topic = "foo";
        final String subscriber = "a subscriber";
        final String message = "hello world!";
        final int MESSAGES = 100000;

        Consumer c = new Consumer(new Jedis("localhost"), subscriber);

        long start = Calendar.getInstance().getTimeInMillis();
        new Thread(new Runnable() {
            public void run() {
                Producer p = new Producer(new Jedis("localhost"));
                for (int n = 0; n < MESSAGES; n++) {
                    p.publish(topic, message + "n");
                }
            }
        }).start();

        for (int n = 0; n < MESSAGES; n++) {
            c.consume(topic);
        }
        long elapsed = Calendar.getInstance().getTimeInMillis() - start;
        System.out.println(((1000 * MESSAGES) / elapsed) + " ops");
    }
}