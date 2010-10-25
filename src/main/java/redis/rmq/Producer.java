package redis.rmq;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

public class Producer {
    private Jedis jedis;

    public Producer(Jedis jedis) {
        this.jedis = jedis;
    }

    public void publish(final String topic, final String message) {
        jedis.watch("topic:" + topic);
        final String slastMessageId = jedis.get("topic:" + topic);
        Integer lastMessageId = 0;
        if (slastMessageId != null) {
            lastMessageId = Integer.parseInt(slastMessageId);
        }
        lastMessageId++;
        Transaction trans = jedis.multi();
        trans.set("topic:" + topic + ":message:" + lastMessageId, message);
        trans.incr("topic:" + topic);
        trans.exec();
        jedis.publish(topic, lastMessageId.toString());
    }
}