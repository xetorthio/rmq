package redis.rmq;

import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.Tuple;

public class Consumer {
    private Jedis jedis;
    private Nest topic;
    private Nest subscriber;
    private String id;

    public Consumer(final Jedis jedis, final String id, final String topic) {
        this.jedis = jedis;
        this.topic = new Nest("topic:" + topic, jedis);
        this.subscriber = new Nest(this.topic.cat("subscribers").key(), jedis);
        this.id = id;
    }

    private void waitForMessages() {
        jedis.subscribe(new JedisPubSub() {
            public void onUnsubscribe(String channel, int subscribedChannels) {
            }

            public void onSubscribe(String channel, int subscribedChannels) {
            }

            public void onPUnsubscribe(String pattern, int subscribedChannels) {
            }

            public void onPSubscribe(String pattern, int subscribedChannels) {
            }

            public void onPMessage(String pattern, String channel,
                    String message) {
            }

            public void onMessage(String channel, String message) {
                unsubscribe();
            }
        }, topic.key());
    }

    public void consume(Callback callback) {
        while (true) {
            readUntilEnd(callback);
            waitForMessages();
        }
    }

    private void readUntilEnd(Callback callback) {
        String message = null;
        do {
            message = read();
            if (message != null) {
                callback.onMessage(message);
                goNext();
            }
        } while (message != null);
    }

    public String consume() {
        String message = read();
        goNext();
        return message;
    }

    private void goNext() {
        subscriber.zincrby(1, id);
    }

    private int getLastReadMessage() {
        Double lastMessageRead = subscriber.zscore(id);
        if (lastMessageRead == null) {
            Set<Tuple> zrangeWithScores = subscriber.zrangeWithScores(0, 1);
            if (zrangeWithScores.iterator().hasNext()) {
                Tuple next = zrangeWithScores.iterator().next();
                Integer lowest = (int) next.getScore() - 1;
                subscriber.zadd(lowest, id);
                return lowest;
            } else {
                return 0;
            }
        }
        return lastMessageRead.intValue();
    }

    private int getTopicSize() {
        String stopicSize = topic.get();
        int topicSize = 0;
        if (stopicSize != null) {
            topicSize = Integer.valueOf(stopicSize);
        }
        return topicSize;
    }

    public String read() {
        int lastReadMessage = getLastReadMessage();
        return topic.cat("message").cat(lastReadMessage + 1).get();
    }

    public int unreadMessages() {
        return getTopicSize() - getLastReadMessage();
    }
}