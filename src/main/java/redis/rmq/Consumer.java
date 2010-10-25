package redis.rmq;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

public class Consumer {
    private Jedis jedis;
    private String subscriber;

    public Consumer(Jedis jedis, String subscriber) {
        this.jedis = jedis;
        this.subscriber = subscriber;
    }

    public void consume(final String topic, Callback callback) {
        while (true) {
            readUntilEnd(topic, callback);
            waitForMessages(topic);
        }
    }

    private void waitForMessages(final String topic) {
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
        }, topic);
    }

    private void readUntilEnd(final String topic, Callback callback) {
        String message = consume(topic);
        while (message != null) {
            if (message != null) {
                callback.onMessage(topic, consume(topic));
            }
            message = consume(topic);
        }
    }

    public String consume(final String topic) {
        return getMessage(topic, true);
    }

    private String getMessage(final String topic, boolean goNext) {
        int topicSize = getTopicSize(topic);
        int lastMessage = getLastReadMessage(topic);
        if (lastMessage < topicSize) {
            Integer newMessage = 0;
            if (goNext) {
                newMessage = jedis.incr("topic:" + topic + ":subscriber:"
                        + subscriber);
            } else {
                String snewMessage = jedis.get("topic:" + topic
                        + ":subscriber:" + subscriber);
                if (snewMessage != null) {
                    newMessage = Integer.parseInt(snewMessage);
                }
                newMessage++;
            }
            return jedis.get("topic:" + topic + ":message:" + newMessage);
        }
        return null;
    }

    private int getLastReadMessage(final String topic) {
        String lastMessageRead = jedis.get("topic:" + topic + ":subscriber:"
                + subscriber);
        int lastMessage = 0;
        if (lastMessageRead != null) {
            lastMessage = Integer.valueOf(lastMessageRead);
        }
        return lastMessage;
    }

    private int getTopicSize(final String topic) {
        String stopicSize = jedis.get("topic:" + topic);
        int topicSize = 0;
        if (stopicSize != null) {
            topicSize = Integer.valueOf(stopicSize);
        }
        return topicSize;
    }

    public String read(final String topic) {
        return getMessage(topic, false);
    }

    public int unreadMessages(final String topic) {
        return getTopicSize(topic) - getLastReadMessage(topic);
    }
}