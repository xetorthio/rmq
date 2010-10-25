package redis.rmq;

public interface Callback {
    public void onMessage(String topic, String message);
}
