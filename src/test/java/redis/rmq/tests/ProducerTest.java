package redis.rmq.tests;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.Jedis;
import redis.rmq.Callback;
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
        private long sleep;

		public SlowProducer(Jedis jedis, String topic) {
            this(jedis, topic, 500L);
        }

        public SlowProducer(Jedis jedis, String topic, long sleep) {
			super(jedis, topic);
			this.sleep = sleep;
		}

		protected Integer getNextMessageId() {
            Integer nextMessageId = super.getNextMessageId();
            sleep(sleep);
            return nextMessageId;
        }
    }
    
    class SlowConsumer extends Consumer {
        private long sleep;

		public SlowConsumer(Jedis jedis, String id, String topic) {
            this(jedis, id, topic, 500L);
        }

        public SlowConsumer(Jedis jedis, String id, String topic, long sleep) {
			super(jedis, id, topic);
			this.sleep = sleep;
		}

		@Override
		public String consume() {
			sleep(sleep);
			return super.consume();
		}
		
		@Override
		public void consume(Callback callback) {
			sleep(sleep);
			super.consume(callback);
		}
    }
    
    
    @Test
    public void expiredMessages() throws InterruptedException {
        Consumer c = new SlowConsumer(new Jedis("localhost"), "a consumer", "foo", 3000L);
        Producer p = new Producer(new Jedis("localhost"), "foo");
        p.publish("un mensaje", 1);
        assertNull(c.consume());
    }
    
    @Test
    public void expiredMessagesCallback() throws InterruptedException {
        Producer producer = new Producer(new Jedis("localhost"), "foo1");
        Consumer consumer = new Consumer(new Jedis("localhost"), "a consumer", "foo1");
        BufferCallback callback = new BufferCallback(3);
        Thread t = new Thread(new BackgroundConsumer(consumer, callback));
                
        producer.publish("1", 1);
        producer.publish("2", 0);
        producer.publish("3", 1);
        producer.publish("4", 0);
        producer.publish("5", 1);
        producer.publish("6", 0);
        sleep(3100L);
        t.start();
        sleep(3100L);
        
       assertEquals(3, callback.messages.size());
        for (String message : callback.messages) {
        	if (!(Integer.valueOf(message)%2 == 0)) {
        		fail("Mensaje impar no puede ser consumido, deberia expirar. ["+message+"]");
        	}
		}        
    }
    

    private void sleep(long sleep) {
    	try {
			Thread.sleep(sleep);
		} catch (InterruptedException e) {}
	}

	class BufferCallback implements Callback {
    	List<String> messages = new CopyOnWriteArrayList<String>();
    	private int msgs;
	
    	public BufferCallback (int msgs) {
    		this.msgs = msgs;    		
    	}
    	
  		public void onMessage(String message) {
  			messages.add(message);	
  			
  			if (messages.size() == msgs)
  				//tiro exception para parar el callback, ni idea como hacerlo bien :P.
  				throw new RuntimeException("Total de los mensajes consumidos");
		}
	};
	
	class BackgroundConsumer implements Runnable {
		private Consumer consumer;
		private Callback callback;
		
		BackgroundConsumer (Consumer consumer, Callback callback) {
			this.consumer = consumer;
			this.callback = callback;
		}
	    
		public void run() {
	    	consumer.consume(callback);
	    }
	}
    
    
    @Test
    public void expiredFirstMessage() throws InterruptedException {
        Consumer c = new SlowConsumer(new Jedis("localhost"), "a consumer", "foo", 2000L);
        Producer p = new Producer(new Jedis("localhost"), "foo");
        p.publish("1", 1);
        p.publish("2", 0);
        
        //el segundo mensaje deberia estar
        assertEquals("2", c.consume());
    }
    
    @Test
    public void notExpiredMessages() throws InterruptedException {
        Consumer c = new SlowConsumer(new Jedis("localhost"), "a consumer", "foo");
        Producer p = new Producer(new Jedis("localhost"), "foo");
        p.publish("foo", 0);
        assertEquals("foo", c.consume());
    }
    
    
}