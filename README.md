# rmq (Redis Message Queue)

rmq is a small and very easy to use message queue based on [Redis](http://github.com/antirez/redis "Redis").

rmq uses [Jedis](http://github.com/xetorthio/jedis "Jedis") as a Redis client.

rmq in intended to be fast and reliable.

## What is the difference with Redis pub/sub?

The main difference is that subscribers don't need to be online or they will miss messages. rmq track which messages the client didn't read and it will ensure that the client will receive them once he comes online.

## How do I use it?

You can download the latests build at: 
    http://github.com/xetorthio/rmq/downloads

To use it just as a producer:
	Producer p = new Producer(new Jedis("localhost"));
	p.publish("some cool topic", "some cool message");

To use it just as a consumer you can

consume messages as they become available (this will block if there are no new messages)
	Consumer c = new Consumer(new Jedis("localhost"));
	c.consume("some cool topic", new Callback() {
		public void onMessage(String topic, String message) {
			//do something here with the message
		}
	});

consume next waiting message and return right away

	Consumer c = new Consumer(new Jedis("localhost"));
	String message = c.consume("some cool topic");

read next message without removing it from the queue

	Consumer c = new Consumer(new Jedis("localhost"));
	String message = c.read("some cool topic");

And you are done!

## License

Copyright (c) 2010 Jonathan Leibiusky

Permission is hereby granted, free of charge, to any person
obtaining a copy of this software and associated documentation
files (the "Software"), to deal in the Software without
restriction, including without limitation the rights to use,
copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the
Software is furnished to do so, subject to the following
conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
OTHER DEALINGS IN THE SOFTWARE.