package io.eventuate.messaging.redis.spring.producer;

import io.eventuate.messaging.redis.spring.common.RedisUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.connection.RedisStreamCommands;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;

public class EventuateRedisProducer {
  private Logger logger = LoggerFactory.getLogger(getClass());

  private StringRedisTemplate redisTemplate;
  private int partitions;
  private long streamWithoutConsumerBalanceMaxLen;

  public EventuateRedisProducer(StringRedisTemplate redisTemplate, int partitions) {
    this.redisTemplate = redisTemplate;
    this.partitions = partitions;
    this.streamWithoutConsumerBalanceMaxLen = Long.parseLong(System.getProperty("eventuate.redis.streams.without-consumer-balance-max-len", "0"));
  }

  public CompletableFuture<?> send(String topic, String key, String body) {
    int partition = Math.abs(key.hashCode()) % partitions;

    logger.info("Sending message = {} with key = {} for topic = {}, partition = {}", body, key, topic, partition);

    String streamKey = RedisUtil.channelToRedisStream(topic, partition);
    RedisStreamCommands.XAddOptions options = RedisStreamCommands.XAddOptions.none();
    if (streamWithoutConsumerBalanceMaxLen > 0 && redisTemplate.opsForStream().groups(streamKey).isEmpty()) {
      options = RedisStreamCommands.XAddOptions.maxlen(streamWithoutConsumerBalanceMaxLen);
    }
    redisTemplate.opsForStream().add(StreamRecords
            .string(Collections.singletonMap(key, body))
            .withStreamKey(streamKey), options);

    logger.info("message sent = {} with key = {} for topic = {}, partition = {}", body, key, topic, partition);

    return CompletableFuture.completedFuture(null);
  }

  public void close() {
  }
}
