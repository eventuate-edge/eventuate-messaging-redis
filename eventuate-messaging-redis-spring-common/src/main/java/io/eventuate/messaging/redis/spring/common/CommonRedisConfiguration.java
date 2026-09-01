package io.eventuate.messaging.redis.spring.common;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.redisson.api.RedissonClient;
import org.redisson.Redisson;
import org.redisson.config.Config;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.data.redis.autoconfigure.DataRedisConnectionDetails;
import org.springframework.boot.ssl.SslBundle;
import org.springframework.util.StringUtils;

@Configuration
public class CommonRedisConfiguration {

  @Bean
  public RedisConfigurationProperties redisConfigurationProperties() {
    return new RedisConfigurationProperties();
  }

  @Bean
  @ConditionalOnProperty(name = "eventuate.redis.servers")
  public RedisServers redisServers(RedisConfigurationProperties redisConfigurationProperties) {
    return new RedisServers(redisConfigurationProperties.getServers());
  }

  @Bean
  public RedissonClients redissonClients(ObjectProvider<RedissonClient> redissonClient,
                                         ObjectProvider<RedisServers> redisServers,
                                         ObjectProvider<DataRedisConnectionDetails> redisDetails) {
    DataRedisConnectionDetails details = redisDetails.getIfAvailable();
    if (details != null) return new RedissonClients(Redisson.create(toRedissonConfig(details)));
    RedissonClient client = redissonClient.getIfAvailable();
    if (client != null) {
      return new RedissonClients(client);
    }
    RedisServers legacyServers = redisServers.getIfAvailable();
    if (legacyServers == null) {
      throw new IllegalStateException("Configure spring.data.redis.*, provide a RedissonClient, or configure the deprecated eventuate.redis.servers property");
    }
    return new RedissonClients(legacyServers);
  }

  private Config toRedissonConfig(DataRedisConnectionDetails details) {
    Config config = new Config();
    String password = details.getPassword();
    if (details.getCluster() != null) {
      var c = config.useClusterServers();
      details.getCluster().getNodes().forEach(n -> c.addNodeAddress(address(n.host(), n.port(), details.getSslBundle())));
      applyCommonRedisConfig(c, details.getUsername(), password);
    } else if (details.getSentinel() != null) {
      var s = config.useSentinelServers().setMasterName(details.getSentinel().getMaster());
      details.getSentinel().getNodes().forEach(n -> s.addSentinelAddress(address(n.host(), n.port(), details.getSslBundle())));
      s.setDatabase(details.getSentinel().getDatabase());
      applyCommonRedisConfig(s, details.getSentinel().getUsername(), details.getSentinel().getPassword());
    } else if (details.getMasterReplica() != null) {
      var m = config.useMasterSlaveServers();
      var nodes = details.getMasterReplica().getNodes();
      if (nodes.isEmpty()) throw new IllegalStateException("spring.data.redis.masterreplica.nodes must not be empty");
      m.setMasterAddress(address(nodes.get(0).host(), nodes.get(0).port(), details.getSslBundle()));
      nodes.subList(1, nodes.size()).forEach(n -> m.addSlaveAddress(address(n.host(), n.port(), details.getSslBundle())));
      applyCommonRedisConfig(m, details.getUsername(), password);
    } else {
      var standalone = details.getStandalone();
      if (standalone == null) throw new IllegalStateException("Redis connection details do not define a topology");
      applyCommonRedisConfig(config.useSingleServer().setAddress(address(standalone.getHost(), standalone.getPort(), details.getSslBundle())).setDatabase(standalone.getDatabase()), details.getUsername(), password);
    }
    return config;
  }

  private String address(String host, int port, SslBundle sslBundle) {
    return (sslBundle != null ? "rediss://" : "redis://") + host + ":" + port;
  }

  private void applyCommonRedisConfig(org.redisson.config.BaseConfig<?> redissonConfig, String username, String password) {
    if (StringUtils.hasText(username)) redissonConfig.setUsername(username);
    if (StringUtils.hasText(password)) redissonConfig.setPassword(password);
  }
}
