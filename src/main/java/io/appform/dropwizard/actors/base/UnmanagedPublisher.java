package io.appform.dropwizard.actors.base;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.MessageProperties;
import io.appform.dropwizard.actors.actor.ActorConfig;
import io.appform.dropwizard.actors.actor.DelayType;
import io.appform.dropwizard.actors.base.utils.NamingUtils;
import io.appform.dropwizard.actors.common.Constants;
import io.appform.dropwizard.actors.common.PublishOperations;
import io.appform.dropwizard.actors.connectivity.RMQConnection;
import io.appform.dropwizard.actors.observers.PublishObserverContext;
import io.appform.dropwizard.actors.observers.RMQObserver;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.lang3.RandomUtils;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;

import static io.appform.dropwizard.actors.common.Constants.MESSAGE_EXPIRY_TEXT;
import static io.appform.dropwizard.actors.common.Constants.MESSAGE_PUBLISHED_TEXT;

@Slf4j
public class UnmanagedPublisher<Message> {

    private final String name;
    private final ActorConfig config;
    private final RMQConnection connection;
    private final ObjectMapper mapper;
    private final String queueName;
    private final RMQObserver observer;
    private Channel publishChannel;

    public UnmanagedPublisher(
            String name,
            ActorConfig config,
            RMQConnection connection,
            ObjectMapper mapper) {
        this.name = NamingUtils.prefixWithNamespace(name);
        this.config = config;
        this.connection = connection;
        this.mapper = mapper;
        this.queueName = NamingUtils.queueName(config.getPrefix(), name);
        this.observer = connection.getRootObserver();
    }

    public final void publishWithDelay(final Message message, final long delayMilliseconds) throws Exception {
        log.info("Publishing message to exchange with delay: {}", delayMilliseconds);
        if (!config.isDelayed()) {
            log.warn("Publishing delayed message to non-delayed queue queue:{}", queueName);
        }

        if (config.getDelayType() == DelayType.TTL) {
            val context = PublishObserverContext.builder()
                    .operation(PublishOperations.PUBLISH_WITH_DELAY.name())
                    .queueName(queueName)
                    .build();
            observer.executePublish(context, () -> {
                try {
                    publishChannel.basicPublish(ttlExchange(config),
                            queueName,
                            new AMQP.BasicProperties.Builder()
                                    .expiration(String.valueOf(delayMilliseconds))
                                    .deliveryMode(2)
                                    .headers(Collections.singletonMap(Constants.SPYGLASS_SOURCE_ID, queueName))
                                    .build(),
                            mapper().writeValueAsBytes(message));
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return null;
            });
        } else {
            publish(message, new AMQP.BasicProperties.Builder()
                    .headers(Collections.singletonMap("x-delay", delayMilliseconds))
                    .deliveryMode(2)
                    .build(), PublishOperations.PUBLISH_WITH_DELAY.name());
        }
    }

    public final void publishWithExpiry(final Message message, final long expiryInMs) throws Exception {
        AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                .deliveryMode(2)
                .build();
        if (expiryInMs > 0) {
            val expiresAt = Instant.now().toEpochMilli() + expiryInMs;
            properties = properties.builder()
                    .headers(ImmutableMap.of(MESSAGE_EXPIRY_TEXT, expiresAt))
                    .build();
        }
        publish(message, properties, PublishOperations.PUBLISH_WITH_EXPIRY.name());
    }

    public final void publish(final Message message) throws Exception {
        publish(message, MessageProperties.MINIMAL_PERSISTENT_BASIC, PublishOperations.PUBLISH.name());
    }

    public final void publish(final Message message, final AMQP.BasicProperties properties, final String operation) throws Exception {
        log.info("Publishing message");
        String routingKey;
        if (config.isSharded()) {
            routingKey = NamingUtils.getShardedQueueName(queueName, getShardId());
        } else {
            routingKey = queueName;
        }
        val context = PublishObserverContext.builder().operation(operation).queueName(queueName).build();
        observer.executePublish(context, () -> {
            val enrichedProperties = getEnrichedProperties(properties);
            try {
                log.info("Publishing with routingKey: {}", routingKey);
                publishChannel.basicPublish(config.getExchange(), routingKey, enrichedProperties, mapper().writeValueAsBytes(message));
                log.info("Published with routingKey: {}", routingKey);
            } catch (IOException e) {
                log.error("Error while publishing: {}", e);
                e.printStackTrace();
            }
            return null;
        });
    }

    private AMQP.BasicProperties getEnrichedProperties(AMQP.BasicProperties properties) {
        HashMap<String, Object> enrichedHeaders = new HashMap<>();
        if (properties.getHeaders() != null) {
            enrichedHeaders.putAll(properties.getHeaders());
        }
        enrichedHeaders.put(MESSAGE_PUBLISHED_TEXT, Instant.now().toEpochMilli());
        enrichedHeaders.put(Constants.SPYGLASS_SOURCE_ID, queueName);
        return properties.builder()
                .headers(Collections.unmodifiableMap(enrichedHeaders))
                .build();
    }

    private int getShardId() {
        return RandomUtils.nextInt(0, config.getShardCount());
    }

    public final long pendingMessagesCount() {
        try {
            if (config.isSharded()) {
                long messageCount  = 0 ;
                for (int i = 0; i < config.getShardCount(); i++) {
                    String shardedQueueName = NamingUtils.getShardedQueueName(queueName, i);
                    messageCount += publishChannel.messageCount(shardedQueueName);
                }
                return messageCount;
            }
            else {
                return publishChannel.messageCount(queueName);
            }
        } catch (IOException e) {
            log.error("Issue getting message count. Will return max", e);
        }
        return Long.MAX_VALUE;
    }

    public final long pendingSidelineMessagesCount() {
        try {
            return publishChannel.messageCount(NamingUtils.getSideline(queueName));
        } catch (IOException e) {
            log.error("Issue getting message count. Will return max", e);
        }
        return Long.MAX_VALUE;
    }

    public void start() throws Exception {
        final String exchange = config.getExchange();
        final String dlx = NamingUtils.getSideline(config.getExchange());
        if (config.isDelayed()) {
            ensureDelayedExchange(exchange);
        } else {
            ensureExchange(exchange);
        }
        ensureExchange(dlx);

        this.publishChannel = connection.newChannel();
        String sidelineQueueName = NamingUtils.getSideline(queueName);
        connection.ensure(sidelineQueueName, queueName, dlx, connection.rmqOpts(config));
        if (config.isSharded()) {
            int bound = config.getShardCount();
            for (int shardId = 0; shardId < bound; shardId++) {
                String shardedQueueName = NamingUtils.getShardedQueueName(queueName, shardId);
                connection.ensure(shardedQueueName, config.getExchange(), connection.rmqOpts(dlx, config));
                connection.addBinding(sidelineQueueName, dlx, shardedQueueName);
            }
        } else {
            connection.ensure(queueName, config.getExchange(), connection.rmqOpts(dlx, config));
        }

        if (config.getDelayType() == DelayType.TTL) {
            connection.ensure(ttlQueue(queueName),
                    queueName,
                    ttlExchange(config),
                    connection.rmqOpts(exchange, config));
        }
    }

    private void ensureExchange(String exchange) throws IOException {
        connection.channel().exchangeDeclare(
                exchange,
                "direct",
                true,
                false,
                ImmutableMap.<String, Object>builder()
                        .put("x-ha-policy", "all")
                        .put("ha-mode", "all")
                        .build());
        log.info("Created exchange: {}", exchange);
    }

    private void ensureDelayedExchange(String exchange) throws IOException {
        if (config.getDelayType() == DelayType.TTL) {
            ensureExchange(ttlExchange(config));
        } else {
            connection.channel().exchangeDeclare(
                    exchange,
                    "x-delayed-message",
                    true,
                    false,
                    ImmutableMap.<String, Object>builder()
                            .put("x-ha-policy", "all")
                            .put("ha-mode", "all")
                            .put("x-delayed-type", "direct")
                            .build());
            log.info("Created delayed exchange: {}", exchange);
        }
    }

    private String ttlExchange(ActorConfig actorConfig) {
        return String.format("%s_TTL", actorConfig.getExchange());
    }

    private String ttlQueue(String queueName) {
        return String.format("%s_TTL", queueName);
    }

    public void stop() throws Exception {
        try {
            if(publishChannel.isOpen()) {
                publishChannel.close();
                log.info("Publisher channel closed for [{}] with prefix [{}]", name, config.getPrefix());
            } else {
                log.warn("Publisher channel already closed for [{}] with prefix [{}]", name, config.getPrefix());
            }
        } catch (Exception e) {
            log.error(String.format("Error closing publisher channel for [%s] with prefix [%s]", name, config.getPrefix()), e);
            throw e;
        }
    }

    protected final RMQConnection connection() {
        return connection;
    }

    protected final ObjectMapper mapper() {
        return mapper;
    }
}
