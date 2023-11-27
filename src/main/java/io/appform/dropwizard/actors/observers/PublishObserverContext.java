package io.appform.dropwizard.actors.observers;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class PublishObserverContext {
    String operation;
    String queueName;
}
