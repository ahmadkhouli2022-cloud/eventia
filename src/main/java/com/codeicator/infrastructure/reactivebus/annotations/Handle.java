package com.codeicator.infrastructure.reactivebus.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import com.codeicator.messages.Message;

/**
 * Marks a method as a reactive bus handler for messages, optionally scoped by topic.
 * Handlers are discovered at runtime and invoked when a matching message is published.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Handle {

    /**
     * The message type the handler expects; defaults to {@link Message} when not specified.
     * Use a concrete subtype to restrict the handler to a specific message class.
     */
    Class<? extends Message> messageType() default Message.class;

    /**
     * The topic to subscribe to; defaults to the wildcard "#" to match all topics.
     */
    String topic() default "#";
}
