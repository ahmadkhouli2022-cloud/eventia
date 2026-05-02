package com.codeicator.infrastructure.reactivebus.annotations;

import com.codeicator.messages.Message;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a method as a handler for a domain event message in the reactive bus.
 * Handlers are discovered at runtime and invoked when a matching message is published.
 * The handler runs within the same aggregate transactional method.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface HandleDomainEvent {

    /**
     * The message type the handler expects; defaults to {@link Message} when not specified.
     * Use a concrete subtype to restrict the handler to a specific event.
     */
    Class<? extends Message> messageType() default Message.class;

}
