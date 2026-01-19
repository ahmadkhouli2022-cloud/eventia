package com.codeicator.infrastructure.reactivebus.annotations;

import com.codeicator.messages.Message;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface HandleDomainEvent {

    Class<? extends Message> messageType() default Message.class;

}
