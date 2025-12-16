
package com.codeicator.infrastructure.reactivebus.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import com.codeicator.messages.Message;
import org.springframework.context.annotation.Bean;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Handle {

    Class<? extends Message> messagType() default Message.class;
    String topic() default "#";
}
