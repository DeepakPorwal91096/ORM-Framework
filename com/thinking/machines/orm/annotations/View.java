package com.thinking.machines.orm.annotations;
import java.lang.annotation.*;
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface View
{
String name();
}