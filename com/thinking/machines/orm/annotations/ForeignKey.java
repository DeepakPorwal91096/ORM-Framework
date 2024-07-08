package com.thinking.machines.orm.annotations;
import java.lang.annotation.*;
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface ForeignKey
{
String parent();
String column();
}