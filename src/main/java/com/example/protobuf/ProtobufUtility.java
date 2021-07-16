package com.example.protobuf;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class ProtobufUtility {
    public ProtobufUtility() {
    }
    public static String getJson(MessageOrBuilder message) throws InvalidProtocolBufferException {
        String jsonString = JsonFormat.printer().print(message);
        return jsonString;
    }
    public static MessageOrBuilder getMessageOrBuilder(String className, byte[] bookBytes) throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Class c = Class.forName(className);
        Method m = c.getDeclaredMethod("parseFrom", byte[].class);
        Object o = m.invoke((Object)null, bookBytes);
        return (MessageOrBuilder)o;
    }
    public static String convertToJson(String className, byte[] bookBytes) throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException, InvalidProtocolBufferException {
        return JsonFormat.printer().print(getMessageOrBuilder(className, bookBytes));
    }
}