package com.cehome.easykafka.consumer;

import com.cehome.easykafka.Consumer;
import com.cehome.easykafka.StandardKafkaClassLoader;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;

/**
 * Created by houyanlin on 2018/06/22
 **/
public class KafkaConsumer implements Consumer{

    private String version;
    private Properties props;
    private StandardKafkaClassLoader standardKafkaClassLoader;
    private Class<?> consumerClazz;
    private Constructor consumerConstructor;
    private Object consumerInstance;
    private Class<?> consumerRecordClazz;


    public KafkaConsumer(String version, Properties props){
        this.version = version;
        this.props = props;
        standardKafkaClassLoader = new StandardKafkaClassLoader(version);
    }
    @Override
    public void createKafkaConsumer() {
        try {
            Thread.currentThread().setContextClassLoader(null);
            this.consumerClazz = standardKafkaClassLoader.findClass("org.apache.kafka.clients.consumer.KafkaConsumer");
            this.consumerRecordClazz = standardKafkaClassLoader.findClass("org.apache.kafka.clients.consumer.ConsumerRecord");
            this.consumerConstructor = consumerClazz.getConstructor(Properties.class);
            this.consumerInstance = consumerConstructor.newInstance(props);
        }catch (ClassNotFoundException e){
            e.printStackTrace();
        }catch (NoSuchMethodException e){
            e.printStackTrace();
        }catch (IllegalAccessException e){
            e.printStackTrace();
        }catch (InstantiationException e){
            e.printStackTrace();
        }catch (InvocationTargetException e){
            e.printStackTrace();
        }catch (Exception e){
            e.printStackTrace();
        }
    }


    @Override
    public void subscribe(String[] topic){
        try {
            if (version.startsWith("0.8")){
                Method method = consumerClazz.getMethod("subscribe", String[].class);
                method.invoke(consumerInstance, (Object)topic);
            }else if (version.startsWith("0.9")){
                Method method = consumerClazz.getMethod("subscribe", List.class);
                method.invoke(consumerInstance, Arrays.asList(topic));
            }else if (version.startsWith("0.10")){
                Method method = consumerClazz.getMethod("subscribe", Collection.class);
                method.invoke(consumerInstance, Arrays.asList(topic));
            }else {
                Method method = consumerClazz.getMethod("subscribe", Collection.class);
                method.invoke(consumerInstance, Arrays.asList(topic));
            }

        }catch (Exception e){
            e.printStackTrace();
        }
    }


    @Override
    public List<ConsumerRecord> poll(long timeout) {
        List list = new ArrayList();
        try {
            Method method = consumerClazz.getMethod("poll", long.class);
            Iterable iterable = (Iterable) method.invoke(consumerInstance, timeout);
            if (iterable != null){
                Iterator iterator = iterable.iterator();
                while (iterator.hasNext()){
                    Method valueMethod = consumerRecordClazz.getMethod("value");
                    Method keyMethod = consumerRecordClazz.getMethod("key");
                    Object record = iterator.next();
                    list.add( new ConsumerRecord((String) keyMethod.invoke(record),(String)valueMethod.invoke(record)));
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return list;
    }

    @Override
    public void commitSync() {
       try {
           Method method = consumerClazz.getMethod("commitSync");
           method.invoke(consumerInstance);
       }catch (Exception e){
           e.printStackTrace();
       }
    }

    public void close(){
        try {
            Method method = consumerClazz.getMethod("close");
            method.invoke(consumerInstance);
        }catch (Exception e){
            e.printStackTrace();
        }
    }


}
