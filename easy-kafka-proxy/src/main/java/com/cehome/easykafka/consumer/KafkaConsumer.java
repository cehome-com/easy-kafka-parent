package com.cehome.easykafka.consumer;

import com.cehome.easykafka.Consumer;
import com.cehome.easykafka.StandardKafkaClassLoader;
import com.cehome.easykafka.enums.VersionEnum;

import java.lang.reflect.Constructor;
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
    public void createKafkaConsumer() throws Exception{
        Thread.currentThread().setContextClassLoader(null);
        this.consumerClazz = standardKafkaClassLoader.findClass("org.apache.kafka.clients.consumer.KafkaConsumer");
        this.consumerRecordClazz = standardKafkaClassLoader.findClass("org.apache.kafka.clients.consumer.ConsumerRecord");
        this.consumerConstructor = consumerClazz.getConstructor(Properties.class);
        this.consumerInstance = consumerConstructor.newInstance(props);

    }


    @Override
    public void subscribe(String[] topic) throws Exception{
        if (version.startsWith(VersionEnum.KAFKA_VERSION_8.getValue())){
            Method method = consumerClazz.getMethod("subscribe", String[].class);
            method.invoke(consumerInstance, (Object)topic);
        }else if (version.startsWith(VersionEnum.KAFKA_VERSION_9.getValue())){
            Method method = consumerClazz.getMethod("subscribe", List.class);
            method.invoke(consumerInstance, Arrays.asList(topic));
        }else if (version.startsWith(VersionEnum.KAFKA_VERSION_10.getValue())){
            Method method = consumerClazz.getMethod("subscribe", Collection.class);
            method.invoke(consumerInstance, Arrays.asList(topic));
        }else {
            Method method = consumerClazz.getMethod("subscribe", Collection.class);
            method.invoke(consumerInstance, Arrays.asList(topic));
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
           if (version.startsWith(VersionEnum.KAFKA_VERSION_8.getValue())){
//                Method method = consumerClazz.getMethod("commit",boolean.class);
//                method.invoke(consumerInstance,true);
           }else{
               Method method = consumerClazz.getMethod("commitSync");
               method.invoke(consumerInstance);
           }

       }catch (Exception e){
           e.printStackTrace();
       }
    }
    @Override
    public void close(){
        try {
            Method method = consumerClazz.getMethod("close");
            method.invoke(consumerInstance);
        }catch (Exception e){
            e.printStackTrace();
        }
    }


}
