package com.cehome.easykafka.producer;

import com.cehome.easykafka.Producer;
import com.cehome.easykafka.StandardKafkaClassLoader;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Properties;

/**
 * Created by houyanlin on 2018/06/22
 **/
public class KafkaProducer implements Producer{

    private String version;
    private Properties props;
    private StandardKafkaClassLoader standardKafkaClassLoader;
    private Class<?> recordClazz;
    private Class<?> producerClazz;
    private Object producerInstance;
    private Constructor recordConstructor;



    public KafkaProducer(String version, Properties props){
        this.version = version;
        this.props = props;
        standardKafkaClassLoader = new StandardKafkaClassLoader(version);
        try {
            Thread.currentThread().setContextClassLoader(null);
            this.producerClazz = standardKafkaClassLoader.findClass("org.apache.kafka.clients.producer.KafkaProducer");
            this.recordClazz = standardKafkaClassLoader.findClass("org.apache.kafka.clients.producer.ProducerRecord");
            Constructor pruducerConstructor = producerClazz.getConstructor(Properties.class);
            this.producerInstance = pruducerConstructor.newInstance(props);
            this.recordConstructor = recordClazz.getConstructor(String.class,Object.class,Object.class);
        }catch (ClassNotFoundException e){
            e.printStackTrace();
        }catch (NoSuchMethodException e){
            e.printStackTrace();
        }catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }

    }
    @Override
    public Object send(String topic, String key, String value) throws Exception{
        Object recordInstance = recordConstructor.newInstance(topic,key,value);
        Method method = producerClazz.getMethod("send", recordClazz);
        method.invoke(producerInstance,recordInstance);
        return value;
    }

    @Override
    public void close() throws Exception{
        Method method = producerClazz.getMethod("close");
        method.invoke(producerInstance);
    }

}
