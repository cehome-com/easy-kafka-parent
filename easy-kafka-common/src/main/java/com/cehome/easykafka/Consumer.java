package com.cehome.easykafka;

/**
 * Created by houyanlin on 2018/06/25
 **/
public interface Consumer {

    public void createKafkaConsumer();

    public void subscribe(String... topic);


    public Object poll(long timeout);

    public void commitSync();

}
