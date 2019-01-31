package com.mpojeda84.mapr;


import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.ojai.joda.DateTime;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;

public class CarDataFileProducer {

    private String topic;
    private String folder;
    private int delay = 0;

    public CarDataFileProducer(String topic, String folder) {
        this.topic = topic;
        this.folder = folder;
    }

    private String replaceDateTime(String line) {
        return new DateTime().toString() + ","+ line.split(",", 2)[1];
    }

    private void sendLines(Path path) {
        try {
            KafkaProducer<String, String> kafkaProducer = getNewDefaultProducer();

            Files.lines(path)
                    //.map(this::replaceDateTime)
                    .forEach(new Consumer<String>() {

                        int count = 0;
                        public void accept(String s){
                            ProducerRecord<String, String> record = new ProducerRecord<>(topic, s);
                            try {
                                Thread.sleep(delay);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            kafkaProducer.send(record);
                            System.out.println( s + " --> count: " + count++);
                        }
                    });
            kafkaProducer.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void produceCarData(int delay, int threadPoolSize) throws IOException {

        this.delay = delay;
        final ExecutorService executor = Executors.newFixedThreadPool(threadPoolSize);
        final List<Future<?>> futures = new ArrayList<>();
        Files.list(Paths.get(folder)).forEach(x -> {
            Future<?> future = executor.submit(() -> {
                sendLines(x);
            });
            futures.add(future);
        });

        try {
            for (Future<?> future : futures) {
                future.get();
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

    }

    private KafkaProducer<String, String> getNewDefaultProducer() {

        Properties props = new Properties();
        props.setProperty("batch.size", "16384");
        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("block.on.buffer.full", "true");

        return new KafkaProducer<>(props);
    }

    public static Options generateOptions() {
        final Option file = Option.builder("f")
                .required()
                .longOpt("topic")
                .hasArg()
                .desc("Topic in Mapr Stream")
                .build();
        final Option topic = Option.builder("t")
                .required()
                .longOpt("topic")
                .hasArg()
                .desc("Topic in Mapr Stream")
                .build();
        final Option delay = Option.builder("d")
                .required()
                .longOpt("delay")
                .hasArg()
                .desc("Delay")
                .build();


        final Options options = new Options();
        options.addOption(file);
        options.addOption(topic);
        options.addOption(delay);
        return options;
    }


}
