package com.mpojeda84.mapr;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;

//run example: -t "/user/mapr/connected-car/streams/car-data:all" -f "/Users/mpereira/projects/connected-car-data/"

public class Application {



    public static void main(String[] argv)throws Exception {


        CommandLine commandLine = new DefaultParser().parse(CarDataFileProducer.generateOptions(), argv);

        CarDataFileProducer carDataFileProducer = new CarDataFileProducer(commandLine.getOptionValue("t", null), commandLine.getOptionValue("f", null));

        carDataFileProducer.produceCarData(1000, 10);

    }

}
