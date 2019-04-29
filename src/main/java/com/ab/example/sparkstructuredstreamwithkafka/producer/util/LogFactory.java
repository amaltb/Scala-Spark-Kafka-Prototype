package com.ab.example.sparkstructuredstreamwithkafka.producer.util;

import org.apache.log4j.*;

public class LogFactory {
    public static Logger getLogger(String logAppenderFileName, Level logLevel) {
        final Logger log = LogManager.getLogger("applicationLogger");
        log.setLevel(logLevel);

        final FileAppender fa = new FileAppender();
        fa.setName("FileLogger");
        fa.setFile(logAppenderFileName);
        fa.setLayout(new PatternLayout("[%p] %d %c %M - %m%n"));
        fa.setAppend(true);
        fa.activateOptions();

        log.addAppender(fa);
        log.setAdditivity(false);

        return log;
    }

    public static Logger getLogger(String logAppenderFileName) {
        return getLogger(logAppenderFileName, Level.ALL);
    }


    public static Logger getLogger() {
        return getLogger("./log/application.log", Level.ALL);
    }

    public static Logger getLogger(Level logLevel) {
        return getLogger("./log/application.log", logLevel);
    }
}
