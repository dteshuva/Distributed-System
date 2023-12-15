package edu.yu.cs.com3800;

import java.io.File;
import java.io.IOException;
import java.util.logging.*;

public interface LoggingServer {

    default Logger initializeLogging(String fileNamePreface) {
        return initializeLogging(fileNamePreface, true);
    }

    default Logger initializeLogging(String fileNamePreface, boolean disableParentHandlers) {
        return createLogger(fileNamePreface, fileNamePreface, disableParentHandlers);
    }

    static Logger createLogger(String loggerName, String fileNamePreface, boolean disableParentHandlers) {
        Logger logger = Logger.getLogger(loggerName);
        logger.setLevel(Level.ALL);
        logger.setUseParentHandlers(!disableParentHandlers);

        new File("./logs").mkdir();
        try {
            FileHandler fh;
            fh = new FileHandler("./logs/" + fileNamePreface + ".log");
            fh.setLevel(Level.ALL);
            fh.setFormatter(new SimpleFormatter());
            logger.addHandler(fh);
        } catch (IOException e) {
            System.err.println("Failed to initialize logging to file");
            e.printStackTrace();
        }

        return logger;
    }
}