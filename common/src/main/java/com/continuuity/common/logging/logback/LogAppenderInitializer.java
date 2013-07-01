package com.continuuity.common.logging.logback;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import com.google.inject.Inject;
import org.slf4j.LoggerFactory;

/**
 * Creates and sets the logback log appender.
 */
public class LogAppenderInitializer {
  private final LogAppender logAppender;

  @Inject
  public LogAppenderInitializer(LogAppender logAppender) {
    this.logAppender = logAppender;
  }

  public void intialize() {
    LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();

    // Display any errors during initialization of log appender to console
//    StatusManager statusManager = loggerContext.getStatusManager();
//    OnConsoleStatusListener onConsoleListener = new OnConsoleStatusListener();
//    statusManager.add(onConsoleListener);

    logAppender.setContext(loggerContext);
    logAppender.start();

    Logger rootLogger = loggerContext.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
    rootLogger.addAppender(logAppender);
  }
}
