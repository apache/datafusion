package org.apache.arrow.datafusion.example;

import org.apache.arrow.datafusion.DataFrame;
import org.apache.arrow.datafusion.ExecutionContext;
import org.apache.arrow.datafusion.ExecutionContexts;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ExampleMain {

  private static final Logger logger = LogManager.getLogger(ExampleMain.class);

  public static void main(String[] args) throws Exception {
    try (ExecutionContext context = ExecutionContexts.create()) {
      DataFrame dataFrame = context.sql("select 1 + 2");
      logger.printf(Level.INFO, "successfully loaded data frame %s", dataFrame);

      dataFrame = context.sql("select 2");
      logger.printf(Level.INFO, "successfully loaded data frame %s", dataFrame);

      dataFrame = context.sql("select cos(2.0)");
      logger.printf(Level.INFO, "successfully loaded data frame %s", dataFrame);
    }
  }
}
