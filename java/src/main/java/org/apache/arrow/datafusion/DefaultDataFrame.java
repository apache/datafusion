package org.apache.arrow.datafusion;

import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

class DefaultDataFrame implements DataFrame, AutoCloseable {

  private static final Logger logger = LogManager.getLogger(DefaultDataFrame.class);

  private final ExecutionContext context;
  private final long pointer;

  DefaultDataFrame(ExecutionContext context, long pointer) {
    this.context = context;
    logger.printf(Level.INFO, "obtaining %x", pointer);
    this.pointer = pointer;
  }

  @Override
  public void close() throws Exception {
    logger.printf(Level.INFO, "closing %x", pointer);
    DataFrames.destroyDataFrame(pointer);
  }

  @Override
  public ArrowReader getReader() {
    throw new UnsupportedOperationException("not implemented");
  }
}
