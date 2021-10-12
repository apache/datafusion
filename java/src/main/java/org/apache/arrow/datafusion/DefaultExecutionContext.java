package org.apache.arrow.datafusion;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

class DefaultExecutionContext implements ExecutionContext {

  private static final Logger logger = LogManager.getLogger(DefaultExecutionContext.class);

  static native long querySql(
      DefaultExecutionContext self, long contextId, long invocationId, String sql);

  public void onErrorMessage(long invocationId, String errorMessage) {
    String oldError = errorMessageInbox.put(invocationId, errorMessage);
    assert oldError == null : "impossibly got duplicated invocation id";
  }

  @Override
  public DataFrame sql(String sql) {
    long invocationId = ThreadLocalRandom.current().nextLong();
    long dataFramePointerId = querySql(this, this.pointer, invocationId, sql);
    if (dataFramePointerId <= 0) {
      throw getErrorForInvocation(invocationId);
    } else {
      DefaultDataFrame frame = new DefaultDataFrame(this, dataFramePointerId);
      DefaultDataFrame absent = dataFrames.putIfAbsent(dataFramePointerId, frame);
      assert null == absent : "got duplicated frame";
      return frame;
    }
  }

  private RuntimeException getErrorForInvocation(long invocationId) {
    String errorMessage = errorMessageInbox.get(invocationId);
    assert errorMessage != null : "onErrorMessage was not properly called from JNI";
    return new RuntimeException(errorMessage);
  }

  @Override
  public void close() throws Exception {
    for (DefaultDataFrame frame : dataFrames.values()) {
      frame.close();
    }
    logger.printf(Level.INFO, "closing %x", pointer);
    ExecutionContexts.destroyExecutionContext(pointer);
  }

  private final long pointer;
  private final ConcurrentMap<Long, String> errorMessageInbox;
  private final ConcurrentMap<Long, DefaultDataFrame> dataFrames;

  DefaultExecutionContext(long pointer) {
    logger.printf(Level.INFO, "obtaining %x", pointer);
    this.pointer = pointer;
    this.errorMessageInbox = new ConcurrentHashMap<>();
    this.dataFrames = new ConcurrentHashMap<>();
  }
}
