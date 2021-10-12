package org.apache.arrow.datafusion;

public class ExecutionContexts {

  private ExecutionContexts() {}

  static native long createExecutionContext();

  static native void destroyExecutionContext(long pointer);

  static {
    // This actually loads the shared object that we'll be creating.
    // The actual location of the .so or .dll may differ based on your
    // platform.
    System.loadLibrary("datafusion_jni");
  }

  public static ExecutionContext create() {
    long pointer = createExecutionContext();
    return new DefaultExecutionContext(pointer);
  }
}
