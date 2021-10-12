package org.apache.arrow.datafusion;

class DataFrames {

  private DataFrames() {}

  static native void destroyDataFrame(long pointer);
}
