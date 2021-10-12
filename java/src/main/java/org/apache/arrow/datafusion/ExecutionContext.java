package org.apache.arrow.datafusion;

public interface ExecutionContext extends AutoCloseable {

  DataFrame sql(String sql);
}
