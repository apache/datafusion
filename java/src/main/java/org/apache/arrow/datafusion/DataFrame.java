package org.apache.arrow.datafusion;

import org.apache.arrow.vector.ipc.ArrowReader;

public interface DataFrame {
  ArrowReader getReader();
}
