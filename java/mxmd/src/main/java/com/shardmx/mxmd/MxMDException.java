package com.shardmx.mxmd;

import java.time.Instant;

import org.immutables.value.Value;
import javax.annotation.Nullable;

import com.shardmx.mxbase.*;

@Value.Immutable @MxTuple
public abstract class MxMDException {
  @Value.Default @Nullable
  public Instant time() { return null; }
  public abstract long tid();
  public abstract MxMDSeverity severity();
  @Value.Default @Nullable
  public String filename() { return null; }
  public abstract int lineNumber();
  @Value.Default @Nullable
  public String function() { return null; }
  @Value.Default @Nullable
  public String message() { return null; }

  @Override
  public String toString() {
    String s = new String() + time() + " [" + tid() + "] " + severity();
    if (!"".equals(filename())) { s += " " + filename() + ":" + lineNumber(); }
    if (!"".equals(function())) { s += " " + function() + "()"; }
    if (!"".equals(message())) { s += " " + message(); }
    return s;
  }
}
