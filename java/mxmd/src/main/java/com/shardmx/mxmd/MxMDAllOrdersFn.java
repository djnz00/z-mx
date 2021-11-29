package com.shardmx.mxmd;

public interface MxMDAllOrdersFn {
  boolean fn(MxMDOrder order);	// non-zero aborts iteration
}
