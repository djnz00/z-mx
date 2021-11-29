package com.shardmx.mxmd;

public interface MxMDAllTickSizesFn {
  boolean fn(MxMDTickSize ts);	// non-zero aborts iteration
}
