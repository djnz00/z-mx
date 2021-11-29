package com.shardmx.mxmd;

public interface MxMDAllTickSizeTblsFn {
  boolean fn(MxMDTickSizeTbl tbl);	// non-zero aborts iteration
}
