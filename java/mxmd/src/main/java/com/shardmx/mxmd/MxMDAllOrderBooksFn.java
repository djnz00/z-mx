package com.shardmx.mxmd;

public interface MxMDAllOrderBooksFn {
  boolean fn(MxMDOrderBook ob);	// non-zero aborts iteration
}
