package com.shardmx.mxmd;

public interface MxMDAllFeedsFn {
  boolean fn(MxMDFeed feed);	// non-zero aborts iteration
}
