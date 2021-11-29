package com.shardmx.mxmd;

public interface MxMDAllSegmentsFn {
  boolean fn(MxMDSegment segment);	// non-zero aborts iteration
}
