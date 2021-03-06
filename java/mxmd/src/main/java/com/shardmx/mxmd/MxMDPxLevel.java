package com.shardmx.mxmd;

import com.shardmx.mxbase.*;

public class MxMDPxLevel implements AutoCloseable {
  private MxMDPxLevel(long ptr) { this.ptr = ptr; }
  public void finalize() { close(); }
  public void close() {
    if (this.ptr != 0L) {
      dtor_(this.ptr);
      this.ptr = 0L;
    }
  }
  private native void dtor_(long ptr);

  // methods

  public native MxMDOBSide obSide();
  public native MxSide side();
  public native int pxNDP();
  public native int qtyNDP();
  public native long price();

  public native MxMDPxLvlData data();

  public native long allOrders(MxMDAllOrdersFn fn);

  @Override
  public String toString() {
    if (ptr == 0L) { return ""; }
    return new String()
      + "{side=" + side()
      + ", px=" + new MxValNDP(price(), pxNDP())
      + ", data=" + data().toString_(qtyNDP())
      + "}";
  }

  // data members

  private long ptr;
}
