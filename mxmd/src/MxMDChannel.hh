//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// MxMD library internal API

#ifndef MxMDChannel_HH
#define MxMDChannel_HH

#ifndef MxMDLib_HH
#include <mxmd/MxMDLib.hh>
#endif

#include <zlib/ZuInt.hh>
#include <zlib/ZuBox.hh>

#include <zlib/ZiIP.hh>

#include <mxbase/MxBase.hh>
#include <mxbase/MxCSV.hh>

struct MxMDChannel {
  MxID			id;
  MxBool		enabled;
  ZuBox_1(int)		shardID;
  ZiIP			tcpIP, tcpIP2;
  ZuBox0(uint16_t)	tcpPort, tcpPort2;
  MxIDString		tcpUsername, tcpPassword;
  ZiIP			udpIP, udpIP2;
  ZuBox0(uint16_t)	udpPort, udpPort2;
  ZiIP			resendIP, resendIP2;
  ZuBox0(uint16_t)	resendPort, resendPort2;
};

class MxMDChannelCSV : public ZvCSV, public MxCSV<MxMDChannelCSV> {
public:
  typedef MxMDChannel Data;
  typedef ZuPOD<Data> POD;

  template <typename App = MxCSVApp>
  MxMDChannelCSV(App *app = 0) {
    new ((m_pod = new POD())->ptr()) Data{};
#ifdef Offset
#undef Offset
#endif
#define Offset(x) offsetof(Data, x)
    add(new MxIDCol("id", Offset(id)));
    add(new MxBoolCol("enabled", Offset(enabled), -1, 1));
    add(new MxIntCol("shardID", Offset(shardID)));
    add(new MxIPCol("tcpIP", Offset(tcpIP)));
    add(new MxPortCol("tcpPort", Offset(tcpPort)));
    add(new MxIPCol("tcpIP2", Offset(tcpIP2)));
    add(new MxPortCol("tcpPort2", Offset(tcpPort2)));
    add(new MxIDStrCol("tcpUsername", Offset(tcpUsername)));
    add(new MxIDStrCol("tcpPassword", Offset(tcpPassword)));
    add(new MxIPCol("udpIP", Offset(udpIP)));
    add(new MxPortCol("udpPort", Offset(udpPort)));
    add(new MxIPCol("udpIP2", Offset(udpIP2)));
    add(new MxPortCol("udpPort2", Offset(udpPort2)));
    add(new MxIPCol("resendIP", Offset(resendIP)));
    add(new MxPortCol("resendPort", Offset(resendPort)));
    add(new MxIPCol("resendIP2", Offset(resendIP2)));
    add(new MxPortCol("resendPort2", Offset(resendPort2)));
#undef Offset
  }

  void alloc(ZuRef<ZuAnyPOD> &pod) { pod = m_pod; }

  template <typename File>
  void read(File &&file, ZvCSVReadFn fn) {
    ZvCSV::readFile(ZuFwd<File>(file),
	ZvCSVAllocFn::Member<&MxMDChannelCSV::alloc>::fn(this), fn);
  }

  ZuInline POD *pod() { return m_pod.ptr(); }
  ZuInline Data *ptr() { return m_pod->ptr(); }

private:
  ZuRef<POD>	m_pod;
};

#endif /* MxMDChannel_HH */
