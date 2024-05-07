//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

/* ZmPQueue unit test */

#include <zlib/ZuLib.hh>

#include <stdio.h>
#include <stdlib.h>

#include <zlib/ZuObject.hh>
#include <zlib/ZuTraits.hh>
#include <zlib/ZuTuple.hh>

#include <zlib/ZmPQueue.hh>
#include <zlib/ZmNoLock.hh>

using Msg_Data = ZuTuple<uint32_t, unsigned>;
struct Msg : public ZuObject, public Msg_Data {
  using Msg_Data::Msg_Data;
  using Msg_Data::operator =;
  Msg(const Msg_Data &v) : Msg_Data(v) { }
  Msg(Msg_Data &&v) : Msg_Data(ZuMv(v)) { }
  uint32_t key() const { return p<0>(); }
  unsigned length() const { return p<1>(); }
  unsigned clipHead(unsigned length) {
    p<0>() += length;
    return p<1>() -= length;
  }
  unsigned clipTail(unsigned length) {
    return p<1>() -= length;
  }
  template <typename I>
  void write(const I &) { }
  unsigned bytes() const { return 1; }
};

using PQueue =
  ZmPQueue<Msg,
    ZmPQueueNode<Msg,
      ZmPQueueBits<1,
	ZmPQueueLevels<4> > > >;

using QMsg = PQueue::Node;

void head(PQueue &q, uint32_t seqNo)
{
  printf("head %u\n", (unsigned)seqNo);
  q.head(seqNo);
}
void dequeue(PQueue &q)
{
  while (ZmRef<QMsg> msg = q.dequeue())
    printf("process %u, %u\n", (unsigned)msg->Msg::key(), msg->length());
}
void enqueue(PQueue &q, uint32_t seqNo, unsigned length)
{
  printf("send %u, %u\n", (unsigned)seqNo, length);
  ZmRef<QMsg> msg = q.rotate(ZmRef<QMsg>(new QMsg(ZuFwdTuple(seqNo, length))));
  printf("send - head %u gap %u, %u\n", (unsigned)q.head(), (unsigned)q.gap().key(), (unsigned)q.gap().length());
  while (msg) {
    printf("send - process %u, %u\n", (unsigned)msg->Msg::key(), msg->length());
    msg = q.dequeue();
    printf("send - head %u gap %u, %u\n", (unsigned)q.head(), (unsigned)q.gap().key(), (unsigned)q.gap().length());
  }
}

int main()
{
  PQueue q(1);

  enqueue(q, 1, 1);
  enqueue(q, 2, 2);
  enqueue(q, 4, 1);

  enqueue(q, 7, 1);
  enqueue(q, 8, 2);
  enqueue(q, 7, 3); // completely overlaps, should be fully clipped (ignored)
  enqueue(q, 9, 2); // should be head-clipped
  enqueue(q, 12, 2);
  enqueue(q, 10, 3); // should be head- and tail-clipped
  enqueue(q, 6, 3); // should be tail-clipped

  enqueue(q, 4, 3); // should be head- and tail-clipped, trigger dequeue

  enqueue(q, 15, 1);
  assert(q.gap().equals(ZuFwdTuple(14, 1)));
  enqueue(q, 17, 1);
  enqueue(q, 19, 1);
  enqueue(q, 21, 3);
  enqueue(q, 14, 8); // should overwrite 15,17,19 and be clipped by 21

  enqueue(q, 28, 1);
  enqueue(q, 27, 3); // should overwrite 28
  enqueue(q, 24, 10); // should overwrite 27

  head(q, 1);

  enqueue(q, 2, 1);
  enqueue(q, 3, 1);
  enqueue(q, 5, 1);
  enqueue(q, 7, 1);
  enqueue(q, 8, 2);
  enqueue(q, 10, 1);
  enqueue(q, 11, 3);

  head(q, 12); // should leave 12+2 in place
  enqueue(q, 15, 1);
  assert(q.gap().equals(ZuFwdTuple(14, 1)));
  dequeue(q);
  assert(q.gap().equals(ZuFwdTuple(14, 1)));
  enqueue(q, 14, 1);
}
