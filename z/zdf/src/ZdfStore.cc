//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed under the MIT license (see LICENSE for details)

// Data Series Manager

#include <zlib/ZdfStore.hh>

using namespace Zdf::Store_;

// default implementation overridden by FileStore, MockStore, etc.

Interface::~Interface() { final(); }

void Interface::final() { BufMgr::final(); }
