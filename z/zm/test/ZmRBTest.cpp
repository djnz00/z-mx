//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=l1,g0,N-s,j1,U1,i4

/*
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Library General Public
 * License as published by the Free Software Foundation; either
 * version 2 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */

/* red/black tree test program */

#include <zlib/ZuLib.hpp>

#include <stdio.h>
#include <stdlib.h>

#include <time.h>

#include <zlib/ZuCmp.hpp>
#include <zlib/ZmRef.hpp>
#include <zlib/ZmObject.hpp>
#include <zlib/ZmRBTree.hpp>
#include <zlib/ZmNoLock.hpp>
#include <zlib/ZmAssert.hpp>

struct Z : public ZmObject {
  Z(int z) : m_z(z) { }

  struct Traits : public ZuBaseTraits<Z> {
    enum { IsReal = 0, IsPrimitive = 0, IsPOD = 1 };
  };
  friend Traits ZuTraitsType(Z *);

  int m_z;
};

template <typename>
struct ZCmp {
  static int cmp(Z *z1, Z *z2) {
    return z1->m_z - z2->m_z;
  }
  static ZmRef<Z> null() { 
    static ZmRef<Z> tmp = new Z(0);
    return tmp; 
  }
};

using Tree = ZmRBTree<ZmRef<Z>, ZmRBTreeCmp<ZCmp> >;

static void delptr(Tree *tree, Z *z) {
  tree->del(z);
#if 0
  auto iter = tree->iterator<ZmRBTreeEqual>(z);
  Tree::NodeRef node;

  while (node = iter.iterate()) if (z == node->key()) { iter.del(); return; }
#endif
}

int main()
{
  Tree tree;
  ZmRef<Z> z;
  int i;

  for (i = 0; i < 20; i++) tree.add(ZmRef<Z>(new Z(i)));

  fputs("0 to 19: ", stdout);
  {
    auto iter = tree.iterator();
    Tree::NodeRef node;

    while (node = iter.iterate())
      printf("%d ", node->key()->m_z);
  }
  putchar('\n');

  printf("min: %d, max: %d\n", tree.minimum()->key()->m_z, tree.maximum()->key()->m_z);

  for (i = 0; i < 20; i += 2) { ZmRef<Z> z = new Z(i); tree.del(z); }

  fputs("17 to 1, odd: ", stdout);
  {
    auto iter = tree.iterator<ZmRBTreeLess>(tree.maximumKey());
    Tree::NodeRef node;

    while (node = iter.iterate())
      printf("%d ", node->key()->m_z);
  }
  putchar('\n');

  printf("min: %d, max: %d\n", tree.minimum()->key()->m_z, tree.maximum()->key()->m_z);

  i = 6;

  fputs("7 to 19, odd: ", stdout);
  {
    ZmRef<Z> iz = new Z(i);
    auto iter = tree.iterator<ZmRBTreeGreater>(iz);
    Tree::NodeRef node;

    while (node = iter.iterate())
      printf("%d ", node->key()->m_z);
  }
  putchar('\n');

  printf("min: %d, max: %d\n", tree.minimum()->key()->m_z, tree.maximum()->key()->m_z);

  i = 7;

  fputs("1 to 7, odd: ", stdout);
  {
    ZmRef<Z> iz = new Z(i);
    auto iter = tree.iterator<ZmRBTreeLessEqual>(iz);
    Tree::NodeRef node;

    while (node = iter.iterate())
      printf("%d ", node->key()->m_z);
  }
  putchar('\n');

  printf("min: %d, max: %d\n", tree.minimum()->key()->m_z, tree.maximum()->key()->m_z);

  tree.clean();

  for (i = 0; i < 40; i++) tree.add(ZmRef<Z>(new Z(i)));
  for (i = 0; i < 20; i++) tree.del(ZmRef<Z>(new Z(i)));

  fputs("20 to 39 #1: ", stdout);
  {
    auto iter = tree.iterator();
    Tree::NodeRef node;

    while (node = iter.iterate())
      printf("%d ", node->key()->m_z);
  }
  putchar('\n');

  tree.clean();

  for (i = 0; i < 40; i++) tree.add(ZmRef<Z>(new Z(i)));
  for (i = 40; --i >= 20;) tree.del(ZmRef<Z>(new Z(i)));

  fputs("0 to 19 #1: ", stdout);
  {
    auto iter = tree.iterator();
    Tree::NodeRef node;

    while (node = iter.iterate())
      printf("%d ", node->key()->m_z);
  }
  putchar('\n');

  tree.clean();

  for (i = 40; --i >= 0;) tree.add(ZmRef<Z>(new Z(i)));
  for (i = 0; i < 20; i++) tree.del(ZmRef<Z>(new Z(i)));

  fputs("20 to 39 #2: ", stdout);
  {
    auto iter = tree.iterator();
    Tree::NodeRef node;

    while (node = iter.iterate())
      printf("%d ", node->key()->m_z);
  }
  putchar('\n');

  tree.clean();

  for (i = 40; --i >= 0;) tree.add(ZmRef<Z>(new Z(i)));
  for (i = 40; --i >= 20;) tree.del(ZmRef<Z>(new Z(i)));

  fputs("0 to 19 #2: ", stdout);
  {
    auto iter = tree.iterator();
    Tree::NodeRef node;

    while (node = iter.iterate())
      printf("%d ", node->key()->m_z);
  }
  putchar('\n');

  tree.clean();

  for (i = 0; i < 40; i++) tree.add(ZmRef<Z>(new Z(i)));
  for (i = 0; i < 20; i += 2) tree.del(ZmRef<Z>(new Z(i)));
  for (i = 1; i < 20; i += 2) tree.del(ZmRef<Z>(new Z(i)));

  fputs("20 to 39 #3: ", stdout);
  {
    auto iter = tree.iterator();
    Tree::NodeRef node;

    while (node = iter.iterate())
      printf("%d ", node->key()->m_z);
  }
  putchar('\n');

  tree.clean();

  for (i = 0; i < 40; i++) tree.add(ZmRef<Z>(new Z(i)));
  for (i = 40; (i -= 2) >= 20;) tree.del(ZmRef<Z>(new Z(i)));
  for (i = 41; (i -= 2) >= 20;) tree.del(ZmRef<Z>(new Z(i)));

  fputs("0 to 19 #3: ", stdout);
  {
    auto iter = tree.iterator();
    Tree::NodeRef node;

    while (node = iter.iterate())
      printf("%d ", node->key()->m_z);
  }
  putchar('\n');

  tree.clean();

  for (i = 40; --i >= 0;) tree.add(ZmRef<Z>(new Z(i)));
  for (i = 0; i < 20; i += 2) tree.del(ZmRef<Z>(new Z(i)));
  for (i = 1; i < 20; i += 2) tree.del(ZmRef<Z>(new Z(i)));

  fputs("20 to 39 #4: ", stdout);
  {
    auto iter = tree.iterator();
    Tree::NodeRef node;

    while (node = iter.iterate())
      printf("%d ", node->key()->m_z);
  }
  putchar('\n');

  tree.clean();

  for (i = 40; --i >= 0;) tree.add(ZmRef<Z>(new Z(i)));
  for (i = 40; (i -= 2) >= 20;) tree.del(ZmRef<Z>(new Z(i)));
  for (i = 41; (i -= 2) >= 20;) tree.del(ZmRef<Z>(new Z(i)));

  fputs("0 to 19 #4: ", stdout);
  {
    auto iter = tree.iterator();
    Tree::NodeRef node;

    while (node = iter.iterate())
      printf("%d ", node->key()->m_z);
  }
  putchar('\n');

  tree.clean();

  for (i = 0; i < 40; i++) tree.add(ZmRef<Z>(new Z(i)));
  for (i = 0; i < 20; i += 3) tree.del(ZmRef<Z>(new Z(i)));
  for (i = 1; i < 20; i += 3) tree.del(ZmRef<Z>(new Z(i)));
  for (i = 2; i < 20; i += 3) tree.del(ZmRef<Z>(new Z(i)));

  fputs("20 to 39 #5: ", stdout);
  {
    auto iter = tree.iterator();
    Tree::NodeRef node;

    while (node = iter.iterate())
      printf("%d ", node->key()->m_z);
  }
  putchar('\n');

  tree.clean();

  for (i = 0; i < 40; i++) tree.add(ZmRef<Z>(new Z(i)));
  for (i = 40; (i -= 3) >= 20;) tree.del(ZmRef<Z>(new Z(i)));
  for (i = 41; (i -= 3) >= 20;) tree.del(ZmRef<Z>(new Z(i)));
  for (i = 42; (i -= 3) >= 20;) tree.del(ZmRef<Z>(new Z(i)));

  fputs("0 to 19 #5: ", stdout);
  {
    auto iter = tree.iterator();
    Tree::NodeRef node;

    while (node = iter.iterate())
      printf("%d ", node->key()->m_z);
  }
  putchar('\n');

  tree.clean();

  for (i = 40; --i >= 0;) tree.add(ZmRef<Z>(new Z(i)));
  for (i = 0; i < 20; i += 3) tree.del(ZmRef<Z>(new Z(i)));
  for (i = 1; i < 20; i += 3) tree.del(ZmRef<Z>(new Z(i)));
  for (i = 2; i < 20; i += 3) tree.del(ZmRef<Z>(new Z(i)));

  fputs("20 to 39 #6: ", stdout);
  {
    auto iter = tree.iterator();
    Tree::NodeRef node;

    while (node = iter.iterate())
      printf("%d ", node->key()->m_z);
  }
  putchar('\n');

  tree.clean();

  for (i = 40; --i >= 0;) tree.add(ZmRef<Z>(new Z(i)));
  for (i = 40; (i -= 3) >= 20;) tree.del(ZmRef<Z>(new Z(i)));
  for (i = 41; (i -= 3) >= 20;) tree.del(ZmRef<Z>(new Z(i)));
  for (i = 42; (i -= 3) >= 20;) tree.del(ZmRef<Z>(new Z(i)));

  fputs("0 to 19 #6: ", stdout);
  {
    auto iter = tree.iterator();
    Tree::NodeRef node;

    while (node = iter.iterate())
      printf("%d ", node->key()->m_z);
  }
  putchar('\n');

  tree.clean();

  {
    ZmRef<Z> zarray[40];
    int j;

    for (i = 0; i < 40; i++) { j = i>>2; tree.add(zarray[i] = new Z(j)); }

    fputs("0 to 9 with 4 duplicates: ", stdout);
    {
      auto iter = tree.iterator();
      Tree::NodeRef node;

      while (node = iter.iterate())
	printf("%d ", node->key()->m_z);
    }
    putchar('\n');

    for (i = 0; i < 10; i++) delptr(&tree, zarray[i<<2]);

    fputs("0 to 9 with 3 duplicates: ", stdout);
    {
      auto iter = tree.iterator();
      Tree::NodeRef node;

      while (node = iter.iterate())
	printf("%d ", node->key()->m_z);
    }
    putchar('\n');

    for (i = 0; i < 10; i++) delptr(&tree, zarray[(i<<2) + 1]);

    fputs("0 to 9 with 2 duplicates: ", stdout);
    {
      auto iter = tree.iterator();
      Tree::NodeRef node;

      while (node = iter.iterate())
	printf("%d ", node->key()->m_z);
    }
    putchar('\n');

    for (i = 0; i < 10; i++) delptr(&tree, zarray[(i<<2) + 2]);

    fputs("0 to 9 with 1 duplicate: ", stdout);
    {
      auto iter = tree.iterator();
      Tree::NodeRef node;

      while (node = iter.iterate())
	printf("%d ", node->key()->m_z);
    }
    putchar('\n');

    for (i = 0; i < 10; i++) delptr(&tree, zarray[(i<<2) + 3]);

    fputs("empty: ", stdout);
    {
      auto iter = tree.iterator();
      Tree::NodeRef node;

      while (node = iter.iterate())
	printf("%d ", node->key()->m_z);
    }
    putchar('\n');
  }

  {
    ZmRef<Z> zarray[40];
    int j;

    for (i = 0; i < 40; i++) { j = i>>2; tree.add(zarray[i] = new Z(j)); }

    fputs("0 to 9 with 4 duplicates: ", stdout);
    {
      auto iter = tree.iterator();
      Tree::NodeRef node;

      while (node = iter.iterate())
	printf("%d ", node->key()->m_z);
    }
    putchar('\n');

    for (i = 0; i < 10; i += 2)
      for (j = 0; j < 4; j++)
	tree.del(ZmRef<Z>(new Z(i)));
    for (i = 1; i < 10; i += 2) delptr(&tree, zarray[(i<<2)]);

    fputs("0 to 9, odd, with 3 duplicates: ", stdout);
    {
      auto iter = tree.iterator();
      Tree::NodeRef node;

      while (node = iter.iterate())
	printf("%d ", node->key()->m_z);
    }
    putchar('\n');

    i = 4;

    fputs("5 to 9, odd, with 3 duplicates: ", stdout);
    {
      ZmRef<Z> iz = new Z(i);
      auto iter = tree.iterator<ZmRBTreeGreater>(iz);
      Tree::NodeRef node;

      while (node = iter.iterate())
	printf("%d ", node->key()->m_z);
    }
    putchar('\n');

    for (i = 1; i < 10; i += 2) delptr(&tree, zarray[(i<<2) + 1]);

    fputs("0 to 9, odd, with 2 duplicates: ", stdout);
    {
      auto iter = tree.iterator();
      Tree::NodeRef node;

      while (node = iter.iterate())
	printf("%d ", node->key()->m_z);
    }
    putchar('\n');

    for (i = 1; i < 10; i += 2) delptr(&tree, zarray[(i<<2) + 2]);

    fputs("0 to 9, odd, with 1 duplicate: ", stdout);
    {
      auto iter = tree.iterator();
      Tree::NodeRef node;

      while (node = iter.iterate())
	printf("%d ", node->key()->m_z);
    }
    putchar('\n');

    for (i = 1; i < 10; i += 2) delptr(&tree, zarray[(i<<2) + 3]);

    fputs("empty: ", stdout);
    {
      auto iter = tree.iterator();
      Tree::NodeRef node;

      while (node = iter.iterate())
	printf("%d ", node->key()->m_z);
    }
    putchar('\n');
  }

  int j;

  for (i = 0, j = 1; i < 100; i += j, j++) tree.add(ZmRef<Z>(new Z(i)));
  for (i = 2, j = 1; i < 100; i += j, j += 2) tree.add(ZmRef<Z>(new Z(i)));
  for (i = 4, j = 1; i < 100; i += j, j += 3) tree.add(ZmRef<Z>(new Z(i)));
  for (i = 6, j = 1; i < 100; i += j, j += 4) tree.add(ZmRef<Z>(new Z(i)));
  for (i = 10, j = 1; i < 100; i += j, j += 5) tree.add(ZmRef<Z>(new Z(i)));

  i = 1;

  {
    ZmRef<Z> iz = new Z(i);
    Tree::NodeRef node = tree.find(iz);

    if (node) z = node->key(); else z = 0;
  }
  if (!z || z->m_z != 1) puts("find() test failed");

  for (i = 0, j = 1; i < 100; i += j, j++) tree.del(ZmRef<Z>(new Z(i)));
  for (i = 2, j = 1; i < 100; i += j, j += 2) tree.del(ZmRef<Z>(new Z(i)));
  for (i = 4, j = 1; i < 100; i += j, j += 3) tree.del(ZmRef<Z>(new Z(i)));
  for (i = 6, j = 1; i < 100; i += j, j += 4) tree.del(ZmRef<Z>(new Z(i)));
  for (i = 10, j = 1; i < 100; i += j, j += 5) tree.del(ZmRef<Z>(new Z(i)));

  printf("zero object count: %d\n", tree.count_());

  for (i = 0, j = 1; i < 100; i += j, j++) tree.add(ZmRef<Z>(new Z(i)));
  for (i = 2, j = 1; i < 100; i += j, j += 2) tree.add(ZmRef<Z>(new Z(i)));
  for (i = 4, j = 1; i < 100; i += j, j += 3) tree.add(ZmRef<Z>(new Z(i)));
  for (i = 6, j = 1; i < 100; i += j, j += 4) tree.add(ZmRef<Z>(new Z(i)));
  for (i = 10, j = 1; i < 100; i += j, j += 5) tree.add(ZmRef<Z>(new Z(i)));

  for (i = 10, j = 1; i < 100; i += j, j += 5) tree.del(ZmRef<Z>(new Z(i)));
  for (i = 6, j = 1; i < 100; i += j, j += 4) tree.del(ZmRef<Z>(new Z(i)));
  for (i = 4, j = 1; i < 100; i += j, j += 3) tree.del(ZmRef<Z>(new Z(i)));
  for (i = 2, j = 1; i < 100; i += j, j += 2) tree.del(ZmRef<Z>(new Z(i)));
  for (i = 0, j = 1; i < 100; i += j, j++) tree.del(ZmRef<Z>(new Z(i)));

  printf("zero object count: %d\n", tree.count_());

  for (i = 0; i < 20; i++) tree.add(ZmRef<Z>(new Z(i)));

  fputs("0 to 19, deleting all elements: ", stdout);
  {
    auto iter = tree.iterator();
    Tree::NodeRef node;

    while (node = iter.iterate()) {
      printf("%d ", node->key()->m_z);
      iter.del(node);
    }
  }
  putchar('\n');

  printf("zero object count: %d\n", tree.count_());

  for (i = 0; i < 20; i++) tree.add(ZmRef<Z>(new Z(i)));

  fputs("0 to 19, deleting odd elements: ", stdout);
  {
    auto iter = tree.iterator();
    Tree::NodeRef node;

    while (node = iter.iterate()) {
      printf("%d ", (z = node->key())->m_z);
      if (z->m_z & 1) iter.del(node);
    }
  }
  putchar('\n');

  fputs("0 to 18, even: ", stdout);
  {
    auto iter = tree.iterator();
    Tree::NodeRef node;

    while (node = iter.iterate())
      printf("%d ", node->key()->m_z);
  }
  putchar('\n');

  printf("min: %d, max: %d\n", tree.minimum()->key()->m_z, tree.maximum()->key()->m_z);

  tree.clean();

  for (i = 0; i < 60; i++) { int j = i / 3; tree.add(ZmRef<Z>(new Z(j))); }

  fputs("0 to 19 with 3 duplicates, deleting every fourth element: ", stdout);
  {
    auto iter = tree.iterator();
    Tree::NodeRef node;
    int j = 0;

    while (node = iter.iterate()) {
      printf("%d ", node->key()->m_z);
      if (!(j & 3)) iter.del(node);
      j++;
    }
  }
  putchar('\n');

  i = 20;

  fputs("0 to 19 reverse order, remaining duplicates: ", stdout);
  {
    ZmRef<Z> iz = new Z(i);
    auto iter = tree.iterator<ZmRBTreeLess>(iz);
    Tree::NodeRef node;

    while (node = iter.iterate())
      printf("%d ", node->key()->m_z);
  }
  putchar('\n');

  printf("min: %d, max: %d\n", tree.minimum()->key()->m_z, tree.maximum()->key()->m_z);

  tree.clean();

  for (i = 0; i < 60; i++) { int j = i / 3; tree.add(ZmRef<Z>(new Z(j))); }

  i = 20;

  fputs("0 to 19 with 3 duplicates reverse order, deleting every fourth element: ", stdout);
  {
    ZmRef<Z> iz = new Z(i);
    auto iter = tree.iterator<ZmRBTreeLess>(iz);
    Tree::NodeRef node;
    int j = 0;

    while (node = iter.iterate()) {
      printf("%d ", node->key()->m_z);
      if (!(j & 3)) iter.del(node);
      j++;
    }
  }
  putchar('\n');

  fputs("0 to 19, remaining duplicates: ", stdout);
  {
    auto iter = tree.iterator();
    Tree::NodeRef node;

    while (node = iter.iterate())
      printf("%d ", node->key()->m_z);
  }
  putchar('\n');

  printf("min: %d, max: %d\n", tree.minimum()->key()->m_z, tree.maximum()->key()->m_z);

  tree.clean();

  fputs("empty tree: ", stdout);
  {
    auto i = tree.iterator();
    while (auto node = i.iterate()) printf("%d ", node->key()->m_z);
  }
  putchar('\n');

  {
    ZmRBTree<uint64_t> tree2;
    uint64_t add[] = {
      0x7fd2c4296790, 0x7fd2c4296800, 0x7fd2c4296870, 0x7fd2c42a2f80,
      0x7fd2c42975d0, 0x7fd2c4297640, 0x7fd2c429a870, 0x7fd2c42a2490,
      0x7fd2c42a2500, 0x7fd2c42a2570, 0x7fd2c4295a00, 0x7fd230005610,
      0x7fd2300056b0, 0x7fd230005c70, 0x7fd230005d70
    };
    uint64_t del[] = {
      0x7fd2c4296870, 0x7fd2c4296800, 0x7fd2c4296790, 0x7fd2c4297640,
      0x7fd2c42975d0, 0x7fd2c42a2f80, 0x7fd2c42a2500, 0x7fd2c42a2490,
      0x7fd2c429a870, 0x7fd2c4295a00, 0x7fd2c42a2570, 0x7fd230005610
    };
    for (unsigned i = 0; i < 15; i++) tree2.add(add[i]);
    for (unsigned i = 0; i < 12; i++) ZmAssert(tree2.del(del[i]));
    printf("tree2 count: %u\n", (unsigned)tree2.count_());
  }
}
