// Copyright 2025 Patrick Steil
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package processors

import "fmt"

type UnionFind[T comparable] struct {
	parent  map[T]T
	rank    map[T]int
	size    map[T]int
	numSets int

	isPreferredParent map[T]bool
}

func NewUnionFind[T comparable]() *UnionFind[T] {
	return &UnionFind[T]{
		parent:            make(map[T]T),
		rank:              make(map[T]int),
		size:              make(map[T]int),
		numSets:           0,
		isPreferredParent: make(map[T]bool),
	}
}

func (uf *UnionFind[T]) InitKey(k T) {
	uf.parent[k] = k
	uf.rank[k] = 0
	uf.size[k] = 1

	uf.numSets += 1
}

func (uf *UnionFind[T]) MarkAsParent(key T) {
	uf.isPreferredParent[key] = true
}

func (uf *UnionFind[T]) FindSet(i T) T {
	if uf.parent[i] != i {
		uf.parent[i] = uf.FindSet(uf.parent[i])
	}
	return uf.parent[i]
}

func (uf *UnionFind[T]) IsSameSet(i, j T) bool {
	return uf.FindSet(i) == uf.FindSet(j)
}

func (uf *UnionFind[T]) UnionSet(x, y T) {
	xRoot := uf.FindSet(x)
	yRoot := uf.FindSet(y)

	if xRoot == yRoot {
		return
	}

	uf.numSets--

	if uf.isPreferredParent[xRoot] && !uf.isPreferredParent[yRoot] {
		uf.parent[yRoot] = xRoot
		uf.size[xRoot] += uf.size[yRoot]
	} else if uf.isPreferredParent[yRoot] && !uf.isPreferredParent[xRoot] {
		uf.parent[xRoot] = yRoot
		uf.size[yRoot] += uf.size[xRoot]
	} else {
		if uf.rank[xRoot] > uf.rank[yRoot] {
			uf.parent[yRoot] = xRoot
			uf.size[xRoot] += uf.size[yRoot]
		} else {
			uf.parent[xRoot] = yRoot
			uf.size[yRoot] += uf.size[xRoot]
			if uf.rank[xRoot] == uf.rank[yRoot] {
				uf.rank[yRoot]++
			}
		}
	}
}

func (uf *UnionFind[T]) NumDisjointSets() int {
	return uf.numSets
}

func (uf *UnionFind[T]) SizeOfSet(i T) int {
	return uf.size[uf.FindSet(i)]
}

func (uf *UnionFind[T]) Apply(f func(key T, parent T)) {
	for key := range uf.parent {
		parent := uf.FindSet(key)
		f(key, parent)
	}
}

func (uf *UnionFind[T]) Print() {
	uf.Apply(func(key, parent T) {
		fmt.Printf("%v -> %v\n", key, parent)
	})
}
