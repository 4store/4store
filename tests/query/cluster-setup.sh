#!/usr/bin/env bash
4s-cluster-stop cluster_test
4s-cluster-create cluster_test --segments 8
4s-cluster-start cluster_test
4s-import -v cluster_test -m http://example.com/swh.xrdf ../../data/swh.xrdf -m http://example.com/TGR06001.nt ../../data/tiger/TGR06001.nt  -m http://example.com/nasty.ttl ../../data/nasty.ttl
4s-delete-model cluster_test http://example.com/nasty.ttl
4s-cluster-stop cluster_test
