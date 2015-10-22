#!/bin/bash
for p in `seq 0.0 0.1 1.0`; do
  cat logs/baseline-32-4-$p | grep Sum | awk "{print $p,\$6}"
done
