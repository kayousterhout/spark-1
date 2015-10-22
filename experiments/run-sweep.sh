#!/bin/bash
for p in `seq 0.0 0.1 1.0`; do
  bin/run-example DrizzleBaseline 32 4 $p | tee logs/baseline-32-4-$p
done
