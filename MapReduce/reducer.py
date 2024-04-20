#!/usr/bin/env python3
import sys
import io

input_stream = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
output_stream = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

for line in input_stream:
    key, count = line.strip().split('\t', 1)
    print(f"{key}\t{count}", file=output_stream)
