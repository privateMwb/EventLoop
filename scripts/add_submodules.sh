#!/bin/sh
# Adds the three dependency submodules under libs/internal/.
# Run once after `git init`; later clones use `git submodule update --init`.
set -e
for r in ThreadPoolPro FunctionPro PoolAllocator; do
    git submodule add "https://github.com/privateMwb/$r.git" "libs/internal/$r"
done
