#!/bin/bash
export PYTHONPATH=$0/src:$PYTHONPATH
cd "$(dirname "$0")"
python -m src.main