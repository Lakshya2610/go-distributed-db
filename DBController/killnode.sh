#!/bin/bash
ssh -o StrictHostKeyChecking=no $1 "rm -rf /virtual/guptalak/ && pkill DBNode"
