#!/bin/bash

# List all JSON files in the current directory
echo "The following .json files will be removed:"
ls *.json 2>/dev/null

# Confirm removal
read -p "Do you want to proceed? (y/n): " confirm

if [[ "$confirm" == "y" ]]; then
  # Remove all JSON files
  rm -f *.json
  echo "All .json files have been removed."
else
  echo "Operation canceled."
fi
