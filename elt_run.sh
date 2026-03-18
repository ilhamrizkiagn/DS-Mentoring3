#!/bin/bash

echo "========== Start Orcestration Process =========="

# Virtual Environment Path
source "/home/ilham/miniconda3/etc/profile.d/conda.sh"

# Activate Virtual Environment
conda activate ds-mentoring3

# Set Python script
PYTHON_SCRIPT="/mnt/d/belajar_pacmann/data_storage/mentoring3/elt_pipeline.py"

# Run Python Script 
python3 "$PYTHON_SCRIPT"


echo "========== End of Orcestration Process =========="