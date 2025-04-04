#!/bin/bash
#SBATCH --job-name=problem3_titanic
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=4
#SBATCH --time=01:00:00
#SBATCH --mem=16G
#SBATCH --output=problem3_output.txt
#SBATCH --error=problem3_error.txt

# Load required modules
module load devel/Spark/3.5.4-foss-2023b-Java-17

# Run the PySpark script
spark-submit --master local[4] /home/users/fdollaku/Big_Data_Analytics/problem3.py
