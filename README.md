# IDS568 Milestone 4 - kalam5

## Setup

python3 -m venv venv  
source venv/bin/activate  
pip install -r requirements.txt  

## Generate Data

python3 generate_data.py --rows 1000 --output data  
python3 generate_data.py --rows 10000000 --output big_data  

## Run Pipeline

python3 pipeline.py --input data --output output  
python3 pipeline.py --input big_data --output big_output  

## Reproducibility

python3 generate_data.py --rows 100 --seed 42 --output run1  
python3 generate_data.py --rows 100 --seed 42 --output run2  