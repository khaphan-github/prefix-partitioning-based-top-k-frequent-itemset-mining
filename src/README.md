# Run
pip install -r requirments.txt
python3 main_with_json_config.py config_sequential.json
python3 main_with_json_config.py config_parallel.json
python3 main.py
# Run main with json config
python -m pytest ./tests/ -v