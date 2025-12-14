python -m pytest ./tests/ -v
python3 main.py

# Run main with json config
python main_with_json_config.py config_parallel.json
python main_with_json_config.py config_sequential.json