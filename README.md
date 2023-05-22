# MoniterUtils
Some Python scripts for monitoring DL tasks

## database_monitor_util.py
A Python script for monitoring database changes, but only for specific monitoring.

SQLAlchemy is 1.x version, if you want to use 2.x version, you may need to migrate this script.

Before running the script, you need to modify Line 119-122, Line 241, and ADD your own listener manually.

## task_monitor_util.py
A simple streamlit app for monitoring your own task, all the monitor is handled by your own commands.

Just `pip install streamlit` should be fine, as there only some simple usage.

Before running the app, you need to check this script content first, and write your own things.

`streamlit run task_monitor_util.py` can run this. For more options, please check `streamlit --help`.
