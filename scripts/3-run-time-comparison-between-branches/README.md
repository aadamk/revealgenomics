Scripts in this folder allow us to compare the performance of different branches by examining the
run time of `search_biosamples()` and `search_expression()`

# A brief description of scripts in this folder

## `get_downloadTime_data.R`
- This script calls `search_biosamples()` and `search_expression()` and records the run time time for 
each of these calls.
- Log files generated by this script are saved at `./data/log_raw/` as `runTime_YYYYmmdd.txt`.
- Run time records are saved at `./data/raw/` as `runTime_YYYYmmdd.csv`
- For further details please refer to this script.

## `reshape data.R`
- This script modifies long form run time data written using `get_downloadTime_data.R` into a list 
with each element holding data for a particular study.
- For further details please refer to this script.

## `analyze_runTime_data.R`
- This script holds functions for analyzing run time data returned by reshape data.
- For further details please refer to this script.

## `plot_runTime_data.R`
- This script can be used to plot static or interactive graphs for comparing the run time of two branches.
- For further details please refer to this script.

# Folder structure

- This folder has scripts and data.
- Data is saved in the folder `./data`
- `./data` has the following structure -
  * `./data/raw` - this folder will contain/contains raw downloaded run time data
  * `./data/log_raw` - this folder will contain/contains log files generated while recording run time
  * `./data/processed` - this folder contains a processed/modified form of run time data

# Execution process

- Please create the following folders - `./data, ./data/raw, ./data/log_raw, ./data/processed`
- First, download run time data using `repeat_runs()` in `get_downloadTime_data.R` (follow naming 
conventions as detailed in this script).
- Second, reshape downloaded data using `reshape_data.R` (using naming conventions as detailed in 
this script).
- Now, for plotting run time data use script `plot_runTime_data.R` or for analyzing performance of 
a branch use functions in `analyze_runTime_data.R`