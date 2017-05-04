#!/bin/sh
cd ~

mkdir final_project
cd final_project
mkdir csvfiles

Python ~/final_project/get_teams.py
python ~/final_project/get_games.py 2015
python ~/final_project/get_games.py 2016
python ~/final_project/get_games.py 2017

python ~/final_project/get_boxscores1.py 2015
python ~/final_project/get_boxscores1.py 2016
python ~/final_project/get_boxscores1.py 2017
python ~/final_project/cleanse.py 2017

python ~/final_project/today_games.py

