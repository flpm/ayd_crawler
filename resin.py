##############################################################################
# Browse AYD website to collect game info and download SGF files             #
##############################################################################
#
#  Felipe Moreno
#  version 1
#  Oct 2016
#
##############################################################################

from bs4 import BeautifulSoup
from urllib2 import urlopen
from urllib import urlretrieve
import re
import psycopg2
import sys
from datetime import datetime
import time
import glob

##########
# CONFIG #
##########

SEASON = "season16"
YD_SCHOOL = "AYD" # for the player profile, based on scrapped URL (game and tournament comes from tournament name)
BASE_AYD_URL = "http://ayd.yunguseng.com"
BASE_AYD_SEASON_URL = BASE_AYD_URL +  '/' + SEASON 

AYD_RATING_URL = "http://ayd.yunguseng.com/rating.html"

DOWNLOAD_GAMES = True
GAME_FOLDER = "./ayd_games/"
WAIT_TIME = 1

DATABASE_NAME = "ayd"
DATABASE_HOST = 'localhost'
USERNAME = "felipe"
PASSWORD = None
RECREATE_DB = False
DEBUG_MODE = True

#############
# FUNCTIONS #
#############

# Parse the ratings page
def parse_ratings_page(rating_page_url):

	# download html and parse
	html = urlopen(rating_page_url).read()
	soup = BeautifulSoup(html, "lxml")

	# get rating table
	rating_table = soup.find("table", attrs={"id": "tablesorter-demo2"})

	players = []
	row_count = 0

	for row in rating_table.find_all("tr"):

		if row_count == 0:
			row_count += 1
			continue

		this_player = {}
		cells = row.find_all("td");

		this_player['school'] = YD_SCHOOL
		this_player['nick'] = cells[3].get_text()
		this_player['name'] = cells[2].find("a", href=True).get_text()
		this_player['active'] = re.match('.+\(P\)', cells[2].get_text()) is not None
		this_player['profile_link'] = BASE_AYD_URL + cells[2].find("a", href=True)['href']

		global BASE_AYD_SEASON_URL
		BASE_AYD_SEASON_URL = this_player['profile_link'].split("/profile")[0] 

		this_player['rating'] = cells[4].get_text()
		this_player['check_time'] = datetime.now()

		players.append(this_player)

	return players


# Parse a player profile page
def parse_player_profile_page(profile_url, download_games = False):

	month_to_digit = {'January':1, 'February':2, 'March':3, 'April':4, 'May':5, 'June':6, 'July':7, 'August':8, 'September':9, 'October':10, 'November':11, 'December':12}

	def expand_color(x):
		if(x.lower() == "b"):
			return "black"
		elif(x.lower() == "w"):
			return "white"
		return None

	html = urlopen(profile_url).read()
	soup = BeautifulSoup(html, "lxml")

	# get player name
	player_name = soup.find_all('h1')[1].get_text().split(" aka ")[1]

	# get game table
	table = soup.find_all("table", attrs={"class": "graytable"})[1]

	tournaments = {}
	all_games = []
	row_count = 0

	for row in table.find_all("tr"):

		if row_count == 0:
			row_count += 1
			continue

		game = {}

		cells = row.find_all("td");
		game['school'] = YD_SCHOOL
		game['gamed_index'] = cells[0].get_text()
		game['tournament'] = cells[1].get_text()
		tournament_temp =  cells[1].find("a", href=True)
		if tournament_temp is not None:
			game['tournament_link'] = BASE_AYD_URL + cells[1].find("a", href=True)['href']
			game['tournament_id'] = int(game['tournament_link'].split("id=")[1])
		else:
			game['tournament_link'] = None
			game['tournament_id'] = None

		tournament_details = re.match(r'^(AYD|EYD)? ?League (\w), (\w+) (\d+)$', game['tournament'])

		if tournament_details:
			game['school'] = tournament_details.group(1)
			if game['school'] is None:
				game['school'] = "EYD"
			game['league'] = tournament_details.group(2)
			game['month'] = month_to_digit[tournament_details.group(3)]
			game['year'] = tournament_details.group(4)
		else:
			game['school'] = None
			game['league'] = None
			game['month'] = None
			game['year'] = None

		if game['tournament_link'] is not None:
			game['season'] = int(game['tournament_link'].split("/season")[1].split("/")[0])
		else:
			game['season'] = None

		game['round'] = cells[2].get_text()

		# regular game row
		if(game['round'] in ["1","2","3","4","5"]):
			game['white'] = cells[3].get_text()
			game['black'] = cells[4].get_text()
			game['result'] = cells[5].get_text()
			result = game['result'].split("+")
			game['win_score'] = result[1].lower()
			game['win_color'] = expand_color(result[0])
			if(game['win_color'] == "black"):
				game['win_player'] =  game['black']
			else:
				game['win_player'] = game['white']
			if(game['win_score'] != "forfeit"):
				game['game_link'] = BASE_AYD_SEASON_URL + "/" + cells[5].find("a", href=True)['href']
			else:
				game['game_link'] = None
			game['sgf_filename'] =  game['white'] + "_" + game['black'] + "_" + str(game['tournament_id']) + ".sgf"

			game['rating'] = cells[6].get_text()
			game['change'] = cells[7].get_text()

		# initial rate set or rate changing row
		else: 
			game['round'] = None
			game['gamed_index'] = None
			game['sgf_filename'] = None
			game['white'] = None
			game['black'] = None
			game['result'] = None
			game['win_player'] = None
			game['win_score'] = None
			game['win_color'] = None
			game['game_link'] = None
			game['rating'] = cells[4].get_text()
			game['change'] = None

		all_games.append(game)

	return(all_games)

## DB functions

def create_DB_structure():
	db_script = "BEGIN TRANSACTION;\n"
	db_script += "DROP TABLE IF EXISTS \"tournaments\";\n"
	db_script += "CREATE TABLE \"tournaments\"(" \
				 "school TEXT, " \
				 "season SMALLINT, " \
				 "tournament TEXT, " \
				 "league TEXT, " \
				 "month SMALLINT, " \
				 "year SMALLINT, " \
	             "tournament_link TEXT, " \
	             "tournament_id SMALLINT primary key);\n"

	db_script += "DROP TABLE IF EXISTS \"players\";\n"
	db_script += "CREATE TABLE \"players\"(" \
				 "school TEXT, " \
				 "name TEXT, " \
	             "nick TEXT primary key, " \
	             "active BOOLEAN, " \
	             "rating TEXT, " \
	             "profile_link TEXT, "\
	             "check_time TEXT);\n"

	db_script += "DROP TABLE IF EXISTS \"games\";\n"
	db_script += "CREATE TABLE \"games\"(" \
				 "school TEXT, " \
				 "season SMALLINT, " \
				 "tournament_id SMALLINT, " \
				 "league TEXT, " \
				 "month SMALLINT, " \
				 "year SMALLINT, " \
	             "round SMALLINT, " \
	             "white TEXT, " \
	             "black TEXT, " \
	             "result TEXT, " \
	             "win_color TEXT, "\
	             "win_player TEXT, "\
	             "win_score TEXT, "\
	             "game_link TEXT, "\
	             "sgf_filename TEXT, "\
	             "PRIMARY KEY(school, tournament_id, round, white, black));\n"	
	db_script += "COMMIT;"

	dbcur = db_con.cursor()
	dbcur.execute(db_script)
	db_con.commit()

def insert_game(game_record):
	db_cur = db_con.cursor()
	db_query = "INSERT INTO games VALUES(%(school)s, %(season)s, %(tournament_id)s, %(league)s, %(month)s, %(year)s, %(round)s, %(white)s, %(black)s, " \
				"%(result)s, %(win_color)s, %(win_player)s, %(win_score)s, %(game_link)s, %(sgf_filename)s);"
	res = db_cur.execute(db_query, game_record)
	db_con.commit()

def insert_tournament(tournament_record):
	db_cur = db_con.cursor()
	db_query = "INSERT INTO tournaments VALUES(%(school)s, %(season)s, %(tournament)s, %(league)s, %(month)s, %(year)s, %(tournament_link)s, %(tournament_id)s);"
	res = db_cur.execute(db_query, tournament_record)
	db_con.commit()

def insert_player(player):
	db_cur = db_con.cursor()
	db_query = "INSERT INTO players VALUES(%(school)s, %(name)s, %(nick)s, %(active)s, %(rating)s, " \
				"%(profile_link)s, %(check_time)s) ON CONFLICT (nick) DO UPDATE SET rating = excluded.rating, active = excluded.active, check_time = excluded.check_time;"
	res = db_cur.execute(db_query, player)
	db_con.commit()	


########
# MAIN #
########

# Open database connection
if(PASSWORD):
	db_con = psycopg2.connect("dbname=" + DATABASE_NAME + " user=" + USERNAME + " host=" + DATABASE_HOST + " password=" + PASSWORD)
else:
	db_con = psycopg2.connect("dbname=" + DATABASE_NAME + " user=" + USERNAME + " host=" + DATABASE_HOST)

if RECREATE_DB:
	if DEBUG_MODE:
		print "[starting] recreating tables in the database"
	create_DB_structure()


# load the players already in the database
existing_players = {}
db_cur = db_con.cursor()
db_cur.execute("SELECT nick, rating, active from players;")
existing_players_list = db_cur.fetchall()
if(len(existing_players_list) > 0):
	for row in existing_players_list:
		existing_players[row[0]] = {'rating': row[1], 'active': row[2]}


# Parse ratings page
players = parse_ratings_page(AYD_RATING_URL)
if DEBUG_MODE:
	print "[ratings] parsed ratings page, found %d players" % len(players)


# Check wich players rank have changed
players_to_update = []
for player in players:
	if player['nick'] in existing_players and player['rating'] == existing_players[player['nick']]['rating']:
		if DEBUG_MODE:
			print "[ratings] player %s rating has not changed since last check, skipping" % player['nick']
	else:
		players_to_update.append(player)
if DEBUG_MODE:
			print "[ratings] found %d players that require parsing profile page" % len(players_to_update)


# load the games already in the database
db_cur = db_con.cursor()
db_cur.execute("SELECT sgf_filename from games;")
processed_games_list = db_cur.fetchall()
if(len(processed_games_list) > 0):
	processed_games = [e for l in processed_games_list for e in l]
else:
	processed_games = []

# load sgf files already downloaded
games_already_downloaded = []
for sgf_file in sorted(glob.glob(GAME_FOLDER + "/*.sgf")):
	games_already_downloaded.append(sgf_file.split("/")[-1])
if DEBUG_MODE:
	print "[sgf files] found %d sgf files in %s" % (len(games_already_downloaded), GAME_FOLDER)

# load tournaments from the database
db_cur = db_con.cursor()
db_cur.execute("SELECT tournament_id from tournaments;")
tournaments_list = db_cur.fetchall()
if(len(processed_games_list) > 0):
	tournaments = [e for l in tournaments_list for e in l]
else:
	tournaments = []


# For each player in the ratings page where
#players_to_insert = []
#games_to_insert = []
player_count = 0

for player in players_to_update:
	games_processed = 0
	player_count += 1

	if DEBUG_MODE:
		print "[players] (%d of %d) parsing profile for player %s" % (player_count, len(players_to_update), player['nick'])

	if WAIT_TIME != 0:
		time.sleep(WAIT_TIME)

	player_games = parse_player_profile_page(player['profile_link'])

	for game in player_games:
		if game['round'] is None:
			continue
		if game['sgf_filename'] not in processed_games:

			if DOWNLOAD_GAMES and game['game_link'] is not None:
				if game['sgf_filename'] not in games_already_downloaded:
					if DEBUG_MODE:
						print "[download] fetching %s from %s" % (game['sgf_filename'], game['game_link'])
					if WAIT_TIME != 0:
						time.sleep(WAIT_TIME)
					urlretrieve(game['game_link'], GAME_FOLDER + game['sgf_filename'])
				else:
					if DEBUG_MODE:
						print "[download] found %s is already downloaded, skipping" % (game['sgf_filename'])
			

			if game['tournament_id'] not in tournaments:
				tournament_record = {
					'school': game['school'],
					'season': game['season'],
					'tournament_id': game['tournament_id'],
					'tournament': game['tournament'],
					'league': game['league'],
					'month': game['month'],
					'year': game['year'],
					'tournament_link': game['tournament_link']
				}
				if DEBUG_MODE:
					print "[tournament] inserting %r into the database" % (game['tournament'])
				insert_tournament(tournament_record)
				tournaments.append(game['tournament_id'])

			game_record = {
				'school': game['school'],
				'season': game['season'],
				'tournament_id': game['tournament_id'],
				'league': game['league'],
				'month': game['month'],
				'year': game['year'],
				'round': game['round'],
				'result': game['result'],
				'white': game['white'],
				'black': game['black'],
				'win_color': game['win_color'],
				'win_player': game['win_player'],
				'win_score': game['win_score'],
				'game_link': game['game_link'],
				'sgf_filename': game['sgf_filename']
			}	
			games_processed += 1			
			processed_games.append(game['sgf_filename'])
			#games_to_insert.append(game_record)
			if DEBUG_MODE:
				print "[game] inserting %s into the database" % (game['sgf_filename'])
			insert_game(game_record)
		else:
			if DEBUG_MODE:
				print "[%s] game: %s already processed, skipping" % (player['nick'], game['sgf_filename'])
	
	#if DEBUG_MODE:
		#print "[%s] game: %d games, %d to be inserted" % (player['nick'], len(player_games), games_processed)

	if DEBUG_MODE:
		print "[%s] inserting player info into the database" % player['nick']
	insert_player(player)
	#players_to_insert.append(player)









