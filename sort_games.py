##############################################################################
# Organize the downloaded games into folders                                 #
##############################################################################
#
#  Felipe Moreno
#  version 1
#  Jan 2017
#
##############################################################################


import re
import psycopg2
import sys
import shutil
import glob
import os
import zipfile

##########
# CONFIG #
##########

GAME_FOLDER = "./ayd_games/"
OUTPUT_FOLDER = "./output/"

DATABASE_NAME = "ayd2"
DATABASE_HOST = 'localhost'
USERNAME = "felipe"
PASSWORD = None

DEBUG_MODE = True
ZIP_PER_SEASON = False

#############
# FUNCTIONS #
#############


########
# MAIN #
########

# Open database connection
if(PASSWORD):
	db_con = psycopg2.connect("dbname=" + DATABASE_NAME + " user=" + USERNAME + " host=" + DATABASE_HOST + " password=" + PASSWORD)
else:
	db_con = psycopg2.connect("dbname=" + DATABASE_NAME + " user=" + USERNAME + " host=" + DATABASE_HOST)

months = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

# load the tournaments from the database
tournaments_list = []
db_cur = db_con.cursor()
db_cur.execute("select season, league, month, year, tournament_id from ayd_tournaments where school = 'AYD';")
tournaments_list = db_cur.fetchall()

if DEBUG_MODE:
	print "[database] found %d tournaments in the database" % len(tournaments_list)

if len(tournaments_list) == 0:
	sys.exit('Cannot load tournaments from the database...')

tournaments_dic = {}
for item in tournaments_list:
	key = item[4]
	tournaments_dic[key] = item

# load the list of downloaded sgf files
games_list = []
for sgf_file in sorted(glob.glob(GAME_FOLDER + "/*.sgf")):
	games_list.append(sgf_file.split("/")[-1])
if DEBUG_MODE:
	print "[sgf files] found %d sgf files in %s" % (len(games_list), GAME_FOLDER)


for game in games_list:
	t_re = re.match(r'^.+_.+_(\d+).sgf$', game)
	if t_re:
		t_id = int(t_re.group(1))
		if t_id in tournaments_dic:
			to_filename = 'S%d_%s%d_%s_%s'% (tournaments_dic[t_id][0], months[tournaments_dic[t_id][2]], tournaments_dic[t_id][3], tournaments_dic[t_id][1],game)
			

			directory = OUTPUT_FOLDER + 'season_%d' % tournaments_dic[t_id][0]
			if not os.path.exists(directory):
				os.makedirs(directory)
			directory += '/%s%d' % (months[tournaments_dic[t_id][2]], tournaments_dic[t_id][3])
			if not os.path.exists(directory):
				os.makedirs(directory)
			directory += '/league_%s' %  tournaments_dic[t_id][1]
			if not os.path.exists(directory):
				os.makedirs(directory)

			shutil.copyfile(GAME_FOLDER + game, directory + "/" + to_filename)
		else:
			if DEBUG_MODE:
				print '[%s]: cannot find tournament %d in AYD list' % (game, t_id)


if ZIP_PER_SEASON:
	for folder in os.listdir(OUTPUT_FOLDER):
	    zipf = zipfile.ZipFile('{0}.zip'.format(os.path.join(OUTPUT_FOLDER, folder)), 'w', zipfile.ZIP_DEFLATED)
	    for root, dirs, files in os.walk(os.path.join(OUTPUT_FOLDER, folder)):
	    	print root
	    	print dirs
	    	print files
	        for filename in files:
	            zipf.write(os.path.abspath(os.path.join(root, filename)), arcname=root.split("./output/")[1] + "/" + filename)
	    zipf.close()





