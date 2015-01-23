import csv      # read in the data files
import urllib   # URL decode functions
import sys
import os, os.path
import time
import gzip
import copy
from optparse import OptionParser
from sqlobject import *
from sqlobject.sqlbuilder import *

TRIBES_FILE = "ally.txt.gz"
PLAYERS_FILE = "tribe.txt.gz"
VILLAGES_FILE = "village.txt.gz"
CONQUERS_FILE = "conquer.txt.gz"
PROFILES_FILE = "profile.txt.gz"
SECS_IN_DAY = 86400
DATA_DIR_PREFIX = "data/w"
CSV_DIR = "CSV/"

sqlhub.processConnection = connectionForURI("sqlite:" + os.path.abspath("tw.db")+"")
def chunks(l, n):
    """ Yield successive n-sized chunks from l.
    """
    for i in xrange(0, len(l), n):
        yield l[i:i+n]
class Village(SQLObject):
    name = StringCol()
    x = IntCol()
    y = IntCol()
    points = IntCol()
    rank = IntCol()
    player = ForeignKey('Player')
    world = ForeignKey('World')
class Player(SQLObject):
    name         = StringCol()#urllib.unquote_plus(data[1]) # url decoding
    tribe        = ForeignKey('Tribe')#None
    #num_villages = #int(data[3])
    points       = IntCol()#int(data[4])
    rank         = IntCol()#int(data[5])
    #area_of_influence = {"continent": "None", "quadrant": "None",
    #                              "num_villages": 0, "points": 0}
    name = StringCol()
    world = ForeignKey('World')
Player.sqlmeta.addJoin(MultipleJoin('Village', joinMethodName='villages'))
class Tribe(SQLObject):
    name = StringCol()
    tag = StringCol()
#    num_members = int(data[3]),
#    num_villages = int(data[4]),
    top_40_points = IntCol()
    total_points = IntCol()
    rank = IntCol()
    world = ForeignKey('World')
    pass
Tribe.sqlmeta.addJoin(MultipleJoin('Player', joinMethodName='players'))
class World(SQLObject):
    worldName = StringCol()
    
    @staticmethod
    def load(world, domain):
        q=World.select(World.q.worldName == world)
        if q.count() > 0:
            w = q[0]
        else:
            w = World(worldName = world)
        lifespan = 1/24.
        data_dir = DATA_DIR_PREFIX + world + "/"

        # check if the data directory exists
        if not os.path.isdir(data_dir):
            os.makedirs(data_dir)
            World.__download_data_files(data_dir, world, domain)
            w._load()

        # make sure all the data files are there
        if not os.path.isfile(data_dir + TRIBES_FILE) or not os.path.isfile(data_dir + PLAYERS_FILE) \
                or not os.path.isfile(data_dir + VILLAGES_FILE) or not os.path.isfile(data_dir + CONQUERS_FILE) \
                or not os.path.isfile(data_dir + PROFILES_FILE):
            World.__download_data_files(data_dir, world, domain)
            w._load()

        # check the time on the tribes file to see if we should re-download
        ctime = os.stat(data_dir + TRIBES_FILE).st_ctime
        

        if time.time() - ctime >= lifespan * SECS_IN_DAY: 
            World.__download_data_files(data_dir, world, domain)
            w._load()
        w.domain = domain
        return w

    def _load(self):
        connection = sqlhub.processConnection
        data_dir = DATA_DIR_PREFIX + self.worldName + "/"

        # Read Tribes
        file = csv.reader(gzip.open(data_dir + TRIBES_FILE, "rb"))

        print "Reading Tribes"
        values = []
        for data in file:
            values.append({
                'id': int(data[0]),
                'name': urllib.unquote_plus(data[1]),
                'tag': urllib.unquote_plus(data[2]),
#                num_members = int(data[3]),
#                num_villages = int(data[4]),
                'top_40_points': int(data[5]),
                'total_points': int(data[6]),
                'rank': int(data[7]),
                'world_id': self.id
                })
        for v in chunks(values, 500):
            insert = Insert('tribe', valueList=v)
            query = connection.sqlrepr(insert)
            connection.query(query)

        print "Reading players"
        # Read Players
        file = csv.reader(gzip.open(data_dir + PLAYERS_FILE, "rb"))

        values = []
        for data in file:
            values.append({
                'id': int(data[0]),
                'name': urllib.unquote_plus(data[1]), # url decoding,
                'tribe_id': int(data[2]),
#                num_villages: int(data[3]),
                'points': int(data[4]),
                'rank': int(data[5]),
                'world_id': self.id
#                area_of_influence = {"continent": "None", "quadrant": "None",
#                                  "num_villages": 0, "points": 0}
                })
        for v in chunks(values, 500):
            insert = Insert('player', valueList=v)
            query = connection.sqlrepr(insert)
            connection.query(query)
        
        print "Reading Villages, this can take a while"
        # Read villages
        file = csv.reader(gzip.open(data_dir + VILLAGES_FILE, "rb"))

        values = []
        for data in file:
            values.append({
                'world_id': self.id,
                'id':  int(data[0]),
                'name':  urllib.unquote_plus(data[1]), #url decoding,
                'x':  int(data[2]),
                'y':  int(data[3]),
                'player_id':  int(data[4]),
                'points':  int(data[5]),
                'rank':  int(data[6])
                })
        for v in chunks(values, 500):
            insert = Insert('village', valueList=v)
            query = connection.sqlrepr(insert)
            connection.query(query)
    @staticmethod 
    def __download_data_files(data_dir, world, domain):
        """ Download the 5 data files from the Tribal Wars server"""
        connection = sqlhub.processConnection
        for d in [Delete('player', where=None),
                Delete('tribe', where=None),
                Delete('village', where=None)]:
            query = connection.sqlrepr(d)
            sqlhub.processConnection.query(query)
        base_url = "http://" + world + "."+domain+"/map/"

        try:
            sys.stdout.write("\nDownloading ally.txt.gz...")
            sys.stdout.flush()
            urllib.urlretrieve(base_url + TRIBES_FILE, data_dir + TRIBES_FILE)
            urllib.urlcleanup()
            sys.stdout.write("Done\n")

            sys.stdout.write("Downloading tribe.txt.gz...")
            sys.stdout.flush()
            urllib.urlretrieve(base_url + PLAYERS_FILE, data_dir + PLAYERS_FILE)
            urllib.urlcleanup()
            sys.stdout.write("Done\n")

            sys.stdout.write("Downloading village.txt.gz...")
            sys.stdout.flush()
            urllib.urlretrieve(base_url + VILLAGES_FILE, data_dir + VILLAGES_FILE)
            urllib.urlcleanup() # clear the cache
            sys.stdout.write("Done\n")
        
            sys.stdout.write("Downloading conquer.txt.gz...")
            sys.stdout.flush()
            urllib.urlretrieve(base_url + CONQUERS_FILE, data_dir + CONQUERS_FILE)
            urllib.urlcleanup()
            sys.stdout.write("Done\n")

            sys.stdout.write("Downloading profile.txt.gz...")
            sys.stdout.flush()
            urllib.urlretrieve(base_url + PROFILES_FILE, data_dir + PROFILES_FILE)
            urllib.urlcleanup()
            sys.stdout.write("Done\n")
        except IOError:
            raise WorldError(world, "Invalid world number")

World.sqlmeta.addJoin(MultipleJoin('Village', joinMethodName='villages'))
World.sqlmeta.addJoin(MultipleJoin('Player', joinMethodName='players'))
World.sqlmeta.addJoin(MultipleJoin('Tribe', joinMethodName='tribes'))

World.createTable(ifNotExists = True)
Player.createTable(ifNotExists = True)
Tribe.createTable(ifNotExists = True)
Village.createTable(ifNotExists = True)

if __name__ == "__main__":
    print "Testing some stuff..."
    w = World.load("41")
    print "#villages in world", w.worldName, len(w.villages)
    print "#players in world", w.worldName, len(w.players)
