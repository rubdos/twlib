# twlib
A Python library that downloads the worlddata available on tribalwars. Tested on W41 on the Dutch TribalWars

# Usage
````
import twlib.tribalwarsORM as tw

world = tw.World.load("41", "tribalwars.nl")
# Now you can iterate over world.villages
# or world.players
# or world.tribes
# eg.
for player in world.players:
    print player.name
````

Please refer to the tribalwarsORM object specifications if you want to see what information can be fetched. Every object has references, which are managed in a local sqlite database. Eg.
````
for member in player.tribe.players:
    print member.name
````
will print every member in a tribe of a player.
````
for village in player.villages:
    print village.name
    # This village will also refer to the owner using village.player
````

The API is pretty much compatible with this library https://code.google.com/p/tribalwars/ , as I was using this before, but lacked the backreferences and implementing them would overhaul the whole library.
