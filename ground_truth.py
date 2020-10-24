import os
from collections import Counter
import re
import configparser

config=configparser.ConfigParser()
config.read('config.ini')


data = []
words = []
id = 1

input_loc=config['MAP_REDUCE']['INPUT_LOCATION']

for book in os.listdir(input_loc):
    with open(input_loc+book, encoding="utf8", errors="surrogateescape") as file:
        for word in file.read().split(" "):
            if re.fullmatch(r'[a-zA-Z]+', word):
                data.append((word, id))
                words.append(word)
    id += 1


w_gd = list(Counter(words).items())
print(w_gd[:10])
with open('word_count_gd.txt', 'a+') as file:
    for key, value in w_gd:
        file.write(key+"       "+str(value)+'\n')


store = dict()
for key, value in data:
    if key in store:
        store[key].add(value)
    else:
        store[key] = {value}

for key in store.keys():
    store[key] = sorted(list(store[key]))

inv_gd = list(store.items())
print(inv_gd[:10])

with open('inv_ind_gd.txt', 'a+') as file:
    for key, value in inv_gd:
        file.write(key+"       "+' '.join(map(str, value))+'\n')

print(len(w_gd))
print(len(inv_gd))