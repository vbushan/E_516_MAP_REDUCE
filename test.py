import re
from collections import Counter
import os
from google.cloud import storage
storage_client = storage.Client()

data = []
words = []
id = 1

# Read the input

for book in os.listdir('./books/'):
    with open(f'./books/{book}', encoding="utf8", errors="surrogateescape") as file:
        for word in file.read().split(" "):
            if re.fullmatch(r'[a-zA-Z]+', word):
                data.append((word, id))
                words.append(word)
    id += 1
# Word count test

# Compute the counts from the input
w_gd = list(Counter(words).items())

store_gd = dict()

for key, value in w_gd:
    store_gd[key] = value

# Read the computed output from cloud storage bucket
bucket_name = "map-red-vathepalli-vamsi-bushan-293105"
bucket = storage_client.bucket(bucket_name)

blobs = list(bucket.list_blobs())

index = 0

out_files = []

# Parse the output and store the computed counts in the dictionary
for blob in blobs:
    if blob.name.startswith('word_count_map_red'):
        file = f'Output-{index}.txt'
        blob.download_to_filename(file)
        out_files.append(file)
        print(
            "Blob {} downloaded to {}.".format(
                blob.name, file
            )
        )
        index += 1


store = {}
for i in range(3):
    with open(out_files[i], 'r') as file:
        data = file.readlines()
        for line in data:
            temp = line.split("       ")
            key = temp[0]
            value = int(temp[1])
            store[key] = value


# Check if the computed counts is equal to the ground truth
print(store_gd == store)
