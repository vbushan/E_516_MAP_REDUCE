import os
in_loc='./books/'

num_mappers=3


book_names = list(os.listdir(in_loc))
map_in_files = [[] for i in range(num_mappers)]
for i in range(len(book_names)):
    index = i % num_mappers
    map_in_files[index].append((i + 1, book_names[i]))


mapper_input = [[] for i in range(num_mappers)]

for i in range(len(map_in_files)):
    for file_index, file in map_in_files[i]:
        mapper_input[i].append((file_index, open(in_loc + file, 'r', encoding='utf-8').read()))


print(map_in_files)
print(len(mapper_input))
print(mapper_input[0][0][1][:100])
print(mapper_input[1][0][1][:100])
print(mapper_input[2][0][1][:100])

#print(mapper_input[1][0][:10])
#print(mapper_input[2][0][:10])