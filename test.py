from itertools import chain


x =  chain([1, 2, 3], ['a', 'b', 'c'], ('Timur', 29, 'Male', 'Teacher'))
print(list(x))