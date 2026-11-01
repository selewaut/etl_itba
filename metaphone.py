import jellyfish

string1 = "knight"
string2 = "night"

print(jellyfish.soundex(string1))
print(jellyfish.soundex(string2))

print(jellyfish.damerau_levenshtein_distance(string1, string2))
print( 1-jellyfish.damerau_levenshtein_distance(string1, string2) / max(len(string1), len(string2)))