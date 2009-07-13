#!
find . -type f -name '*.[ch]' -exec cat '{}' \; | wc -l
