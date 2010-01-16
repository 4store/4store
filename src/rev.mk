export gitrev := $(shell git describe --always --tags 2>/dev/null || git describe --tags)
