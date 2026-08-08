# Deliberately invalid Python. This code location exists only to give the
# dev stack a code location that fails to load, so dagster_code_location_load_error
# (see internal/collector/code_locations.go) has something to actually
# report `1` for. See workspace.yaml at the repo root and the "Testing a
# broken code location" section in README.md for how to load it.
this is not valid python !!!
