# Deliberately valid-but-broken: no Definitions/RepositoryDefinition/etc.
# is exported, so Dagster fails to load this code location with a
# DagsterInvariantViolationError. This exists only to give the dev stack a
# code location that fails to load, so dagster_code_location_load_error
# (see internal/collector/code_locations.go) has something to actually
# report `1` for. See dev/workspace.yaml and the "Testing a broken code
# location" section in README.md for how to load it.
#
# Kept as valid Python (rather than a syntax error) so it doesn't need to
# be excluded from linting.
print("no dagster definitions here")
