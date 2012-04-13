#
# This module searches for the Oracle libraries
#

# Oracle libraries
find_path(ORACLE_LIBRARY_DIR
          names libocci.so
          HINTS /usr/lib64/oracle/*/client/lib64/
                /usr/lib/oracle/*/client/lib/
          DOC "Oracle library dir"
)

# Oracle headers
find_path(ORACLE_INCLUDE_DIR
          names occi.h
          HINTS /usr/include/oracle/*/client/
          DOC "Oracle include dir"
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(oracle DEFAULT_MSG ORACLE_LIBRARY_DIR ORACLE_INCLUDE_DIR)
mark_as_advanced(ORACLE_INCLUDE_DIR ORACLE_INCLUDE_DIR)
