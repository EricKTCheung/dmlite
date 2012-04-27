cmake_minimum_required (VERSION 2.6)

add_library(librarian MODULE Librarian.cpp LibrarianCatalog.cpp)

target_link_libraries (librarian dmlite)
set_target_properties (librarian PROPERTIES PREFIX "plugin_")

install(TARGETS       librarian
        LIBRARY       DESTINATION usr/lib${LIB_SUFFIX}/dmlite/
                      COMPONENT   dmlite-plugins)
