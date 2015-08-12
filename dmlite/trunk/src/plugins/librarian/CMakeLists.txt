cmake_minimum_required (VERSION 2.6)

include_directories (${Boost_INCLUDE_DIR})

add_library (librarian MODULE Librarian.cpp LibrarianCatalog.cpp)

target_link_libraries (librarian dmlite)
set_target_properties (librarian PROPERTIES PREFIX "plugin_")

install(TARGETS       librarian
        LIBRARY       DESTINATION ${INSTALL_PFX_LIB}/dmlite/)
