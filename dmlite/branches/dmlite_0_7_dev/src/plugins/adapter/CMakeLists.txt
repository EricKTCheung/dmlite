cmake_minimum_required (VERSION 2.6)

add_library (adapter MODULE Adapter.cpp
                            DpmAdapter.cpp
                            FilesystemDriver.cpp
                            FunctionWrapper.cpp
                            IO.cpp
                            NsAdapter.cpp
                            NsAdapterINode.cpp
                            RFIO.cpp
            )

target_link_libraries (adapter ${DPM_LIBRARIES} pthread ${DMLITE_LIBRARIES} )
set_target_properties (adapter PROPERTIES PREFIX "plugin_")

install (TARGETS       adapter
         LIBRARY       DESTINATION usr/lib${LIB_SUFFIX}/dmlite/
                       COMPONENT   dmlite-plugins)
