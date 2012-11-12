cmake_minimum_required (VERSION 2.6)

set(DMLITE_CORE_SOURCES core/Exceptions.cpp
                        core/Operators.cpp
                        core/PluginManager.cpp
                        core/StackInstance.cpp

                        core/builtin/Authn.cpp
                        core/builtin/Catalog.cpp

                        core/dummy/DummyCatalog.cpp
                        core/dummy/DummyPool.cpp
                        
                        core/defaults/authn.cpp
                        core/defaults/base.cpp
                        core/defaults/catalog.cpp
                        core/defaults/inode.cpp
                        core/defaults/io.cpp
                        core/defaults/pooldriver.cpp
                        core/defaults/poolmanager.cpp
    PARENT_SCOPE)
