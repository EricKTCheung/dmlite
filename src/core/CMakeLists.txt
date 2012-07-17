cmake_minimum_required (VERSION 2.6)

set(DMLITE_CORE_SOURCES core/Auth.cpp
                        core/Defaults.cpp
                        core/Exceptions.cpp
                        core/PluginManager.cpp
                        core/StackInstance.cpp
                        core/Types.cpp

                        core/builtin/Catalog.cpp
                        core/builtin/UserGroupDb.cpp

                        core/dummy/DummyCatalog.cpp
                        core/dummy/DummyPool.cpp
    PARENT_SCOPE)
