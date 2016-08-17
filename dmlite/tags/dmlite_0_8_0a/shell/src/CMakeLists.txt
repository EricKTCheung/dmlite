cmake_minimum_required (VERSION 2.6)

find_package (PythonLibs REQUIRED)
#find_package (PyDMLite REQUIRED)

execute_process (COMMAND python -c "from distutils.sysconfig import get_python_lib; print (get_python_lib())" 
	  	 	OUTPUT_VARIABLE PYTHON_SITE_PACKAGES
		 	OUTPUT_STRIP_TRAILING_WHITESPACE)

install(PROGRAMS      dmlite-shell
        DESTINATION   ${INSTALL_PFX_USR}/bin)

install(PROGRAMS      dmlite-mysql-dirspaces.py
        DESTINATION   ${INSTALL_PFX_USR}/bin)

install(FILES         __init__.py interpreter.py dbutils.py executor.py
	DESTINATION   ${INSTALL_PYTHON_SITE_PACKAGES}/dmliteshell
)
