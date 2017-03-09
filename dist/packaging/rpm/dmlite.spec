%{!?python_sitelib: %global python_sitelib %(%{__python} -c "from distutils.sysconfig import get_python_lib; print (get_python_lib())")}
%{!?python_sitearch: %global python_sitearch %(%{__python} -c "from distutils.sysconfig import get_python_lib; print (get_python_lib(1))")}

%{!?dmlite_test: %global dmlite_tests 0}

# systemd definition, to do the right thing if we need to restart daemons
%if %{?fedora}%{!?fedora:0} >= 17 || %{?rhel}%{!?rhel:0} >= 7
%global systemd 1
%else
%global systemd 0
%endif



Name:					dmlite
Version:				1.10.0
Release:				1%{?dist}
Summary:				Lcgdm grid data management and storage framework
Group:					Applications/Internet
License:				ASL 2.0
URL:					https://svnweb.cern.ch/trac/lcgdm/wiki/Dpm/Dev/Dmlite
# The source of this package was pulled from upstream's vcs. Use the
# following commands to generate the tarball:
# svn export http://svn.cern.ch/guest/lcgdm/dmlite/tags/dmlite_0_7_0_d dmlite-0.7.0
# tar -czvf dmlite-0.7.0.tar.gz dmlite-0.7.0
Source0:				%{name}-%{version}.tar.gz
Buildroot:				%{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

%if %{?fedora}%{!?fedora:0} >= 17 || %{?rhel}%{!?rhel:0} >= 7
BuildRequires:                  boost-devel >= 1.48.0
%else
BuildRequires:                  boost148-devel >= 1.48.0
%endif


BuildRequires:			cmake
BuildRequires:			cppunit-devel
BuildRequires:			doxygen
BuildRequires:			graphviz
BuildRequires:			openssl-devel
BuildRequires:			python-devel
BuildRequires:			zlib-devel

# Plugins requires
BuildRequires:			dpm-devel
BuildRequires:			lcgdm-devel
BuildRequires:			libmemcached-devel
BuildRequires:			mysql-devel
BuildRequires:			protobuf-devel
BuildRequires:		        davix-devel
BuildRequires:			fcgi-devel

%description
This package provides the dmlite framework that implements common
logic for data management and storage for the Lcg grid.














%package dpmhead-dome
Summary:  DPM Head Node (MySQL)
Group:    Applications/Internet
Requires: bdii

Requires: dpm-dsi%{?_isa} >= 1.9.10
Requires: dmlite-plugins-domeadapter = %{version}
Requires: dmlite-dome = %{version}
Requires: dmlite-shell = %{version}
Requires: dmlite-plugins-mysql = %{version}
Requires: edg-mkgridmap
Requires: fetch-crl
Requires: finger
Requires: lcgdm-dav
Requires: lcgdm-dav-server
Requires: davix

Obsoletes: emi-dpm_mysql
Obsoletes: dpmhead
Conflicts: dpm%{?_isa}
Conflicts: dpm-devel%{?_isa}
Conflicts: dpm-name-server-mysql%{?_isa}
Conflicts: dpm-perl%{?_isa}
Conflicts: dpm-python%{?_isa}
Conflicts: dpm-rfio-server%{?_isa}
Conflicts: dpm-server-mysql%{?_isa}
Conflicts: dpm-srm-server-mysql%{?_isa}
Conflicts: dmlite-plugins-adapter

%description dpmhead-dome
The Disk Pool Manager (DPM) creates a Grid storage element from a set
of disk servers. It provides several interfaces for storing and retrieving
data such as HTTP, Xrootd, GridFTP
This is a metapackage providing all required daemons for a DPM Head Node.

%package dpmdisk-dome
Summary:  DPM Disk Node
Group:    Applications/Internet

Requires: dpm-dsi%{?_isa} >= 1.9.10

Requires: dmlite-plugins-domeadapter = %{version}
Requires: dmlite-dome = %{version}
Requires: edg-mkgridmap
Requires: fetch-crl
Requires: finger
Requires: lcgdm-dav
Requires: lcgdm-dav-server
Requires: davix

Obsoletes: emi-dpm_disk
Obsoletes: dpmdisk
Conflicts: dpm%{?_isa}
Conflicts: dpm-devel%{?_isa}
Conflicts: dpm-perl%{?_isa}
Conflicts: dpm-python%{?_isa}
Conflicts: dpm-rfio-server%{?_isa}
Conflicts: dmlite-plugins-adapter = %{version}

%description dpmdisk-dome
The Disk Pool Manager (DPM) creates a Grid storage element from a set
of disk servers. It provides several interfaces for storing and retrieving
data such as HTTP, Xrootd, GridFTP
This is a metapackage providing all required daemons for a DPM
Disk Node.










%package dpmhead
Summary:  EMI DPM Head Node (MySQL)
Group:    Applications/Internet
Requires: bdii
Requires: dpm%{?_isa} >= 1.9
Requires: dpm-devel%{?_isa} >= 1.9
Requires: dpm-dsi%{?_isa} >= 1.9.10
Requires: dpm-name-server-mysql%{?_isa} >= 1.9
Requires: dpm-perl%{?_isa} >= 1.9
Requires: dpm-python%{?_isa} >= 1.9
Requires: dpm-rfio-server%{?_isa} >= 1.9
Requires: dpm-server-mysql%{?_isa} >= 1.9
Requires: dpm-srm-server-mysql%{?_isa} >= 1.9

Requires: dmlite-plugins-adapter = %{version}
Requires: dmlite-plugins-domeadapter = %{version}
Requires: dmlite-dome = %{version}
Requires: dmlite-shell = %{version}
Requires: dmlite-plugins-mysql = %{version}
Requires: edg-mkgridmap
Requires: fetch-crl
Requires: finger
Requires: lcgdm-dav
Requires: lcgdm-dav-server
Requires: davix

Obsoletes: emi-dpm_mysql


%description dpmhead
The Disk Pool Manager (DPM) creates a Grid storage element from a set
of disk servers. It provides several interfaces for storing and retrieving
data such as HTTP, Xrootd, GridFTP
This is a metapackage providing all required daemons for a DPM Head Node, including
legacy components like SRM, RFIO, libshift, CSec, dpm, dpnsdaemon.

%package dpmdisk
Summary:  EMI DPM Disk Node
Group:    Applications/Internet
Requires: dpm%{?_isa} >= 1.9
Requires: dpm-devel%{?_isa} >= 1.9
Requires: dpm-dsi%{?_isa} >= 1.9.10
Requires: dpm-perl%{?_isa} >= 1.9
Requires: dpm-python%{?_isa} >= 1.9
Requires: dpm-rfio-server%{?_isa} >= 1.9

Requires: dmlite-plugins-adapter = %{version}
Requires: dmlite-plugins-domeadapter = %{version}
Requires: dmlite-dome = %{version}
Requires: dmlite-shell = %{version}
Requires: edg-mkgridmap

Requires: fetch-crl
Requires: finger
Requires: lcgdm-dav
Requires: lcgdm-dav-server
Requires: davix

Obsoletes: emi-dpm_disk

%description dpmdisk
The LCG Disk Pool Manager (DPM) creates a storage element from a set
of disks. It provides several interfaces for storing and retrieving
data such as RFIO and SRM version 1, version 2 and version 2.2.
This is a virtual package providing all required daemons for a DPM
Disk Node, including legacy components like RFIO, libshift, CSec.












%package libs
Summary:			Libraries for dmlite packages
Group:				Applications/Internet

# transition fix for package merge dmlite-*.src.rpm to dmlite.src.rpm
Obsoletes:			dmlite-plugins-adapter-debuginfo < 0.7.0-1
Obsoletes:			dmlite-plugins-mysql-debuginfo < 0.7.0-1
Obsoletes:			dmlite-plugins-memcache-debuginfo < 0.7.0-1
Obsoletes:			dmlite-plugins-profiler-debuginfo < 0.7.0-1
Obsoletes:                      dmlite-plugins-librarian-debuginfo < 0.7.0-1
Obsoletes:			dmlite-shell-debuginfo < 0.7.0-1

# This was due to the switch to boost 1.48, linked to the bugs in propertytree
Conflicts:      dpm-xrootd <= 3.6.0

# Not really necessary, just better to limit the space for mistakes
Conflicts:      dpm-dsi < 1.9.10

# Versions prior to this one do not properly do accounting on directories
Conflicts:      lcgdm-libs <= 1.8

%description libs
This package provides the core libraries used by dmlite.

%package dome
Summary:			The dome daemon
Group:				Applications/Internet
Requires:     httpd
%if %systemd == 0
Requires:     mod_proxy_fcgi
%endif

%description dome
This package provides the binaries necessary to run the dome daemon.

%package devel
Summary:			Development libraries and headers for dmlite
Group:				Applications/Internet
Requires:			%{name}-libs%{?_isa} = %{version}-%{release}

%description devel
This package provides C headers and development libraries for dmlite.

%package docs
Summary:			Documentation files for dmlite
Group:				Applications/Internet


%description docs
This package contains the man pages and HTML documentation for dmlite.


%package private-devel
Summary:			Private development libraries and headers for dmlite
Group:				Applications/Internet
Requires:			%{name}-devel%{?_isa} = %{version}-%{release}
%if %{?fedora}%{!?fedora:0} >= 17 || %{?rhel}%{!?rhel:0} >= 7
BuildRequires:                  boost-devel >= 1.48.0
%else
BuildRequires:                  boost148-devel >= 1.48.0
%endif



%description private-devel
Private development headers for dmlite. Provided for the development of
dmlite plugins only, no API compatibility is guaranteed on these headers.



%package dpm-tester
Summary:      The dpm tester tool
Group:        Applications/Internet
Requires:     python
Requires:     gfal2-python

%description dpm-tester
Tool that is useful to test the main features of a DPM setup






%package -n python-dmlite
Summary:			Python wrapper for dmlite
Group:				Development/Libraries

%description -n python-dmlite
This package provides a python wrapper for dmlite.

%package test
Summary:			All sorts of tests for dmlite interfaces
Group:				Applications/Internet

%description test
Set of C,CPP and Python tests for dmlite interfaces and plug-ins.

%package plugins-memcache
Summary:			Memcached plugin for dmlite
Group:				Applications/Internet
Requires:			%{name}-libs%{?_isa} = %{version}-%{release}

# Merge migration
Obsoletes:			dmlite-plugins-memcache < 0.7.0-1

%description plugins-memcache
This package provides the memcached plug-in for dmlite. It provides a
memcached based layer for the Lcgdm nameserver.

%package plugins-profiler
Summary:			Monitoring plugin for dmlite
Group:				Applications/Internet
Requires:			%{name}-libs%{?_isa} = %{version}-%{release}

# Merge migration
Obsoletes:			dmlite-plugins-profiler < 0.7.0-1

%description plugins-profiler
This package provides the profiler plug-in for dmlite. This plug-in
provides multiple performance measurement tools for dmlite.


%package plugins-librarian
Summary:                        Libarian plugin for dmlite
Group:                          Applications/Internet
Requires:                       %{name}-libs%{?_isa} = %{version}-%{release}

# Merge migration
Obsoletes:                      dmlite-plugins-librarian < 0.7.0-1

%description plugins-librarian
This package provides the librarian plug-in for dmlite.




%package shell
Summary:			Shell environment for dmlite
Group:				Applications/Internet
#%if %{?fedora}%{!?fedora:0} >= 10 || %{?rhel}%{!?rhel:0} >= 6
#BuildArch:			noarch
#%endif

Requires:			python-dateutil
Requires:			python-dmlite = %{version}
Requires:                       MySQL-python
Requires:                       python-pycurl

Obsoletes:			dmlite-shell < %{version}-%{release}

%description shell
This package provides a shell environment for dmlite. It includes useful
commands for system administration, testers and power users.

%package plugins-mysql
Summary:			MySQL plugin for dmlite
Group:				Applications/Internet
Requires:			%{name}-libs%{?_isa} = %{version}-%{release}

Obsoletes:			dmlite-plugins-mysql < 0.7.0-1

%description plugins-mysql
This package provides the MySQL plug-in for dmlite.

%package plugins-adapter
Summary:			Adapter plugin for dmlite
Group:				Applications/Internet
Requires:			%{name}-libs%{?_isa} = %{version}-%{release}
Requires:			dpm-libs >= 1.8.8
Requires:			lcgdm-libs >= 1.8.8

Obsoletes:			dmlite-plugins-adapter < 0.7.0-1

%description plugins-adapter
This package provides the adapter plug-in for dmlite. This plug-in provides both
a name-space and pool management implementation which fallback to forwarding
calls to the old LcgDM DPNS and DPM daemons.

%package plugins-domeadapter
Summary:      Adapter plugin for dmlite
Group:        Applications/Internet
Requires:     %{name}-libs%{?_isa} = %{version}-%{release}

%description plugins-domeadapter
This package provides the next-generation adapter plug-in for dmlite, which uses
dome and does not depend on the old LcgDM DPNS and DPM daemons.

%prep
%setup -q -n %{name}-%{version}

%build
%cmake . -DCMAKE_INSTALL_PREFIX=/ -DRUN_ONLY_STANDALONE_TESTS=ON

make %{?_smp_mflags}
make doc

%check
pushd tests
LD_LIBRARY_PATH=~+/../src/ ctest -V
if [ $? -ne 0 ]; then
    exit 1
fi
popd

%install
rm -rf %{buildroot}
mkdir -p %{buildroot}

make install DESTDIR=%{buildroot}
# clean up the startup scripts we don't need - otherwise rpmbuild will fail
# due to unpackaged files
%if %systemd
  rm -rf %{buildroot}/%{_sysconfdir}/rc.d
%else
  rm -rf %{buildroot}/usr/lib/systemd
%endif

## remote tests if not needed
%if %{?dmlite_tests} == 0
rm -rf %{buildroot}/%{_libdir}/dmlite/test
%endif

%clean
rm -rf %{buildroot}

%post dome
%if %systemd
        /bin/systemctl try-restart domehead.service || true
        /bin/systemctl try-restart domedisk.service || true
%else
        /sbin/service domehead condrestart || true
        /sbin/service domedisk condrestart || true
%endif

%post libs
/sbin/ldconfig
/sbin/service rsyslog condrestart || true
%if %systemd
        /bin/systemctl try-restart dpm.service || true
        /bin/systemctl try-restart dpnsdaemon.service || true
        /bin/systemctl try-restart httpd.service || true
        /bin/systemctl try-restart dpm-gsiftp.service || true
%else
        /sbin/service dpm condrestart  || true
        /sbin/service dpnsdaemon condrestart ||true
        /sbin/service httpd condrestart || true
        /sbin/service dpm-gsiftp condrestart || true
%endif


%postun libs -p /sbin/ldconfig



%files dpmhead-dome
%defattr(-,root,root,-)
%{_prefix}/share/dmlite/dbscripts
%{_prefix}/share/dmlite/filepull


%files dpmdisk-dome
%defattr(-,root,root,-)
%{_prefix}/share/dmlite/filepull



%files dpmhead
%defattr(-,root,root,-)
%{_prefix}/share/dmlite/dbscripts
%{_prefix}/share/dmlite/filepull


%files dpmdisk
%defattr(-,root,root,-)
%{_prefix}/share/dmlite/filepull

%files libs
%defattr(-,root,root,-)
%dir %{_sysconfdir}/dmlite.conf.d
%dir %{_libdir}/dmlite
%config(noreplace) %{_sysconfdir}/dmlite.conf
%config(noreplace) %{_sysconfdir}/logrotate.d/dmlite
%config(noreplace) %{_sysconfdir}/rsyslog.d/20-log-dmlite.conf
%{_libdir}/libdmlite.so.*
%{_libdir}/dmlite/plugin_config.so
%doc README LICENSE RELEASE-NOTES

%files dome
%defattr(-,root,root,-)
%dir %{_localstatedir}/www/fcgi-bin
%{_bindir}/dome-checksum
%if %systemd
/usr/lib/systemd/system/domehead.service
/usr/lib/systemd/system/domedisk.service
%else
%{_sysconfdir}/rc.d/init.d/domehead
%{_sysconfdir}/rc.d/init.d/domedisk
%endif

%{_localstatedir}/www/fcgi-bin/dome

%files devel
%defattr(-,root,root,-)
%{_includedir}/dmlite/c
%{_includedir}/dmlite/common
%{_libdir}/libdmlite.so

%files private-devel
%defattr(-,root,root,-)
%{_includedir}/dmlite/cpp

%files dpm-tester
%defattr(-,root,root,-)
%{_bindir}/dpm-tester.py


%files docs
%defattr(-,root,root,-)
%{_mandir}/man3/*
%{_defaultdocdir}/%{name}-%{version}

%files -n python-dmlite
%defattr(-,root,root,-)
%{python_sitearch}/pydmlite.so

%if %{?dmlite_tests}

%files test
%defattr(-,root,root,-)
%{_libdir}/dmlite/test

%endif %{?dmlite_tests}

%files plugins-memcache
%defattr(-,root,root,-)
%{_libdir}/dmlite/plugin_memcache.so
%doc LICENSE README RELEASE-NOTES
%config(noreplace) %{_sysconfdir}/dmlite.conf.d/zmemcache.conf.example

%files plugins-profiler
%defattr(-,root,root,-)
%{_libdir}/dmlite/plugin_profiler.so
%doc LICENSE README RELEASE-NOTES
%config(noreplace) %{_sysconfdir}/dmlite.conf.d/profiler.conf

%files plugins-librarian
%defattr(-,root,root,-)
%{_libdir}/dmlite/plugin_librarian.so
%doc LICENSE README RELEASE-NOTES


%files shell
%defattr(-,root,root,-)
%{_bindir}/dmlite-shell
%{_bindir}/dmlite-mysql-dirspaces.py
%if %{?rhel}%{!?rhel:99} <= 5
%{_bindir}/dmlite-mysql-dirspaces.pyc
%{_bindir}/dmlite-mysql-dirspaces.pyo
%endif
%{python_sitearch}/dmliteshell
%doc LICENSE README RELEASE-NOTES

%files plugins-mysql
%defattr(-,root,root,-)
%{_libdir}/dmlite/plugin_mysql.so
%doc LICENSE README RELEASE-NOTES
%config(noreplace) %{_sysconfdir}/dmlite.conf.d/mysql.conf

%files plugins-adapter
%defattr(-,root,root,-)
%{_libdir}/dmlite/plugin_adapter.so
%doc LICENSE README RELEASE-NOTES
%config(noreplace) %{_sysconfdir}/dmlite.conf.d/adapter.conf.example

%files plugins-domeadapter
%defattr(-,root,root,-)
%{_libdir}/dmlite/plugin_domeadapter.so
%doc LICENSE README RELEASE-NOTES
%config(noreplace) %{_sysconfdir}/dmlite.conf.d/domeadapter.example

%changelog
* Thu Oct 13 2016  Fabrizio Furano <furano@cern.ch> - 0.8.1
- Add metapackages for dome flavour setup

* Mon Feb 15 2016  Andrea Manzi <amanzi@cern.ch> - 0.7.6-1
- Added move replicat to dmlite-shell
- fix crash in dmlite-plugins-mysql
- some fixes in dmlite-shell drain


* Mon Nov 02 2015  Andrea Manzi <amanzi@cern.ch> - 0.7.5-1
- added xattr to Memcache plugin
- fix for checksums store

* Wed Jul 08 2015  Fabrizio Furano <furano@cern.ch> - 0.7.3-1
- Add librarian to the core plugins

* Mon Nov 17 2014  Fabrizio Furano <furano@cern.ch> - 0.7.2-1
- Fix logname on RFIO.cpp
- Fix logging issue in adapter

* Fri Oct 03 2014 Andrea Manzi <amanzi@cern.ch> - 0.7.1-1
- Fix for wrong file size stored in Memcache
- Fix for xroot third party copy when Memcache enabled

* Mon Jun 16 2014 Fabrizio Furano <furano@cern.ch> - 0.7.0-2
- Push on Fedora/EPEL for 0.7.0
- Fix ppc EPEL5 compilation issue

* Mon Jun 16 2014 Fabrizio Furano <furano@cern.ch> - 0.7.0-1
- Introduced the private devel headers
- Merged shell, profiler, memcache, mysql, adapter

* Fri Nov 29 2013 Alejandro Alvarez <aalvarez@cern.ch> - 0.6.1-2
- Enabled Python bindings

* Wed Jul 10 2013 Alejandro Alvarez <aalvarez@cern.ch> - 0.6.1-1
- Update for new upstream release

* Wed Dec 19 2012 Ricardo Rocha <ricardo.rocha@cern.ch> - 0.6.0-1
- Update for new upstream release

* Thu Oct 25 2012 Ricardo Rocha <ricardo.rocha@cern.ch> - 0.5.0-1
- Update for new upstream release

* Wed Oct 24 2012 Ricardo Rocha <ricardo.rocha@cern.ch> - 0.4.2-2
- Fedora #869568 - dmlite-libs should own /usr/lib(64)/dmlite

* Mon Sep 24 2012 Ricardo Rocha <ricardo.rocha@cern.ch> - 0.4.2-1
- update for new upstream release
- dropped plugin packages (moved to separate individual packages)

* Sat Sep 22 2012  Remi Collet <remi@fedoraproject.org> - 0.3.0-2
- rebuild against libmemcached.so.11 without SASL

* Thu Jul 19 2012 Ricardo Rocha <ricardo.rocha@cern.ch> - 0.3.0-1
- Update for new upstream release

* Wed Jul 18 2012 Fedora Release Engineering <rel-eng@lists.fedoraproject.org> - 0.2.0-4
- Rebuilt for https://fedoraproject.org/wiki/Fedora_18_Mass_Rebuild

* Tue Jun 05 2012 Ricardo Rocha <ricardo.rocha@cern.ch> - 0.2.0-3
- Removed subversion build dep
- Added patches for proper tests compilation (missing include, wrong cmake dep)

* Tue Feb 28 2012 Ricardo Rocha <ricardo.rocha@cern.ch> - 0.2.0-2
- Split plugins into multiple packages, added dependencies
- Updated package descriptions

* Tue Jan 31 2012 Alejandro Alvarez <alejandro.alvarez.ayllon@cern.ch> - 0.2.0-1
- Added documentation to the build process

* Mon Jan 23 2012 Alejandro Alvarez <alejandro.alvarez.ayllon@cern.ch> - 0.1.0-1
- Added cppunit-devel as a build dependency

* Fri Jan 20 2012 Alejandro Alvarez <alejandro.alvarez.ayllon@cern.ch> - 0.1.0-1
- Created spec file
