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
Version:				0.7.3
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

%if %{?fedora}%{!?fedora:0} >= 10 || %{?rhel}%{!?rhel:0} >= 6
BuildRequires:			boost-devel >= 1.41.0
%else
BuildRequires:			boost141-devel
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

%description
This package provides the dmlite framework that implements common
logic for data management and storage for the Lcg grid.

%package libs
Summary:			Libraries for dmlite packages
Group:				Applications/Internet

# transition fix for package merge dmlite-*.src.rpm to dmlite.src.rpm
Obsoletes:			dmlite-plugins-adapter-debuginfo < 0.7.0-1
Obsoletes:			dmlite-plugins-mysql-debuginfo < 0.7.0-1
Obsoletes:			dmlite-plugins-memcache-debuginfo < 0.7.0-1
Obsoletes:			dmlite-plugins-profiler-debuginfo < 0.7.0-1
Obsoletes:			dmlite-shell-debuginfo < 0.7.0-1

%description libs
This package provides the core libraries used by dmlite.

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
%if %{?fedora}%{!?fedora:0} >= 10 || %{?rhel}%{!?rhel:0} >= 6
Requires:			boost-devel >= 1.41.0
%else
Requires:			boost141-devel
%endif

%description private-devel
Private development headers for dmlite. Provided for the development of 
dmlite plugins only, no API compatibility is guaranteed on these headers.


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
Summary:			Memcached plugin for dmlite
Group:				Applications/Internet
Requires:			%{name}-libs%{?_isa} = %{version}-%{release}

# Merge migration 
Obsoletes:			dmlite-plugins-profiler < 0.7.0-1

%description plugins-profiler
This package provides the profiler plug-in for dmlite. This plug-in 
provides multiple performance measurement tools for dmlite. 

%package shell
Summary:			Shell environment for dmlite
Group:				Applications/Internet
%if %{?fedora}%{!?fedora:0} >= 10 || %{?rhel}%{!?rhel:0} >= 6
BuildArch:			noarch
%endif

Requires:			python-dateutil
Requires:			python-dmlite = %{version}

Obsoletes:			dmlite-shell < 0.7.0-1

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

## remote tests if not needed
%if %{?dmlite_tests} == 0
rm -rf %{buildroot}/%{_libdir}/dmlite/test
%endif 

%clean
rm -rf %{buildroot}

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
        /sbin/service dpm-gsiftp condrestart ||true
%endif


%postun libs -p /sbin/ldconfig

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

%files devel
%defattr(-,root,root,-)
%{_includedir}/dmlite/c
%{_includedir}/dmlite/common
%{_libdir}/libdmlite.so

%files private-devel
%defattr(-,root,root,-)
%{_includedir}/dmlite/cpp

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
%config(noreplace) %{_sysconfdir}/dmlite.conf.d/zmemcache.conf

%files plugins-profiler
%defattr(-,root,root,-)
%{_libdir}/dmlite/plugin_profiler.so
%doc LICENSE README RELEASE-NOTES
%config(noreplace) %{_sysconfdir}/dmlite.conf.d/profiler.conf

%files shell
%defattr(-,root,root,-)
%{_bindir}/*
%{python_sitelib}/dmliteshell
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
%config(noreplace) %{_sysconfdir}/dmlite.conf.d/adapter.conf

%changelog
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

* Tue Jan 20 2012 Alejandro Alvarez <alejandro.alvarez.ayllon@cern.ch> - 0.1.0-1
- Created spec file
