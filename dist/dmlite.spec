%{!?python_sitelib: %global python_sitelib %(%{__python} -c "from distutils.sysconfig import get_python_lib; print (get_python_lib())")}
%{!?python_sitearch: %global python_sitearch %(%{__python} -c "from distutils.sysconfig import get_python_lib; print (get_python_lib(1))")}

Name:		dmlite
Version:	0.7.0
Release:	1%{?dist}
Summary:	Common libraries for grid data management and storage
Group:		Applications/Internet
License:	ASL 2.0
URL:		https://svnweb.cern.ch/trac/lcgdm/wiki/Dpm/Dev/Dmlite
# The source of this package was pulled from upstream's vcs. Use the
# following commands to generate the tarball:
# svn export http://svn.cern.ch/guest/lcgdm/dmlite/tags/dmlite_0_6_1 dmlite-0.6.1
# tar -czvf dmlite-0.6.1.tar.gz dmlite-0.6.1
Source0:	%{name}-%{version}.tar.gz
Buildroot:	%{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

%if %{?fedora}%{!?fedora:0} >= 10 || %{?rhel}%{!?rhel:0} >= 6
BuildRequires:	boost-devel >= 1.41.0
%else
BuildRequires:	boost141-devel
%endif
BuildRequires:	cmake
BuildRequires:	cppunit-devel
BuildRequires:	doxygen
BuildRequires:	graphviz
BuildRequires:	openssl-devel
BuildRequires:	python-devel
BuildRequires:	zlib-devel

%description
This package provides a set of common libraries and plugins that implement
logic for data management and storage on the grid.

%package libs
Summary:	Common libraries for all dmlite packages
Group:		Applications/Internet

%description libs
This package provides the libraries used by dmlite components.

%package devel
Summary:	Development libraries and headers for dmlite
Group:		Applications/Internet
Requires:	%{name}-libs%{?_isa} = %{version}-%{release}
%if %{?fedora}%{!?fedora:0} >= 10 || %{?rhel}%{!?rhel:0} >= 6
Requires:	boost-devel >= 1.41.0
%else
Requires:	boost141-devel
%endif

%description devel
This package provides headers and development libraries for dmlite.

%package docs
Summary:	API documentation for dmlite
Group:		Applications/Internet


%description docs
Man pages and HTML documentation for dmlite.


%package private-devel
Summary:	Private development libraries and headers for dmlite
Group:		Applications/Internet
Requires:	%{name}-libs%{?_isa} = %{version}-%{release}
%if %{?fedora}%{!?fedora:0} >= 10 || %{?rhel}%{!?rhel:0} >= 6
Requires:	boost-devel >= 1.41.0
%else
Requires:	boost141-devel
%endif

%description private-devel
This package provides private headers and development libraries for dmlite. These components are generally not binary compatible across releases.


%package -n python-dmlite
Summary:	Python wrapper for dmlite
Group:		Development/Libraries

%description -n python-dmlite
This package provides a python wrapper for dmlite.

%package test
Summary:	All sorts of tests for dmlite interfaces
Group:		Applications/Internet

%description test
Set of C,CPP and Python tests for dmlite interfaces and plug-ins.

%package -n dmlite-plugins-memcache
Summary:	Memcached plugin for dmlite
Group:		Applications/Internet
%if %{?fedora}%{!?fedora:0} >= 10 || %{?rhel}%{!?rhel:0} >= 6
BuildRequires:	boost-devel >= 1.41.0
%else
BuildRequires:	boost141-devel
%endif
BuildRequires:	cmake
BuildRequires:	libmemcached-devel
BuildRequires:	protobuf-devel

Requires:	dmlite-libs = %{version}

%description -n dmlite-plugins-memcache
This package provides the memcached plug-in for dmlite. It provides a
memcached based implementation of the NS interface.

%package -n dmlite-plugins-profiler
Summary:	Memcached plugin for dmlite
Group:		Applications/Internet
%if %{?fedora}%{!?fedora:0} >= 10 || %{?rhel}%{!?rhel:0} >= 6
BuildRequires:	boost-devel >= 1.41.0
%else
BuildRequires:	boost141-devel
%endif
BuildRequires:	cmake

Requires:	dmlite-libs = %{version}

%description -n dmlite-plugins-profiler
This package provides the profiler plug-in for dmlite. This plug-in is a simple
wrapper around a real plug-in implementation, and is used to do multiple
measurements regarding the performance of each call to dmlite.

%package -n dmlite-shell
Summary:	Shell environment for dmlite
Group:		Applications/Internet
BuildRequires:	python2-devel
BuildArch:	noarch

Requires:	python-dateutil
Requires:	python-dmlite

%description -n dmlite-shell
This package provides a shell environment for dmlite. It includes useful
commands for system administration, testers and power users.






%package -n dmlite-plugins-mysql
Summary:	MySQL plugin for dmlite
Group:		Applications/Internet
BuildRequires:	mysql-devel

Requires:	dmlite-libs >= 0.6.2

%description
This package provides the MySQL plug-in for dmlite.

















%prep
%setup -q -n %{name}-%{version}

%build
%cmake . -DCMAKE_INSTALL_PREFIX=/ -DRUN_ONLY_STANDALONE_TESTS=ON

make %{?_smp_mflags}

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

%clean
rm -rf %{buildroot}

%post libs -p /sbin/ldconfig

%postun libs -p /sbin/ldconfig

%files libs
%defattr(-,root,root,-)
%config(noreplace) %{_sysconfdir}/dmlite.conf
%dir %{_sysconfdir}/dmlite.conf.d
%{_libdir}/libdmlite.so.*
%dir %{_libdir}/dmlite
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

%files test
%defattr(-,root,root,-)
%{_libdir}/dmlite/test

%files -n dmlite-plugins-memcache
%defattr(-,root,root,-)
%{_libdir}/dmlite/plugin_memcache.so
%doc LICENSE README RELEASE-NOTES
%config(noreplace) %{_sysconfdir}/dmlite.conf.d/zmemcache.conf

%files -n dmlite-plugins-profiler
%defattr(-,root,root,-)
%{_libdir}/dmlite/plugin_profiler.so
%doc LICENSE README RELEASE-NOTES
%config(noreplace) %{_sysconfdir}/dmlite.conf.d/profiler.conf

%files -n dmlite-shell
%defattr(-,root,root,-)
%{_bindir}/*
%{python_sitelib}/dmliteshell
%doc LICENSE README RELEASE-NOTES

%files 
%defattr(-,root,root,-)
%{_libdir}/dmlite/plugin_mysql.so
%doc LICENSE README RELEASE-NOTES
%config(noreplace) %{_sysconfdir}/dmlite.conf.d/*



%changelog
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
