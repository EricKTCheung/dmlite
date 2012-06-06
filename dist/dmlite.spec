Name:		dmlite
Version:	0.3.0
Release:	1%{?dist}
Summary:	Common libraries for grid data management and storage
Group:		Applications/Internet
License:	ASL 2.0
URL:		https://svnweb.cern.ch/trac/lcgdm/wiki/Dpm/Dev/Dmlite
# The source of this package was pulled from upstream's vcs. Use the
# following commands to generate the tarball:
# svn export http://svn.cern.ch/guest/lcgdm/dmlite/tags/dmlite_0_3_0 dmlite-0.3.0
# tar -czvf dmlite-0.3.0.tar.gz dmlite-0.3.0
Source0:	%{name}-%{version}.tar.gz
Buildroot:	%{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

BuildRequires:	cmake
BuildRequires:	cppunit-devel
BuildRequires:	doxygen
BuildRequires:	dpm-devel
BuildRequires:	libmemcached-devel
BuildRequires:	mysql-devel
BuildRequires:	protobuf-devel

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

%description devel
This package provides headers and development libraries for dmlite.

%package plugins-adapter
Summary:	Adapter plugin for dmlite
Group:		Applications/Internet
Requires:	%{name}-libs%{?_isa} = %{version}-%{release}

%description plugins-adapter
This package provides the adapter plugin for dmlite. This plugin provides both
a name-space and pool management implementation which fallback to forwarding
calls to the old DPNS and DPM daemons.

%package plugins-librarian
Summary:	Librarian plugin for dmlite
Group:		Applications/Internet
Requires:	%{name}-libs%{?_isa} = %{version}-%{release}

%description plugins-librarian
This package provides the librarian plugin for dmlite. This plugin handles
the necessary logic to hop between difference replicas when accessing a file
managed by the grid.

%package plugins-memcached
Summary:	Memcached plugin for dmlite
Group:		Applications/Internet
Requires:	%{name}-libs%{?_isa} = %{version}-%{release}

%description plugins-memcached
This package provides the memcached plugin for dmlite. It provides a
memcached based implementation of the NS interface.

%package plugins-mysql 
Summary:	MySQL plugin for dmlite
Group:		Applications/Internet
Requires:	%{name}-libs%{?_isa} = %{version}-%{release}
Requires:	mysql%{?_isa}

%description plugins-mysql
This package provides the MySQL plugin for dmlite.

%package plugins-profiler
Summary:	Profiler plugin for dmlite
Group:		Applications/Internet
Requires:	%{name}-libs%{?_isa} = %{version}-%{release}

%description plugins-profiler
This package provides the profiler plugin for dmlite. This plugin is a simple
wrapper around a real plugin implementation, and is used to do multiple
measurements regarding the performance of each call to dmlite.

%package docs
Summary:	API documentation for dmlite
Group:		Applications/Internet

%description docs
Man pages and HTML documentation for dmlite.

%prep
%setup -q -n %{name}-%{version}

%build
%cmake . -DCMAKE_INSTALL_PREFIX=/

make %{?_smp_mflags}

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
%{_libdir}/libdmlite.so.*
%{_libdir}/libdmlitecommon.so.*
%doc README LICENSE

%files devel
%defattr(-,root,root,-)
%{_includedir}/dmlite
%{_libdir}/libdmlite.so
%{_libdir}/libdmlitecommon.so

%files plugins-adapter
%defattr(-,root,root,-)
%{_libdir}/dmlite/plugin_adapter.so

%files plugins-librarian
%defattr(-,root,root,-)
%{_libdir}/dmlite/plugin_librarian.so

%files plugins-memcached
%defattr(-,root,root,-)
%{_libdir}/dmlite/plugin_memcache.so

%files plugins-mysql
%defattr(-,root,root,-)
%{_libdir}/dmlite/plugin_mysql.so

%files plugins-profiler
%defattr(-,root,root,-)
%{_libdir}/dmlite/plugin_profiler.so

%files docs
%defattr(-,root,root,-)
%{_mandir}/man3/*
%{_defaultdocdir}/%{name}-%{version}

%changelog
* Tue Jun 05 2012 Ricardo Rocha <ricardo.rocha@cern.ch> - 0.2.0-3
- Removed subversion build dep
- Added patches for proper tests compilation (missing include, wrong cmake dep)

* Sun May 20 2012 Ricardo Rocha <ricardo.rocha@cern.ch> - 0.3.0-1
- Update for new upstream release

* Tue Feb 28 2012 Ricardo Rocha <ricardo.rocha@cern.ch> - 0.2.0-2
- Split plugins into multiple packages, added dependencies
- Updated package descriptions

* Tue Jan 31 2012 Alejandro Alvarez <alejandro.alvarez.ayllon@cern.ch> - 0.2.0-1
- Added documentation to the build process

* Mon Jan 23 2012 Alejandro Alvarez <alejandro.alvarez.ayllon@cern.ch> - 0.1.0-1
- Added cppunit-devel as a build dependency

* Tue Jan 20 2012 Alejandro Alvarez <alejandro.alvarez.ayllon@cern.ch> - 0.1.0-1
- Created spec file
