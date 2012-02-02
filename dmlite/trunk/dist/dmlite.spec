Name:		dmlite
Version:	0.2.0
Release:	1%{?dist}
Summary:	Abstraction library for LCGDM components
Group:		Applications/Internet
License:	ASL 2.0
URL:		https://svnweb.cern.ch/trac/lcgdm
# The source of this package was pulled from upstream's vcs. Use the
# following commands to generate the tarball:
# svn export http://svn.cern.ch/guest/lcgdm/libdm/tags/dmlite_0_2_0 dmlite-0.2.0
# tar -czvf dmlite-0.2.0.tar.gz dmlite-0.2.0
Source0:	%{name}-%{version}.tar.gz
Buildroot:	%{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

BuildRequires:	cmake%{?_isa}
BuildRequires:	dpm-devel%{?_isa}
BuildRequires:	subversion%{?_isa}
BuildRequires:	mysql-devel%{?_isa}
BuildRequires:	cppunit-devel%{?_isa}
BuildRequires:	doxygen%{?_isa}

%description
This package provides a set of libraries and plugins that implements
the common logic for LCGDM components.

%package libs
Summary:	Libraries
Group:		Applications/Internet

%description libs
DMLite libraries.

%package devel
Summary:	Development libraries and headers for dmlite
Group:		Applications/Internet
Requires:	%{name}-libs%{?_isa} = %{version}-%{release}

%description devel
Development headers and libraries for dmlite.

%package plugins
Summary:	Set of plugins for dmlite
Group:		Applications/Internet
Requires:	%{name}-libs%{?_isa} = %{version}-%{release}

%description plugins
Default set of plugins for dmlite: MySQL, Adapter, Profiler and Librarian.

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
rm -rf $RPM_BUILD_ROOT
mkdir -p $RPM_BUILD_ROOT

make install DESTDIR=$RPM_BUILD_ROOT

%clean
rm -rf $RPM_BUILD_ROOT

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

%files plugins
%defattr(-,root,root,-)
%{_libdir}/dmlite

%files docs
%defattr(-,root,root,-)
%{_mandir}/man3/*
%{_defaultdocdir}/%{name}-%{version}

%changelog
* Tue Jan 31 2012 Alejandro Alvarez <alejandro.alvarez.ayllon@cern.ch> - 0.2.0-1
- Added documentation to the build process
* Mon Jan 23 2012 Alejandro Alvarez <alejandro.alvarez.ayllon@cern.ch> - 0.1.0-1
- Added cppunit-devel as a build dependency
* Tue Jan 20 2012 Alejandro Alvarez <alejandro.alvarez.ayllon@cern.ch> - 0.1.0-1
- Created spec file
