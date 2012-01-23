Name:		  dmlite
Version:	0.1.0
Release:	1%{?dist}
Summary:	Common libraries for LCGDM components.
Group:		Applications/Internet
License:	ASL 2.0
URL:		  https://svnweb.cern.ch/trac/lcgdm
# The source of this package was pulled from upstream's vcs. Use the
# following commands to generate the tarball:
# svn export http://svn.cern.ch/guest/lcgdm/libdm/tags/dmlite_0_1_0 dmlite-0.1.0
# tar -czvf dmlite-0.1.0.tar.gz dmlite-0.1.0
Source0:	  %{name}-%{version}.tar.gz
Buildroot:	%{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

BuildRequires:	cmake%{?_isa}
BuildRequires:	dpm-devel%{?_isa}
BuildRequires:	subversion%{?_isa}
BuildRequires:	mysql-devel%{?_isa}
BuildRequires:	cppunit-devel%{?_isa}

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
DMLite development headers and libraries.

%package plugins
Summary:	Set of plugins for dmlite.
Group:		Applications/Internet
Requires:	%{name}-libs%{?_isa} = %{version}-%{release}

%description plugins
DMLite default set of plugins: MySQL, Adapter, Profiler and Librarian.

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

%changelog
* Mon Jan 23 2012 Alejandro Alvarez <alejandro.alvarez.ayllon@cern.ch> - 0.1.0-1
- Added cppunit-devel as a build dependency
* Tue Jan 20 2012 Alejandro Alvarez <alejandro.alvarez.ayllon@cern.ch> - 0.1.0-1
- Created spec file
