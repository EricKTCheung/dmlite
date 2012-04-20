Name:		dmlite-hadoop
Version:	0.3.0
Release:	1%{?dist}
Summary:	Hadoop plugin for DMLITE
Group:		Applications/Internet
License:	ASL 2.0
URL:		https://svnweb.cern.ch/trac/lcgdm
# The source of this package was pulled from upstream's vcs. Use the
# following commands to generate the tarball:
# svn export http://svn.cern.ch/guest/lcgdm/dmlite/tags/dmlite_0_3_0 dmlite-0.3.0
# tar -czvf dmlite-0.3.0.tar.gz dmlite-0.3.0
Source0:	%{name}-%{version}.tar.gz
Buildroot:	%{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

BuildRequires:	cmake%{?_isa}
BuildRequires:	cppunit-devel%{?_isa}
BuildRequires:	dmlite-devel%{?_isa}
BuildRequires:	doxygen%{?_isa}
BuildRequires:	hadoop-0.20-libhdfs
BuildRequires:	java-devel

%description
This package provides the hadoop plugin for DMLITE. It is separated from
the main spec file due to dependencies on the hadoop libraries, which are
not available in Fedora/EPEL.

%package plugins-hadoop
Summary:	Hadoop plugin for DMLITE
Group:		Applications/Internet

%description plugins-hadoop
This package provides the hadoop plugin for DMLITE. It provides a hadoop
implementation of all the DMLITE interfaces (pool, IO, etc).

%prep
%setup -q -n %{name}-%{version}

%build
cd plugins/hadoop
%cmake . -DCMAKE_INSTALL_PREFIX=/

make %{?_smp_mflags}

%install
rm -rf $RPM_BUILD_ROOT
mkdir -p $RPM_BUILD_ROOT

cd plugins/hadoop
make install DESTDIR=$RPM_BUILD_ROOT

%clean
rm -rf $RPM_BUILD_ROOT

%files plugins-hadoop
%defattr(-,root,root,-)
%{_libdir}/dmlite/plugin_hadoop.so

%changelog
* Wed Apr 11 2012 Ricardo Rocha <ricardo.rocha@cern.ch> - 0.2.0-1
- Initial build
