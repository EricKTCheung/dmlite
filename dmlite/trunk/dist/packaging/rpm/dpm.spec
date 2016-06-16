Name:		dpm
Version:	1.9.0
Release:	1%{?dist}
Summary:	The Disk Pool Manager, belonging to the LHC Computing Grid Data Management

Group:		Applications/Internet
License:	ASL 2.0
URL:		http://glite.web.cern.ch/glite/

%description
This package provides dependencies to all the main DPM components,
plus various utilities and scripts.

%package -n dpm_head
Summary:	EMI DPM Head Node (MySQL)
Group:		Applications/Internet
Requires:	bdii
Requires:	dpm%{?_isa} = %{version}
Requires:	dpm-copy-server-mysql%{?_isa} = %{version}
Requires:	dpm-devel%{?_isa} = %{version}
Requires:	dpm-dsi%{?_isa}
Requires:	dpm-name-server-mysql%{?_isa} = %{version}
Requires:	dpm-perl%{?_isa} = %{version}
Requires:	dpm-python%{?_isa} = %{version}
Requires:	dpm-rfio-server%{?_isa} = %{version}
Requires:	dpm-server-mysql%{?_isa} = %{version}
Requires:	dpm-srm-server-mysql%{?_isa} = %{version}
Requires:	dpm-yaim >= 4.2.20
Requires:	dmlite-plugins-adapter >= 0.4.0
Requires:	dmlite-plugins-mysql >= 0.4.0
Requires:	edg-mkgridmap
Requires:       emi-version
Requires:	fetch-crl
Requires:	finger
Requires:	lcgdm-dav
Requires:	lcgdm-dav-server
Requires:	lcg-expiregridmapdir

Obsoletes: emi-dpm_mysql

%description -n dpm_head
The LCG Disk Pool Manager (DPM) creates a storage element from a set
of disks. It provides several interfaces for storing and retrieving
data such as HTTP, Xrootd, GridFTP

%package -n dpm_disk
Summary:	EMI DPM Disk Node
Group:		Applications/Internet
Requires:	dpm%{?_isa} = %{version}
Requires:	dpm-devel%{?_isa} = %{version}
Requires:	dpm-dsi%{?_isa}
Requires:	dpm-perl%{?_isa} = %{version}
Requires:	dpm-python%{?_isa} = %{version}
Requires:	dpm-rfio-server%{?_isa} = %{version}
Requires:	dpm-yaim >= 4.2.20
Requires:	dmlite-plugins-adapter >= 0.4.0
Requires:	edg-mkgridmap
Requires:       emi-version
Requires:	fetch-crl
Requires:	finger
Requires:	lcgdm-dav
Requires:	lcgdm-dav-server
Requires:	lcg-expiregridmapdir

Obsoletes: emi-dpm_disk

%description -n dpm_disk
The LCG Disk Pool Manager (DPM) creates a storage element from a set
of disks. It provides several interfaces for storing and retrieving
data such as RFIO and SRM version 1, version 2 and version 2.2.
This is a virtual package providing all required daemons for a DPM
Disk Node. 

%prep

%build

%install


%files -n dpm_head

%files -n dpm_disk

%changelog
* Thu Jun 16 2016 Fabrizio Furano <fabrizio.furano@cern.ch> - 1.9.0-1
- First release of the new dpm main package
