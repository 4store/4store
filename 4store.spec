Summary: 4store RDF storage and SPARQL query engine
Name: 4store
Version: 1.0
Release: 1
Source0: %{name}-%{version}.tar.gz
License: GPLv3+
Group: Applications/Databases
BuildRoot: %{_builddir}/%{name}-root
Prefix: /usr/local

%description
4store is a distributed RDF storage engine and SPARQL query engine. 4store runs
on a single node, or a loose cluster of machines.
%prep
%setup -q
%build
make %{_smp_mflags}
%install
rm -rf %{buildroot}
make DESTDIR=%{buildroot} install
%clean
rm -rf %{buildroot}
%files
%defattr(-,root,root)
/usr/local/bin/
%doc /usr/local/share/man/man1/*
