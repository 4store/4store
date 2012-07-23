#!/usr/bin/perl
#
# Script to build a static binary of 4store, including its dependancies
#

use File::Basename;
use Cwd;
use strict;
use warnings;

my $TOP_DIR = Cwd::realpath(dirname(__FILE__).'/..');
my $BUILD_DIR = "$TOP_DIR/build";
my $ROOT_DIR = "$TOP_DIR/root";
my $DEFAULT_CONFIGURE_ARGS = "--enable-static --disable-shared --prefix=$ROOT_DIR ".
                             "--disable-gtk-doc --disable-dependency-tracking ".
                             "--disable-rebuilds";

my $packages = [
    {
        'url' => 'http://pkgconfig.freedesktop.org/releases/pkg-config-0.25.tar.gz',
        'md5' => 'a3270bab3f4b69b7dc6dbdacbcae9745',
        'config' => "./configure $DEFAULT_CONFIGURE_ARGS --with-pc-path=${ROOT_DIR}/lib/pkgconfig",
        'checkfor' => ['bin/pkg-config', 'share/aclocal/pkg.m4']
    },
    {
        # libuuid
        'url' => 'http://www.kernel.org/pub/linux/utils/util-linux/v2.21/util-linux-2.21.2.tar.gz',
        'md5' => 'b228170ecdfce9ced77313d57b21b37c',
        'make' => "cd libuuid && make",
        'install' => "cd libuuid && make install",
        'checkfor' => ['include/uuid/uuid.h', 'lib/libuuid.a', 'lib/pkgconfig/uuid.pc'],
    },
    {
        'url' => 'http://curl.haxx.se/download/curl-7.26.0.tar.bz2',
        'md5' => 'bfa80f01b3d300359cfb4d409b6136a3',
        'config' => "./configure $DEFAULT_CONFIGURE_ARGS ".
                    "--disable-ssh --disable-ldap --disable-ldaps --disable-rtsp ".
                    "--without-librtmp --disable-dict --disable-telnet --disable-pop3 ".
                    "--disable-imap --disable-smtp --disable-manual --without-libssh2",
        'checkfor' => ['include/curl/curl.h', 'lib/libcurl.a', 'lib/pkgconfig/libcurl.pc'],
    },
    {
        'url' => 'http://kent.dl.sourceforge.net/project/pcre/pcre/8.30/pcre-8.30.tar.bz2',
        'md5' => '98e8928cccc945d04279581e778fbdff',
        'config' => "./configure $DEFAULT_CONFIGURE_ARGS --enable-utf8 ".
                    "--enable-unicode-properties --enable-pcregrep-libz ".
                    "--enable-pcregrep-libbz2",
        'checkfor' => ['include/pcre.h', 'lib/libpcre.a', 'lib/pkgconfig/libpcre.pc']
    },
    {
        'url' => 'http://xmlsoft.org/sources/libxml2-2.8.0.tar.gz',
        'md5' => 'c62106f02ee00b6437f0fb9d370c1093',
        'checkfor' => ['include/libxml2', 'lib/libxml2.a', 'lib/pkgconfig/libxml-2.0.pc'],
    },
    {
        'url' => 'http://xmlsoft.org/sources/libxslt-1.1.26.tar.gz',
        'md5' => 'e61d0364a30146aaa3001296f853b2b9',
        # Hack to fix broken compile on Mac OS 10.5 SDK
        'make' => 'touch libxslt/ansidecl.h && make',
        'checkfor' => ['include/libxslt/xslt.h', 'lib/libxslt.a', 'lib/pkgconfig/libxslt.pc'],
    },
    {
        'url' => 'http://ftp.gnu.org/pub/gnu/gettext/gettext-0.18.1.1.tar.gz',
        'md5' => '3dd55b952826d2b32f51308f2f91aa89',
        'patch' => 'gettext.patch',
        'config' => "./configure $DEFAULT_CONFIGURE_ARGS --disable-debug ".
                    "--without-included-gettext --without-included-glib ".
                    "--without-included-libcroco --without-included-libxml ".
                    "--without-emacs --disable-java",
        'checkfor' => ['bin/gettext', 'include/gettext-po.h', 'lib/libgettextlib.la'],
    },
#     {
#         'url' => 'http://ftp.gnu.org/gnu/readline/readline-6.2.tar.gz',
#         'checkfor' => 'lib/libreadline.a',
#     },
    {
        'url' => 'http://ftp.gnome.org/pub/gnome/sources/glib/2.28/glib-2.28.8.tar.bz2',
        'md5' => '789e7520f71c6a4bf08bc683ec764d24',
        'patch' => 'glib2.patch',
        'config' => "./configure $DEFAULT_CONFIGURE_ARGS --with-pcre=system --disable-selinux && ".
                    "ed - config.h < ${TOP_DIR}/app-aux/glib2-config.h.ed",
        'checkfor' => ['include/glib-2.0/glib.h', 'lib/libglib-2.0.a', 'lib/pkgconfig/glib-2.0.pc'],
    },
    {
        'url' => 'http://github.com/lloyd/yajl/tarball/2.0.4',
        'md5' => 'ee6208e697c43dcccf798ce80d370379',
        'dirname' => 'lloyd-yajl-fee1ebe',
        'tarname' => 'yajl-2.0.4.tar.gz',
        'config' => "mkdir build && cd build && cmake ..",
        'make' => "cd build && make yajl_s",
        'install' => "cd build/yajl-2.0.4 && ".
                     "cp -Rfv include/yajl ${ROOT_DIR}/include/ && ".
                     "cp -fv lib/libyajl_s.a ${ROOT_DIR}/lib/libyajl.a",
        'checkfor' => ['include/yajl/yajl_common.h', 'lib/libyajl.a'],
    },
    {
        'url' => 'http://download.librdf.org/source/raptor2-2.0.8.tar.gz',
        'md5' => 'ac60858b875aab8fa7917f21a1237aa9',
        'config' => "./configure $DEFAULT_CONFIGURE_ARGS --with-yajl=${ROOT_DIR}",
        'checkfor' => ['lib/libraptor2.a', 'include/raptor2/raptor2.h', 'lib/pkgconfig/raptor2.pc'],
    },
    {
        'url' => 'http://download.librdf.org/source/rasqal-0.9.29.tar.gz',
        'md5' => '49e4b75a0c67465edf55dd20606715fa',
        'config' => "./configure $DEFAULT_CONFIGURE_ARGS ".
                    "--with-regex-library=pcre --enable-raptor2",
        'checkfor' => ['include/rasqal/rasqal.h', 'lib/librasqal.a', 'lib/pkgconfig/rasqal.pc'],
    },
    {
        'name' => '4store',
        'dirpath' => $TOP_DIR,
        'test' => 'make check',
        'checkfor' => ['bin/4s-query', 'include/4store/4store.h', 'lib/lib4store.la', 'lib/pkgconfig/4store-0.pc'],
        'alwaysbuild' => 1,
    },
];

# Reset environment variables
$ENV{'CFLAGS'} = "-O2 -I${ROOT_DIR}/include";
$ENV{'CPPFLAGS'} = "-I${ROOT_DIR}/include";
$ENV{'ASFLAGS'} = "-I${ROOT_DIR}/include";
$ENV{'LDFLAGS'} = "-L${ROOT_DIR}/lib";
$ENV{'INFOPATH'} = "${ROOT_DIR}/share/info";
$ENV{'MANPATH'} = "${ROOT_DIR}/share/man";
$ENV{'M4PATH'} = "${ROOT_DIR}/share/aclocal";
$ENV{'PATH'} = "${ROOT_DIR}/bin:/usr/bin:/bin:/sbin:/usr/local/bin";
$ENV{'PKG_CONFIG_PATH'} = "${ROOT_DIR}/lib/pkgconfig";
$ENV{'CLASSPATH'} = '';

# Check tools required are available
my @TOOLS_REQUIRED = ('cmake', 'curl', 'ed', 'make', 'md5', 'patch', 'tar');
foreach my $cmd (@TOOLS_REQUIRED) {
  system("which $cmd > /dev/null") && die "Error: $cmd is not available on this system.";
}

# Add extra CFLAGS if this is Mac OS X
if (`uname` =~ /^Darwin/) {
    die "Mac OS X Developer Tools are not available." unless (-e '/Developer/');

    # Build x86_64 only binary against 10.5+
    my $SDK;
    my $SDKver;
    for my $ver ('10.5', '10.6') {
	if (-d "/Developer/SDKs/MacOSX${ver}.sdk") {
	    $SDKver = $ver;
            $SDK = "/Developer/SDKs/MacOSX${ver}.sdk";
	    last;
	}
    }
    die "Mac OS X SDK is not available." unless ($SDK);
    my $ARCHES = '-arch x86_64';
    my $MINVER = "-mmacosx-version-min=$SDKver";

    $ENV{'CFLAGS'} .= " -isysroot $SDK $ARCHES $MINVER";
    $ENV{'LDFLAGS'} .= " -Wl,-syslibroot,$SDK $ARCHES $MINVER";
    $ENV{'CFLAGS'} .= " -force_cpusubtype_ALL";
    $ENV{'LDFLAGS'} .= " -Wl,-headerpad_max_install_names";
    $ENV{'MACOSX_DEPLOYMENT_TARGET'} = $SDKver;
    $ENV{'CMAKE_OSX_ARCHITECTURES'} = 'x86_64';
    $ENV{'CMAKE_OSX_SYSROOT'} = $SDK;

    my $GCC_VER = '4.2';
    $ENV{'CC'} = "/Developer/usr/bin/gcc-$GCC_VER";
    $ENV{"CPP"} = "/Developer/usr/bin/cpp-$GCC_VER";
    $ENV{"CXX"} = "/Developer/usr/bin/g++-$GCC_VER";
    die "gcc version $GCC_VER is not available." unless (-e $ENV{'CC'});
}

$ENV{'CXXFLAGS'} = $ENV{'CFLAGS'};

print "Build directory: $BUILD_DIR\n";
mkdir($BUILD_DIR);

print "Root directory: $ROOT_DIR\n";
mkdir($ROOT_DIR);
mkdir($ROOT_DIR.'/bin');
mkdir($ROOT_DIR.'/include');
mkdir($ROOT_DIR.'/lib');
mkdir($ROOT_DIR.'/share');

gtkdoc_hack($ROOT_DIR);


foreach my $pkg (@$packages) {
    if (defined $pkg->{'url'} and !defined $pkg->{'tarname'}) {
        ($pkg->{'tarname'}) = ($pkg->{'url'} =~ /([^\/]+)$/);
    }
    if (defined $pkg->{'tarname'} and !defined $pkg->{'tarpath'}) {
        $pkg->{'tarpath'} = $BUILD_DIR.'/'.$pkg->{'tarname'};
    }
    if (defined $pkg->{'tarname'} and !defined $pkg->{'dirname'}) {
        ($pkg->{'dirname'}) = ($pkg->{'tarname'} =~ /^([\w\.\-]+[\d\.\-]+\d)/);
        $pkg->{'dirname'} =~ s/_/\-/g;
    }
    if (defined $pkg->{'dirname'} and !defined $pkg->{'dirpath'}) {
        $pkg->{'dirpath'} = $BUILD_DIR.'/'.$pkg->{'dirname'};
    }
    if (defined $pkg->{'dirname'} and !defined $pkg->{'name'}) {
        $pkg->{'name'} = $pkg->{'dirname'};
    }

    if ($pkg->{'alwaysbuild'} or check_installed($pkg) == 0) {
        download_package($pkg) if (defined $pkg->{'url'});
        check_digest($pkg) if (defined $pkg->{'tarpath'});
        extract_package($pkg) if (defined $pkg->{'tarpath'});
        clean_package($pkg);
        patch_package($pkg);
        config_package($pkg);
        make_package($pkg);
        test_package($pkg);
        install_package($pkg);

        if (check_installed($pkg) == 0) {
            die "Building $pkg->{'name'} failed.";
        }
    }
}

print "Finished compiling:\n";
foreach my $pkg (sort {$a->{'name'} cmp $b->{'name'}} @$packages) {
    print " * ".$pkg->{'name'}."\n";
}


sub check_installed {
    my ($pkg) = @_;
    my $checkfor = $pkg->{'checkfor'};

    unless (defined $checkfor and $checkfor) {
        die "Don't know how to check if ".$pkg->{'name'}." is built.";
    }

    print "Checking if ".$pkg->{'name'}." is built correctly.\n";
    $checkfor = [$checkfor] unless (ref $checkfor eq 'ARRAY');
    foreach (@$checkfor) {
        my $path = $ROOT_DIR . '/' . $_;
        unless (-e $path) {
            print "  No - $path is missing.\n";
            return 0;
        }
    }

    # Everything exists
    print "  Yes.\n";
    return 1;
}

sub download_package {
    my ($pkg) = @_;

    unless (-f $pkg->{'tarpath'}) {
        safe_chdir();
        print "Downloading: ".$pkg->{'tarname'}."\n";
        safe_system('curl', '-L', '-k', '-o', $pkg->{'tarpath'}, $pkg->{'url'});
    }
}

sub check_digest {
    my ($pkg) = @_;

    print "Checking digest for: ".$pkg->{'tarname'}."\n";
    if ($pkg->{'md5'}) {
        if (`md5 $pkg->{'tarpath'}` =~ / = ([0-9a-f]{32})$/) {
            if ($pkg->{'md5'} ne $1) {
                warn "MD5 digests don't match.\n";
                warn "  Expected: $pkg->{'md5'}\n";
                warn "  Actual: $1\n";
                exit(-1);
            }
        } else {
            die "  Unable to calculate MD5 digest for downloaded package.\n";
        }
    } else {
        die "  Error: no digest defined.\n";
    }
}

sub extract_package {
    my ($pkg) = @_;
    if (-e $pkg->{'dirpath'}) {
        print "Deleting old: $pkg->{'dirpath'}\n";
        safe_system('rm', '-Rf', $pkg->{'dirpath'});
    }

    safe_chdir();
    print "Extracting: $pkg->{'tarname'} into $pkg->{'dirpath'}\n";
    if ($pkg->{'tarname'} =~ /bz2$/) {
        safe_system('tar', '-jxf', $pkg->{'tarpath'});
    } elsif ($pkg->{'tarname'} =~ /gz$/) {
        safe_system('tar', '-zxf', $pkg->{'tarpath'});
    } else {
        die "Don't know how to decomress archive.";
    }
}

sub clean_package {
    my ($pkg) = @_;

    safe_chdir($pkg->{'dirpath'});
    print "Cleaning: ".$pkg->{'name'}."\n";
    if ($pkg->{'clean'}) {
        system($pkg->{'clean'});
    } else {
        # this is allowed to fail
        system('make', 'clean') if (-e 'Makefile');
    }
}

sub patch_package {
    my ($pkg) = @_;
    if ($pkg->{'patch'}) {
        safe_chdir($pkg->{'dirpath'});
        my $patchfile = $TOP_DIR.'/app-aux/'.$pkg->{'patch'};
        safe_system("patch -p0 < $patchfile");
    }
}

sub config_package {
    my ($pkg) = @_;

    safe_chdir($pkg->{'dirpath'});
    print "Configuring: ".$pkg->{'name'}."\n";
    if ($pkg->{'config'}) {
        safe_system($pkg->{'config'});
    } else {
        if (-e "./configure") {
          safe_system("./configure $DEFAULT_CONFIGURE_ARGS");
        } elsif (-e "./autogen.sh") {
          safe_system("./autogen.sh $DEFAULT_CONFIGURE_ARGS");
        } else {
          die "Don't know how to configure ".$pkg->{'name'};
        }
    }
}

sub make_package {
    my ($pkg) = @_;

    safe_chdir($pkg->{'dirpath'});
    print "Making: ".$pkg->{'name'}."\n";
    if ($pkg->{'make'}) {
        safe_system($pkg->{'make'});
    } else {
        safe_system('make');
    }
}

sub test_package {
    my ($pkg) = @_;

    safe_chdir($pkg->{'dirpath'});
    if ($pkg->{'test'}) {
        print "Testing: ".$pkg->{'name'}."\n";
        safe_system($pkg->{'test'});
    }
}

sub install_package {
    my ($pkg) = @_;

    safe_chdir($pkg->{'dirpath'});
    print "Installing: ".$pkg->{'name'}."\n";
    if ($pkg->{'install'}) {
        safe_system($pkg->{'install'});
    } else {
        safe_system('make','install');
    }
}

sub safe_chdir {
    my ($dir) = @_;
    $dir = $BUILD_DIR unless defined $dir;
    print "Changing to: $dir\n";
    chdir($dir) or die "Failed to change directory: $!";
}

sub safe_system {
    my (@cmd) = @_;
    print "Running: ".join(' ',@cmd)."\n";
    if (system(@cmd)) {
        die "Command failed";
    }
}


# HACK to fix bad gtkdoc detection
sub gtkdoc_hack {
    my ($dir) = @_;
    my $script = "$dir/bin/gtkdoc-rebase";

    open(SCRIPT, ">$script") or die "Failed to open $script: $!";
    print SCRIPT "#/bin/sh\n";
    close(SCRIPT);

    chmod(0755, $script) or die "Failed to chmod 0755 $script: $!";
}
