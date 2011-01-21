#!/bin/bash

rm -rf 4store.app
mkdir -p 4store.app/Contents/Resources 4store.app/Contents/MacOS/{bin,lib}
bv=`cat app-aux/bundle-version`
bv=`expr $bv + 1`
echo $bv > app-aux/bundle-version
cp app-aux/Info.plist 4store.app/Contents/
sed -e 's/${BV}/'$bv'/' -i "" 4store.app/Contents/Info.plist
version=`git describe --tags --always | sed 's/^v//; s/-.*//'`
sed -e 's/${AV}/'$version'/' -i "" 4store.app/Contents/Info.plist
cp -r app-aux/Resources/* 4store.app/Contents/Resources/
cp app-aux/MacOS/* 4store.app/Contents/MacOS/
for i in src/frontend/4s-* src/backend/4s-* src/utilities/4s-{backend,cluster}-* src/utilities/4s-{dump,restore} src/http/4s-*; do
	if test -x $i ; then
		cp $i 4store.app/Contents/MacOS/bin/
	fi
done
cp -p /usr/local/lib/librasqal*.dylib /usr/local/lib/libraptor2*.dylib \
	/sw/lib/libglib-2*.dylib /sw/lib/libintl*.dylib \
	/sw/lib/libiconv*.dylib /sw/lib/libpcre*.dylib \
	/sw/lib/libintl*.dylib /sw/lib/libiconv*.dylib \
	/sw/lib/libpcre*.dylib /sw/lib/libreadline*.dylib \
	/sw/lib/ncurses/libncurses*.dylib /sw/lib/libgthread-*.dylib \
	4store.app/Contents/MacOS/lib/
