#!/bin/bash

rm -rf 4store.app
mkdir -p 4store.app/Contents/Resources 4store.app/Contents/MacOS/bin
bv=`cat app-aux/bundle-version`
bv=`expr $bv + 1`
echo $bv > app-aux/bundle-version
cp app-aux/Info.plist 4store.app/Contents/
sed -e 's/${BV}/'$bv'/' -i "" 4store.app/Contents/Info.plist
version=`git describe --tags --always | sed 's/^v//; s/-.*//'`
sed -e 's/${AV}/'$version'/' -i "" 4store.app/Contents/Info.plist
cp -r app-aux/Resources/* 4store.app/Contents/Resources/
cp app-aux/MacOS/* 4store.app/Contents/MacOS/
for i in root/bin/4s-*; do
	if test -x $i ; then
		cp $i 4store.app/Contents/MacOS/bin/
	fi
done
