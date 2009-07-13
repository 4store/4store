#!

revision=`git describe --tags --always`
name="4store-${revision}"

(cd src && make clean)
cp -r src ${name}
echo "export gitrev = ${revision}r" > ${name}/rev.mk
tar cvfz ${name}.tar.gz -h --exclude .git ${name}
rm -rf ${name}
