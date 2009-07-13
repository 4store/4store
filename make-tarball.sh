#!

revision=`git describe --tags --always`
name="4store-${revision}"

(cd src && make clean)
rm -f tests/{query,import}/results/*
rm -rf /tmp/${name}
cp -r . /tmp/${name}
echo "export gitrev = ${revision}" > /tmp/${name}/src/rev.mk
(cd /tmp && tar cvfz ${name}.tar.gz -h --exclude .git --exclude .gitignore --exclude dawg-tests --exclude '*.tar.gz' ${name})
mv /tmp/${name}.tar.gz .
rm -rf /tmp/${name}
