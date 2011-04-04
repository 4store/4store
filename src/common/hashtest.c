/*
    4store - a clustered RDF storage and query engine

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/
#include "4s-hash.h"

#define ITS 1000000

int main()
{
	char *teststr[80] = {
		"http://foo.bar.baz/qux/quux#ln_10269874325342",
		"http://foo.bar.baz/qux/quux#ln_10269874325343",
		"http://foo.bar.baz/qux/quux#ln_10269874325344",
		"http://foo.bar.baz/qux/quux#ln_10269874325345",
		"http://foo.bar.baz/qux/quux#ln_10269874325346",
		"http://foo.bar.baz/qux/quux#ln_10269874325347",
		"http://foo.bar.baz/qux/quux#ln_10269874325348",
		"http://foo.bar.baz/qux/quux#ln_10269874325349",
		"http://bar.bar.baz/qux/quux#ln_10269874325342",
		"http://bar.bar.baz/qux/quux#ln_10269874325343",
		"http://bar.bar.baz/qux/quux#ln_10269874325344",
		"http://bar.bar.baz/qux/quux#ln_10269874325345",
		"http://bar.bar.baz/qux/quux#ln_10269874325346",
		"http://bar.bar.baz/qux/quux#ln_10269874325347",
		"http://bar.bar.baz/qux/quux#ln_10269874325348",
		"http://bar.bar.baz/qux/quux#ln_10269874325349",
		"http://baz.bar.baz/qux/quux#ln_10269874325342",
		"http://baz.bar.baz/qux/quux#ln_10269874325343",
		"http://baz.bar.baz/qux/quux#ln_10269874325344",
		"http://baz.bar.baz/qux/quux#ln_10269874325345",
		"http://baz.bar.baz/qux/quux#ln_10269874325346",
		"http://baz.bar.baz/qux/quux#ln_10269874325347",
		"http://baz.bar.baz/qux/quux#ln_10269874325348",
		"http://baz.bar.baz/qux/quux#ln_10269874325349",
		"http://qux.bar.baz/qux/quux#ln_10269874325342",
		"http://qux.bar.baz/qux/quux#ln_10269874325343",
		"http://qux.bar.baz/qux/quux#ln_10269874325344",
		"http://qux.bar.baz/qux/quux#ln_10269874325345",
		"http://qux.bar.baz/qux/quux#ln_10269874325346",
		"http://qux.bar.baz/qux/quux#ln_10269874325347",
		"http://qux.bar.baz/qux/quux#ln_10269874325348",
		"http://qux.bar.baz/qux/quux#ln_10269874325349",
		"http://quux.bar.baz/qux/quux#ln_10269874325342",
		"http://quux.bar.baz/qux/quux#ln_10269874325343",
		"http://quux.bar.baz/qux/quux#ln_10269874325344",
		"http://quux.bar.baz/qux/quux#ln_10269874325345",
		"http://quux.bar.baz/qux/quux#ln_10269874325346",
		"http://quux.bar.baz/qux/quux#ln_10269874325347",
		"http://quux.bar.baz/qux/quux#ln_10269874325348",
		"http://quux.bar.baz/qux/quux#ln_10269874325349",
		"https://foo.bar.baz/qux/quux#ln_10269874325342",
		"https://foo.bar.baz/qux/quux#ln_10269874325343",
		"https://foo.bar.baz/qux/quux#ln_10269874325344",
		"https://foo.bar.baz/qux/quux#ln_10269874325345",
		"https://foo.bar.baz/qux/quux#ln_10269874325346",
		"https://foo.bar.baz/qux/quux#ln_10269874325347",
		"https://foo.bar.baz/qux/quux#ln_10269874325348",
		"https://foo.bar.baz/qux/quux#ln_10269874325349",
		"https://bar.bar.baz/qux/quux#ln_10269874325342",
		"https://bar.bar.baz/qux/quux#ln_10269874325343",
		"https://bar.bar.baz/qux/quux#ln_10269874325344",
		"https://bar.bar.baz/qux/quux#ln_10269874325345",
		"https://bar.bar.baz/qux/quux#ln_10269874325346",
		"https://bar.bar.baz/qux/quux#ln_10269874325347",
		"https://bar.bar.baz/qux/quux#ln_10269874325348",
		"https://bar.bar.baz/qux/quux#ln_10269874325349",
		"https://baz.bar.baz/qux/quux#ln_10269874325342",
		"https://baz.bar.baz/qux/quux#ln_10269874325343",
		"https://baz.bar.baz/qux/quux#ln_10269874325344",
		"https://baz.bar.baz/qux/quux#ln_10269874325345",
		"https://baz.bar.baz/qux/quux#ln_10269874325346",
		"https://baz.bar.baz/qux/quux#ln_10269874325347",
		"https://baz.bar.baz/qux/quux#ln_10269874325348",
		"https://baz.bar.baz/qux/quux#ln_10269874325349",
		"https://qux.bar.baz/qux/quux#ln_10269874325342",
		"https://qux.bar.baz/qux/quux#ln_10269874325343",
		"https://qux.bar.baz/qux/quux#ln_10269874325344",
		"https://qux.bar.baz/qux/quux#ln_10269874325345",
		"https://qux.bar.baz/qux/quux#ln_10269874325346",
		"https://qux.bar.baz/qux/quux#ln_10269874325347",
		"https://qux.bar.baz/qux/quux#ln_10269874325348",
		"https://qux.bar.baz/qux/quux#ln_10269874325349",
		"https://quux.bar.baz/qux/quux#ln_10269874325342",
		"https://quux.bar.baz/qux/quux#ln_10269874325343",
		"https://quux.bar.baz/qux/quux#ln_10269874325344",
		"https://quux.bar.baz/qux/quux#ln_10269874325345",
		"https://quux.bar.baz/qux/quux#ln_10269874325346",
		"https://quux.bar.baz/qux/quux#ln_10269874325347",
		"https://quux.bar.baz/qux/quux#ln_10269874325348",
		"https://quux.bar.baz/qux/quux#ln_10269874325349",
	};

	fs_hash_init(FS_HASH_UMAC);

	printf("hash(<%s>) = %016llx\n", teststr[0], fs_hash_uri(teststr[0]));
	printf("hash('%s', 0x0) = %016llx\n", teststr[0], fs_hash_literal(teststr[0], 0));
	printf("hash('%s', 0x12345678) = %016llx\n", teststr[0], fs_hash_literal(teststr[0], 0x12345678));
	printf("hash('%s', 0x12345679) = %016llx\n", teststr[0], fs_hash_literal(teststr[0], 0x12345679));
	printf("hash('%s', 0x12345678) = %016llx\n", teststr[0], fs_hash_literal(teststr[0], 0x12345678));
	double then = fs_time();
	for (int i=0; i<ITS; i++) {
		fs_hash_uri(teststr[i % 8]);
	}
	double now = fs_time();

	printf("%f us/hash\n", 1000000 * (now-then) / (double)ITS);

	return 0;
}
