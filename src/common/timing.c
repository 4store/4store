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
/*
 *  Copyright (C) 2006 Steve Harris for Garlik
 */

#include <stdlib.h>
#include <stdio.h>
#include <sys/time.h>

#include "timing.h"

#define HASH_SIZE 1024

const char *ts_timing_report = "X-REPORT";

static double sum[HASH_SIZE];
static int count[HASH_SIZE];
static char *string[HASH_SIZE];

void ts_timelog(const char *tag)
{
    static struct timeval t0 = {0, 0};
    static struct timeval base = {0, 0};
    struct timeval now;

    gettimeofday(&now, 0);

    if (t0.tv_sec == 0) {
	t0.tv_sec = now.tv_sec;
	t0.tv_usec = now.tv_usec;
    }

    if (tag == ts_timing_report) {
	int i;
	double diff = (now.tv_sec - t0.tv_sec) +
			(now.tv_usec - t0.tv_usec) * 0.000001;
	double pc = 0.0;

	for (i=0; i<HASH_SIZE; i++) {
	    if (string[i]) {
		double this_pc = -1.0;

		if (count[i] > 0) {
		    this_pc = 100.0*sum[i]/diff;
		    pc += this_pc;
		}

		if (this_pc > 0.0) {
		    printf("[T] %9.3fs %2d%% %s (%d)\n", sum[i], (int)(this_pc), string[i], abs(count[i]));
		} else {
		    printf("[T] %9.3fs     %s (%d)\n", sum[i], string[i], abs(count[i]));
		}
	    }
	}
	printf("[T] %9.3fs %2d%% TOTAL\n", diff, (int)pc);
    } else if (tag) {
	double diff = (now.tv_sec - base.tv_sec) +
			(now.tv_usec - base.tv_usec) * 0.000001;

	ts_timing_add(tag, diff, 1);
    }

    base.tv_sec = now.tv_sec;
    base.tv_usec = now.tv_usec;
}

void ts_timing_add(const char *tag, const double diff, const int cnt)
{
    unsigned long hash = ((unsigned long)tag / 4) & (HASH_SIZE - 1);

    if (!string[hash]) {
	string[hash] = (char *)tag;
    }
    sum[hash] += diff;
    count[hash] += cnt;
}

/* vi:set ts=8 sts=4 sw=4: */
