#ifndef TIMING_H
#define TIMING_H

//#define TIMING

#ifdef TIMING
#define TIME(t) ts_timelog(t)
#else
#define TIME(t)
#endif

void ts_timelog(const char *tag);

void ts_timing_add(const char *tag, const double diff, const int cnt);

#define TS_TIMING_REPORT ((char *)ts_timing_report)
extern const char *ts_timing_report;

#endif
