#ifndef __M_LOG_H__
#define __M_LOG_H__
#include <iostream>
#include <ctime>

#define DBG_LEVEL 0
#define INF_LEVEL 1
#define ERR_LEVEL 2
#define DEFAULT_LEVEL DBG_LEVEL
#define LOG(lev_str, level, format, ...) {\
    if (level >= DEFAULT_LEVEL) {\
        time_t t = time(nullptr);\
        struct tm tm_buf;\
        /* localtime 不可重入（返回静态缓冲区），多线程并发打日志会互相覆盖；用线程安全版本 */\
        struct tm* ptm = localtime_r(&t, &tm_buf);\
        char time_str[32];\
        strftime(time_str, 31, "%H:%M:%S", ptm);\
        printf("[%s][%s][%s:%d]\t" format "\n", lev_str, time_str, __FILE__, __LINE__, ##__VA_ARGS__);\
    }\
}

#define DLOG(format, ...) LOG("DBG", DBG_LEVEL, format, ##__VA_ARGS__)
#define ILOG(format, ...) LOG("INF", INF_LEVEL, format, ##__VA_ARGS__)
#define ELOG(format, ...) LOG("ERR", ERR_LEVEL, format, ##__VA_ARGS__)
#endif