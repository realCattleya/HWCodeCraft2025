#pragma once
#define MAX_DISK_NUM (10)
#define MAX_DISK_SIZE (16384)
#define MAX_REQUEST_NUM (30000000)
#define MAX_OBJECT_NUM (100000)
#define REP_NUM (3)
#define FRE_PER_SLICING (1800)
#define EXTRA_TIME (105)
#define MAX_TOKENS_NUM (1000)


// parameters
#define TOP_K_RATE (0.95)
#define IS_RANDOM_LATIN (1)
#define IS_PAIR_WISE_WRITE (1)
#define WINDOW_SIZE (V/M)
#define JUMP_TRIGGER (0)
#define BUFF_RATE (0.05)

// 跳转时机，当dp[0]==0时不受影响
#define JUMP_START_TS (10000)
#define JUMP_END_TS (T-10000)
#define JUMP_THRES (G / 60)