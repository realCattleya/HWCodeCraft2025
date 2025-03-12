#pragma once
#include <iostream> 
#include <vector>
#include "config.h"
using namespace std;

std::vector<std::vector<int>> read_matrix(int T, int rows) {
    vector<vector<int>> mat(rows);
    int cnt = (T + FRE_PER_SLICING - 1) / FRE_PER_SLICING;
    for (int i = 0; i < rows; ++i) {
        mat[i].resize(cnt);
        for (int j = 0; j < cnt; ++j)
            cin >> mat[i][j];
    }
    return mat;
};