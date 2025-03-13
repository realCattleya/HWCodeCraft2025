#include "./include/controller.h"
#include "./include/utils.h"
#include <iostream>
using namespace std;

int main() {
    // 读取全局输入
    int T, M, N, V, G;
    cin >> T >> M >> N >> V >> G;
    
    auto fre_del = read_matrix(T, M);
    auto fre_write = read_matrix(T, M);
    auto fre_read = read_matrix(T, M);
    
    StorageController controller(T, M, N, V, G, fre_del, fre_write, fre_read);
    controller.pre_process();

    for (int t=1; t<=T+EXTRA_TIME; t++) {
        controller.timestamp_align();
        controller.delete_action();
        controller.write_action();
        controller.read_action();
    }
    
    return 0;
}

