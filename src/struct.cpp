#include "include/struct.h"
#include "include/controller.h"

#include <cmath>


void Disk::op_start() {
    left_tokens = _controller->get_available_tokens();
    _actions.clear();
    _is_jmp = false;
}

void Disk::op_jump(int offset) {
    // 更新 head_pos 为 jump_target 后的下一个位置
    assert(left_tokens == _controller->get_available_tokens() && "Invalid jump operation: tokens insufficient");
    assert(offset >= 1 && offset <= capacity && "Invalid jump operation: offset error");
    head_pos = offset;
    _actions += ("j " + std::to_string(offset));
    _is_jmp = true;
}

void Disk::op_read() {
    // int last_read_cost = (pre_tokens > 0) ? pre_tokens : 0;
    int cost = (last_read_cost == 0) ? 64 : std::max(16, int(std::ceil(float(last_read_cost) * 0.8)));
    assert(cost <= left_tokens && "Invalid read operation: tokens insufficient");
    left_tokens -= cost;
    pre_tokens += 1;
    last_read_cost = cost;
    _step();
    _actions += "r";
}

void Disk::op_pass() {
    assert(left_tokens >= 1 && "Invalid pass operation: tokens insufficient");
    left_tokens -= 1;
    pre_tokens = 0;
    last_read_cost = 0;
    _step();
    _actions += "p";
}

void Disk::op_finish() {
    if (!_is_jmp)
    {
        _actions += "#";
    }
    
}

void Disk::_step() {
    head_pos++;
    if (head_pos > capacity) {
        head_pos = 1;
    }
}