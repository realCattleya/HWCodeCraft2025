#include "include/struct.h"


float Object::get_score(int obj_offset, int time, const std::unordered_set<int> &offsets) {
    float score = 0;
    for (auto rq: active_requests) {
        score += rq->get_score(obj_offset, time, offsets);
    }
    return score;
}