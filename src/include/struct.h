#pragma once
#include "config.h"
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <string>
#include <iostream>
#include <algorithm>
#include <deque>

struct ReadRequest;

struct SortItem {
    int disk_id;
    int current_size;

    SortItem(int did) {
        disk_id = did;
        current_size = 0;
    }
};

struct Unit {
    int unit_id;
    int obj_id;
    int obj_offset;
};

typedef struct Bound_ {
    int lower;
    int upper;
    bool is_reverse;
    Bound_(){}
    Bound_(int lower,int upper, bool is_reverse) :
    lower(lower), upper(upper), is_reverse(is_reverse){}
} Bound;

struct Disk {
    int id;
    int capacity;
    int used_capcity = 0;
    int head_pos = 1;
    int pre_tokens;
    int curr_tag_to_read;
    int used_units_cnt = 0;
    std::string last_action;
    // std::unordered_map<int, Unit *> used_units;
    Unit *units;
    std::unordered_map<int,std::deque<int>> unique_hot_tags_circular_que; // 指示前TOPK热点区的循环队列
    std::unordered_map<int, Bound> tag_bounds;  // 记录每个 tag 的分区， is_reverse 表示需要从分区高位逆着放对象
    std::unordered_map<int, int> tag_last_allocated; // 记录每个 tag 最近一次分配的位置
    std::unordered_map<int, int> tag_continuous;     // 记录每个 tag 当前连续写入的长度

    // Disk(){}
    Disk(int id, int v) : id(id), capacity(v) {
        curr_tag_to_read = -1;
        units = new Unit[v + 1];
        auto p = units;
        for (int i = 0; i <= v; ++i) {
            p->unit_id = i;
            p->obj_id = -1;
            p++;
        }
    }

    ~Disk() {
        delete units;
    }

    void pair_wise_partition_units(std::vector<std::pair<int,long long>> tags_size_sum){
        double buffer_rate = BUFF_RATE;
        std::sort(tags_size_sum.begin(), tags_size_sum.end(), 
        [](const std::pair<int, long long>& a, const std::pair<int, long long>& b) {
            return a.second > b.second; // 降序排序
        });
        long long total = 0;
        int idx = 0;
        int M = tags_size_sum.size();
        std::vector<long long> tmp;
        // M % 2 == 1 时没考虑， 出BUG了记得排查（
        for(idx=0;idx<M/2;idx++){
            total += tags_size_sum[idx].second + tags_size_sum[M-idx-1].second;
            tmp.push_back(total);
        }
        // 前 buffer_rate 的空间作为缓存区
        int last_bound = int(capacity*buffer_rate);
        tag_bounds[0] = Bound(1,int(capacity*buffer_rate),false);
        for(idx=0;idx<M/2;idx++){
            int bound = int(double(tmp[idx])/double(total)*(1-buffer_rate)*double(capacity)+capacity*buffer_rate);
            tag_bounds[tags_size_sum[idx].first] = Bound(last_bound+1,bound,false);
            tag_bounds[tags_size_sum[M-idx-1].first] = Bound(last_bound+1,bound,true);
            last_bound = bound;
        }
    }

    // 考虑每轮大时间段开始时调整一下分区？无效
    void partition_units(std::vector<std::pair<int,long long>> tags_size_sum){
        double buffer_rate = BUFF_RATE;
        long long total = 0;
        int idx = 0;
        int M = tags_size_sum.size();
        std::vector<long long> tmp;
        for(idx=0;idx<M;idx++){
            total += tags_size_sum[idx].second;
            tmp.push_back(total);
        }
        // 前 buffer_rate 的空间作为缓存区
        int last_bound = int(capacity*buffer_rate);
        tag_bounds[0] = Bound(1,int(capacity*buffer_rate),false);
        for(idx=0;idx<M;idx++){
            int bound = int(double(tmp[idx])/double(total)*(1-buffer_rate)*double(capacity)+capacity*buffer_rate);
            tag_bounds[tags_size_sum[idx].first] = Bound(last_bound+1,bound,false);
            last_bound = bound;
        }
        return;
    }

    std::vector<int> next_free_unit(int size, int tag){
        std::vector<int> free_units;
        bool is_reverse = tag_bounds[tag].is_reverse;
        // first pass
        if(is_reverse){
            for(int unit_id = tag_bounds[tag].upper; unit_id >= tag_bounds[tag].lower ; --unit_id){
                // 找到空位置
                bool al = true;
                for (int i = 0; i < size; ++i) {
                    int uid = unit_id - i;
                    if (uid < 1) {
                        uid += capacity;
                    }
                    if(units[uid].obj_id != -1){
                        al = false;
                    }
                }
                
                if(al) {
                    for (int i = 0; i < size; ++i) {
                        int uid = unit_id - i;
                        if (uid < 1) {
                            uid += capacity;
                        }
                        free_units.push_back(uid);
                    }
                    break;
                } 
            }
        } else {
            for(int unit_id = tag_bounds[tag].lower; unit_id >= tag_bounds[tag].upper ; ++unit_id){
                // 找到空位置
                bool al = true;
                for (int i = 0; i < size; ++i) {
                    int uid = unit_id + i;
                    if (uid > capacity) {
                        uid -= capacity;
                    }
                    if(units[uid].obj_id != -1){
                        al = false;
                    }
                }
                
                if(al) {
                    for (int i = 0; i < size; ++i) {
                        int uid = unit_id + i;
                        if (uid > capacity) {
                            uid -= capacity;
                        }
                        free_units.push_back(uid);
                    }
                    break;
                } 
            }

            // for(int unit_id = tag_bounds[tag].lower; unit_id <= tag_bounds[tag].upper ; ++unit_id){
            //     // 找到空位置
            //     if(units[unit_id].obj_id == -1){
            //         free_units.push_back(unit_id);
            //     }
            //     if(free_units.size() == size) break;
            // }
        }
        if (free_units.size() < size) {
            if(is_reverse){
                for(int unit_id = tag_bounds[tag].upper; unit_id >= tag_bounds[tag].lower ; --unit_id){
                    // 找到空位置
                    if(units[unit_id].obj_id == -1){
                        free_units.push_back(unit_id);
                    }
                    if(free_units.size() == size) break;
                }
            } else {
                for(int unit_id = tag_bounds[tag].lower; unit_id <= tag_bounds[tag].upper ; ++unit_id){
                    // 找到空位置
                    if(units[unit_id].obj_id == -1){
                        free_units.push_back(unit_id);
                    }
                    if(free_units.size() == size) break;
                }
            }
        }
        // 区间内没有足够的空位置,开始找位置塞，从缓存区开始找（unit_id=1）
        if(free_units.size() < size) {
            free_units.clear();
            for(int unit_id = 1; unit_id <= capacity; ++unit_id){
                if(units[unit_id].obj_id == -1){
                    free_units.push_back(unit_id);
                }
                if(free_units.size() == size) break;
            }
        }
        if(free_units.size() < size) {
            std::cerr << "write obj failed!" << std::endl;
            exit(1);
        }
        return free_units;
    }

    void hot_tags_record(std::unordered_map<int,std::deque<int>> common_deq){
        unique_hot_tags_circular_que.clear();
        for(auto deque : common_deq){
            int t = deque.first;
            unique_hot_tags_circular_que[t] = std::deque<int>(deque.second);
            //根据磁盘id来错开同时访问的分区
            for(int i = 0; i < id; ++i){
                int tag = unique_hot_tags_circular_que[t].front();
                unique_hot_tags_circular_que[t].pop_front();
                unique_hot_tags_circular_que[t].push_back(tag);
            }
        } 
        
    }

    void insert(int unit_id, int obj_id, int obj_offset, int tag) {
        auto unit = units + unit_id;
        unit->obj_id = obj_id;
        unit->obj_offset = obj_offset;
        tag_last_allocated[tag] = unit_id;
        used_capcity += 1;
    }
    void erase(int unit_id) {
        used_capcity -= 1;
        units[unit_id].obj_id = -1;
    }
};

struct ObjectReplica {
    int disk_id;
    std::vector<int> units;

    ObjectReplica(){}
    ObjectReplica(int disk_id, std::vector<int> units) : disk_id(disk_id), units(units) {}
};

struct Object {
    int id;
    int size;
    int tag;
    bool is_delete;
    std::vector<ObjectReplica> replicas;
    std::unordered_set<ReadRequest *> active_requests;
    std::vector<int> invalid_requests;

    Object(){}
    Object(int id, int size, int tag) : id(id), size(size), tag(tag), is_delete(false) {}
    void replica_allocate(int disk_id, std::vector<int> unit_ids){
        ObjectReplica rep(disk_id,unit_ids);
        replicas.push_back(std::move(rep));
    }

    float get_score(int obj_offset, int time);
};

struct ReadRequest {
    int req_id;
    int obj_id;
    int start_time;
    int expire_time;
    bool isdone;
    std::unordered_map<int, std::unordered_set<int>> read_blocks; // 原有记录，每个磁盘读到的块
    std::vector<bool> block_read; // 每个对象块是否被读到，大小为对象块数，初始为 false
    int unique_blocks_read = 0; // 已读到的唯一块数量

    Object *target;
    
    ReadRequest(){}
    ReadRequest(int req_id, int start_time, Object *obj) : req_id(req_id), start_time(start_time), target(obj) {
        obj_id = obj->id;
        isdone = false;
        block_read.assign(obj->size, false);
        unique_blocks_read = 0;
        expire_time = start_time + 105;
    }

    float get_score(int obj_offset, int time) {
        if (block_read[obj_offset]) {
            return 0;
        } else {
            float score;
            int delta_time = time - start_time;
            if (0 <= delta_time && delta_time <= 10) {
                score = 1 - 0.005 * delta_time;
            } else if (delta_time <= 105) {
                score = 1.05 - 0.01 * delta_time;
            } else {
                score = 0;
            }
            return score / target->size * (target->size + 1);
        }
        
    }
};
