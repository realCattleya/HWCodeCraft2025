#pragma once
#include "config.h"
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <string>
#include <iostream>

struct Unit {
    int unit_id;
    int obj_id;
    int obj_offset;
};

struct Disk {
    int id;
    int capacity;
    int used_capcity = 0;
    int head_pos = 1;
    std::vector<int> next_free_unit;
    int pre_tokens;
    std::string last_action;
    // std::unordered_map<int, Unit *> used_units;
    Unit *units;
    std::unordered_map<int, int> tag_last_allocated; // 记录每个 tag 最近一次分配的位置
    std::unordered_map<int, int> tag_continuous;     // 记录每个 tag 当前连续写入的长度

    // Disk(){}
    Disk(int id, int v) : id(id), capacity(v) {
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

    void partition_units(std::vector<long long> tags_size_sum){
        long long total = 0;
        std::vector<int> partition;
        for(long long size : tags_size_sum){
            total += size;
        }
        for(long long size : tags_size_sum){
            partition.push_back(int(double(size)/double(total)*double(capacity)));
        }
        next_free_unit.push_back(0); // tag 从 1 开始数
        int start = 1;
        for(int p : partition){
            next_free_unit.push_back(start);
            start+=p;
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
    std::unordered_set<int> active_requests;
    std::vector<int> invalid_requests;

    Object(){}
    Object(int id, int size, int tag) : id(id), size(size), tag(tag), is_delete(false) {}
    void replica_allocate(int disk_id, std::vector<int> unit_ids){
        ObjectReplica rep(disk_id,unit_ids);
        replicas.push_back(std::move(rep));
    }
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
};