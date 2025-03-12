#pragma once
#include "config.h"
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <string>

typedef struct Unit_ {
    int unit_id;
    int obj_id;
    int obj_offset;
} Unit;

typedef struct Disk_ {
    int id;
    int capacity;
    int head_pos = 1;
    int next_free_unit = 1;
    int pre_tokens;
    std::string last_action;
    std::unordered_map<int, Unit> used_units;
    std::unordered_map<int, int> tag_last_allocated; // 记录每个 tag 最近一次分配的位置
    std::unordered_map<int, int> tag_continuous;     // 记录每个 tag 当前连续写入的长度

    Disk_(){}
    Disk_(int id, int v) : id(id), capacity(v) {}
    void insert(int unit_id, int obj_id, int obj_offset, int tag) {
        Unit unit = {unit_id, obj_id, obj_offset};
        used_units[unit_id] = unit;
        tag_last_allocated[tag] = unit_id;  
    }
    void erase(int unit_id) {
        used_units.erase(unit_id);
    }
} Disk;

typedef struct ObjectReplica_ {
    int disk_id;
    std::vector<int> units;

    ObjectReplica_(){}
    ObjectReplica_(int disk_id, std::vector<int> units) : disk_id(disk_id), units(units) {}
} ObjectReplica;

typedef struct Object_ {
    int id;
    int size;
    int tag;
    bool is_delete;
    std::vector<ObjectReplica> replicas;
    std::unordered_set<int> active_requests;

    Object_(){}
    Object_(int id, int size, int tag) : id(id), size(size), tag(tag), is_delete(false) {}
    void replica_allocate(int disk_id, std::vector<int> unit_ids){
        ObjectReplica rep(disk_id,unit_ids);
        replicas.push_back(std::move(rep));
    }
} Object;

typedef struct ReadRequest_ {
    int req_id;
    int obj_id;
    int start_time;
    bool isdone;
    std::unordered_map<int, std::unordered_set<int>> read_blocks; // 原有记录，每个磁盘读到的块
    std::vector<bool> block_read; // 每个对象块是否被读到，大小为对象块数，初始为 false
    int unique_blocks_read = 0; // 已读到的唯一块数量
    
    ReadRequest_(){}
    ReadRequest_(int req_id, int start_time, Object& obj) : req_id(req_id), start_time(start_time){
        obj_id = obj.id;
        isdone = false;
        block_read.assign(obj.size, false);
        unique_blocks_read = 0;
    }
}ReadRequest;