#pragma once
#include "./struct.h"
class StorageController {
    private:
        int T; // time slices
        int M; // total tags number
        int N; // total disks number
        int V; // units number per disk
        int G; // tokens budget per disk
        std::vector<Disk *> disks;
        std::unordered_map<int, Object *> objects;
        std::unordered_map<int, ReadRequest *> requests;
        std::unordered_set<ReadRequest *> pending_completed_requests; // 跟踪待完成的请求
        int current_time = 0;
        int current_time_interval = 0; // 记录当前的时间区间
        int last_not_expire_req_id = 1;
    
        // pre-process data
        std::vector<std::vector<int>> fre_del;
        std::vector<std::vector<int>> fre_write;
        std::vector<std::vector<int>> fre_read;
    
        // 新增：按时间段存储热度信息
        std::vector<std::vector<double>> tag_hotness; // tag_hotness[tag][time_interval]
        std::vector<std::vector<std::vector<int>>> latin_templates;
    
    public:
        StorageController(int T, int M, int N, int V, int G,
            std::vector<std::vector<int>>& del,
            std::vector<std::vector<int>>& write,
            std::vector<std::vector<int>>& read);
        void pre_process();
        void timestamp_align();
        std::vector<int> handle_delete(const std::vector<int>& obj_ids);
        void delete_action();
        bool write_object(int id, int size, int tag);
        void write_action();
        void process_read_request(int req_id, int obj_id);
        std::vector<std::string> generate_disk_actions(); 
        std::vector<int> check_completed_requests(); 
        void read_action();

        int get_available_tokens() {return G;};
    };