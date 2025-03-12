#include <iostream>    // 输入输出流（cin/cout）
#include <vector>      // vector容器
#include <unordered_map> // 哈希表（unordered_map）
#include <set>         // 集合（set）
#include <unordered_set> // 无序集合（unordered_set）
#include <string>      // 字符串类型
#include <algorithm>   // 算法（sort）
#include <cassert>     // 断言（assert）
#include <tuple>       // 元组（tuple）
#include <cmath>       // 数学函数（ceil）
#include <cstdlib>     // 退出函数（exit）
#include <utility>     // 工具类（pair）

using namespace std;

#define MAX_DISK_NUM (10)
#define MAX_DISK_SIZE (16384)
#define MAX_REQUEST_NUM (30000000)
#define MAX_OBJECT_NUM (100000)
#define REP_NUM (3)
#define FRE_PER_SLICING (1800)
#define EXTRA_TIME (105)
#define MAX_TOKENS_NUM (1000)

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
    string last_action;
    unordered_map<int, Unit> used_units;
    unordered_map<int, int> tag_last_allocated; // 记录每个 tag 最近一次分配的位置
    unordered_map<int, int> tag_continuous;     // 记录每个 tag 当前连续写入的长度

    Disk_(int id, int v) : id(id), capacity(v) {}
} Disk;

typedef struct ObjectReplica_ {
    int disk_id;
    vector<int> units;
} ObjectReplica;

typedef struct Object_ {
    int id;
    int size;
    int tag;
    bool is_delete;
    vector<ObjectReplica> replicas;
    unordered_set<int> active_requests;
} Object;

struct ReadRequest {
    int req_id;
    int obj_id;
    int start_time;
    bool isdone;
    unordered_map<int, unordered_set<int>> read_blocks; // 原有记录，每个磁盘读到的块
    vector<bool> block_read; // 每个对象块是否被读到，大小为对象块数，初始为 false
    int unique_blocks_read = 0; // 已读到的唯一块数量
};


class StorageController {
private:
    int T; // time slices
    int M; // total tags number
    int N; // total disks number
    int V; // units number per disk
    int G; // tokens budget per disk
    vector<Disk> disks;
    unordered_map<int, Object> objects;
    unordered_map<int, ReadRequest> requests;
    unordered_set<int> pending_completed_requests; // 跟踪待完成的请求
    int current_time = 0;
    int current_time_interval = 0; // 记录当前的时间区间

    // pre-process data
    vector<vector<int>> fre_del;
    vector<vector<int>> fre_write;
    vector<vector<int>> fre_read;

    // 新增：按时间段存储热度信息
    vector<vector<double>> tag_hotness; // tag_hotness[tag][time_interval]

public:
    StorageController(int T, int M, int N, int V, int G,
                    vector<vector<int>>& del,
                    vector<vector<int>>& write,
                    vector<vector<int>>& read)
        : T(T), M(M), N(N), V(V), G(G),
          fre_del(del), fre_write(write), fre_read(read) {
        for(int i=1; i<=N; ++i)
            disks.emplace_back(i, V);

        int time_intervals = (T + FRE_PER_SLICING - 1) / FRE_PER_SLICING;
        tag_hotness.assign(M + 1, vector<double>(time_intervals, 0.0));
    }

    void pre_process(){
        int time_intervals = (T + FRE_PER_SLICING - 1) / FRE_PER_SLICING;

        // 逐个时间段计算每个 tag 的热度
        for (int tag = 1; tag <= M; ++tag) {
            for (int t = 0; t < time_intervals; ++t) {
                long long total_write = fre_write[tag - 1][t];
                long long total_read = fre_read[tag - 1][t];
                // 计算热度：读取量 / (写入量 + 1)，防止除零
                tag_hotness[tag][t] = (double)total_read / (total_write + 1);
            }
        }
        cout << "OK" << endl;
        cout.flush();
    }

    void timestamp_align(){
        string cmd;
        cin >> cmd >> current_time;
        assert(cmd == "TIMESTAMP");

        // 计算当前时间片对应的时间段索引
        current_time_interval = current_time / FRE_PER_SLICING;

        cout << "TIMESTAMP " << current_time << endl;
        cout.flush();
    }

    vector<int> handle_delete(const vector<int>& obj_ids){
        vector<int> aborted;
        for(int obj_id : obj_ids) {
            auto it = objects.find(obj_id);
            if(it == objects.end()) continue;
            // 清除该对象在各盘中占用的单元
            for(auto& rep : it->second.replicas) {
                Disk& disk = disks[rep.disk_id-1];
                for (int u : rep.units) {
                    disk.used_units.erase(u);
                }
            }
            // 取消对应的读取请求
            for(int req_id : it->second.active_requests) {
                aborted.push_back(req_id);
                requests.erase(req_id);
            }
            objects.erase(it);
        }
        return aborted;
    }

    void delete_action(){
        // recieve input
        int n_del;
        cin >> n_del;
        vector<int> del_objs(n_del);
        for(int i=0; i<n_del; ++i) cin >> del_objs[i];
        // handle_delete
        auto aborted = handle_delete(del_objs);
        cout << aborted.size() << endl;
        for(int id : aborted) cout << id << endl;
        cout.flush();
    }



    bool write_object(int id, int size, int tag) {
        // 预分配候选磁盘数组空间
        vector<Disk*> candidates;
        candidates.reserve(disks.size());
        for (auto &d : disks) {
            if (d.used_units.size() + size <= V)
                candidates.push_back(&d);
        }
        if (candidates.size() < REP_NUM) return false;

        // 调整磁盘选择策略
        const int BASE_BONUS = 1000;  // 基础分数，数值越低越优先
        const int FACTOR = 100;  // 连续性奖励因子
        sort(candidates.begin(), candidates.end(), [&, tag](Disk* a, Disk* b) {
            int bonus_a = BASE_BONUS, bonus_b = BASE_BONUS;

            // 优先选择已存储相同 `tag` 的磁盘
            if (a->tag_continuous.count(tag)) 
                bonus_a = max(0, BASE_BONUS - FACTOR * a->tag_continuous[tag]);
            if (b->tag_continuous.count(tag)) 
                bonus_b = max(0, BASE_BONUS - FACTOR * b->tag_continuous[tag]);

            // 评分 = (已用空间 + 碎片化程度)，数值越小越优
            if (a->used_units.size() + bonus_a == b->used_units.size() + bonus_b)
                return a->used_units.size() < b->used_units.size();
            return (a->used_units.size() + bonus_a) < (b->used_units.size() + bonus_b);
        });

        Object obj;
        obj.id = id;
        obj.size = size;
        obj.tag = tag;
        obj.is_delete = false;

        // 为每个副本分配空间
        for (int i = 0; i < REP_NUM; ++i) {
            Disk* target_disk = candidates[i];
            ObjectReplica rep;
            rep.disk_id = target_disk->id;
            
            int start;
            int last_alloc_before_write = target_disk->tag_last_allocated.count(tag) ? target_disk->tag_last_allocated[tag] : -1;

            // 若磁盘已有该 tag 数据，则从 tag_last_allocated[tag] 之后开始分配
            if (target_disk->tag_last_allocated.count(tag)) {
                int expected = target_disk->tag_last_allocated[tag] + 1;
                start = expected;
            } else {
                start = target_disk->next_free_unit;
            }
            if (start > target_disk->capacity) start = 1;

            int first_alloc = start;
            for (int j = 0; j < size; ++j) {
                while (target_disk->used_units.count(start)) {
                    ++start;
                    if (start > target_disk->capacity) start = 1;
                }
                rep.units.push_back(start);
                Unit unit = { start, id, j };
                target_disk->used_units[start] = unit;
                target_disk->tag_last_allocated[tag] = start;
                ++start;
                if (start > target_disk->capacity) start = 1;
            }

            // 更新 next_free_unit 为扫描结束后的 start
            target_disk->next_free_unit = start;

            // 更新 tag 连续存储信息
            if (last_alloc_before_write != -1) {  // 只有 tag 之前有存储时才计算
                int expected = last_alloc_before_write + 1;
                if (expected > target_disk->capacity) expected = 1;
                int old_cont = target_disk->tag_continuous.count(tag) ? target_disk->tag_continuous[tag] : 0;
    
                if (first_alloc == expected) {
                    target_disk->tag_continuous[tag] = old_cont + size;
                } else {
                    target_disk->tag_continuous[tag] = size;
                }
            } else {
                target_disk->tag_continuous[tag] = size;  // 第一次存储，初始化连续长度
            }

            obj.replicas.push_back(move(rep));
        }

        objects[id] = move(obj);
        return true;
    }

    void write_action(){
        // recieve input
        int n_write;
        cin >> n_write;
        vector<tuple<int, int, int>> write_ops;
        for(int i=0; i<n_write; ++i) {
            int id, size, tag;
            cin >> id >> size >> tag;
            write_ops.emplace_back(id, size, tag);
        }

        for(auto& [id, size, tag] : write_ops) {
                bool success = write_object(id, size, tag);
                if(!success) {
                    // this branch should not be triggered
                    cerr << "Write failed for object " << id << endl;
                    exit(1);
                }
                
                auto& obj = objects[id];
                cout << obj.id << endl;
                for(auto& rep : obj.replicas) {
                    cout << rep.disk_id;
                    for(int u : rep.units) cout << " " << u;
                    cout << endl;
                }
            }
            cout.flush();
    }

    void process_read_request(int req_id, int obj_id) {
        auto it = objects.find(obj_id);
        if (it == objects.end()) return;
        
        ReadRequest req;
        req.req_id = req_id;
        req.obj_id = obj_id;
        req.start_time = current_time;
        // 初始化 block_read 数组，大小为对象块数（obj.size），全部置 false
        req.block_read.assign(it->second.size, false);
        // 初始化计数器
        req.unique_blocks_read = 0;
        
        requests[req_id] = req;
        it->second.active_requests.insert(req_id);
    }
    


    vector<string> generate_disk_actions() {
        vector<string> actions;
        actions.reserve(disks.size());
        for (auto &disk : disks) {
            
            int tokens = G;  // 每个磁头每个时间片最多使用 G/2 个令牌
            string act_str;
            act_str.reserve(64); // 预估动作串长度，减少扩容
            // 如果上一个时间片有 read 动作，则根据其令牌消耗计算首次 read 消耗
            int last_read_cost = (disk.pre_tokens > 0) ? disk.pre_tokens : 0;
            
            while (tokens > 0) {
                int pos = disk.head_pos;
                auto it = disk.used_units.find(pos);
                if (it != disk.used_units.end()) {
                    // 当前有对象块，计划执行 Read 操作
                    int cost = (last_read_cost == 0) ? 64 : max(16, int(ceil(float(last_read_cost) * 0.8) ));
                    if (tokens >= cost) {
                        act_str.push_back('r');
                        tokens -= cost;
                        last_read_cost = cost;  // 更新当前 Read 消耗
                        
                        Unit u = it->second; // 当前读取的单元
                        int obj_id = u.obj_id;
                        int blk_idx = u.obj_offset;
                        auto obj_it = objects.find(obj_id);
                        if (obj_it != objects.end()) {
                            Object &obj = obj_it->second;
                            // 遍历所有活跃该对象的读请求，更新增量计数器
                            for (int req_id : obj.active_requests) {
                                auto req_it = requests.find(req_id);
                                if (req_it != requests.end()){
                                    ReadRequest &req = req_it->second;
                                    // 如果该块第一次被读到，则更新计数器
                                    if (!req.block_read[blk_idx]) {
                                        req.block_read[blk_idx] = true;
                                        req.unique_blocks_read++;
                                    }
                                    // 检查是否完成
                                    auto obj_it = objects.find(req.obj_id);
                                    if (obj_it != objects.end()) {
                                        Object& obj = obj_it->second;
                                        if (req.unique_blocks_read >= obj.size) {
                                            pending_completed_requests.insert(req.req_id);
                                        }
                                    }
                                    // 仍然保留原有 read_blocks 记录（如果有其他用途）
                                    req.read_blocks[disk.id].insert(blk_idx);
                                }
                            }
                        }
                    } else {
                        break;
                    }
                } else {
                    // 没有对象块则执行 Pass 动作，消耗 1 个令牌
                    if (tokens >= 1) {
                        act_str.push_back('p');
                        tokens -= 1;
                        last_read_cost = 0;  // 重置连续 Read 消耗记录
                    } else {
                        break;
                    }
                }
                // 更新磁头位置（环形排列）
                disk.head_pos++;
                if(disk.head_pos>disk.capacity) disk.head_pos = 1;
            }
            act_str.push_back('#');
            // 如果本时间片最后一个动作为 Read，则记录其令牌消耗；否则置 0
            if (act_str.size() > 1 && act_str[act_str.size()-2] == 'r')
                disk.pre_tokens = last_read_cost;
            else
                disk.pre_tokens = 0;
            actions.push_back(move(act_str));
        }
        return actions;
    }
    
    
    vector<int> check_completed_requests() {
        vector<int> completed;
        for (int req_id : pending_completed_requests) {
            auto req_it = requests.find(req_id);
            if (req_it == requests.end()) continue; // 请求已被处理或取消

            ReadRequest& req = req_it->second;
            auto obj_it = objects.find(req.obj_id);
            if (obj_it == objects.end()) {
                // 对象已删除，标记为完成
                completed.push_back(req_id);
                requests.erase(req_id);
            } else {
                Object& obj = obj_it->second;
                if (req.unique_blocks_read >= obj.size) {
                    completed.push_back(req_id);
                    obj.active_requests.erase(req_id);
                    requests.erase(req_id);
                }
            }
        }
        pending_completed_requests.clear();
        return completed;
    }
    

    void read_action(){
        // recieve input
        int n_read;
        cin >> n_read;
        vector<pair<int, int>> read_ops;
        for(int i=0; i<n_read; ++i) {
            int req_id, obj_id;
            cin >> req_id >> obj_id;
            read_ops.emplace_back(req_id, obj_id);
        }

        for(auto& [req_id, obj_id] : read_ops)
                process_read_request(req_id, obj_id);

        auto actions = generate_disk_actions();
        for(string& act : actions)
            cout << act << endl;
        
        auto completed = check_completed_requests();
        cout << completed.size() << endl;
        for(int id : completed) cout << id << endl;
        cout.flush();
    }

};

int main() {
    // 读取全局输入
    int T, M, N, V, G;
    cin >> T >> M >> N >> V >> G;

    auto read_matrix = [&](int T, int rows) {
        vector<vector<int>> mat(rows);
        int cnt = (T + FRE_PER_SLICING - 1) / FRE_PER_SLICING;
        for (int i = 0; i < rows; ++i) {
            mat[i].resize(cnt);
            for (int j = 0; j < cnt; ++j)
                cin >> mat[i][j];
        }
        return mat;
    };

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

