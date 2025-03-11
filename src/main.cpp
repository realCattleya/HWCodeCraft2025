#include <iostream>    // 输入输出流（cin/cout）
#include <vector>      // vector容器
#include <unordered_map> // 哈希表（unordered_map）
#include <set>         // 集合（set）
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

typedef struct Unit_ {
    int unit_id;
    int obj_id;
    int obj_offset;
} Unit;

typedef struct Disk_ {
    int id;
    int capacity;
    int head_pos = 1;
    int pre_tokens;
    string last_action;
    unordered_map<int, Unit> used_units;

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
    set<int> active_requests;
} Object;

typedef struct ReadRequest_ {
    int req_id;
    int obj_id;
    int start_time;
    bool isdone;
    unordered_map<int, set<int>> read_blocks; // disk_id -> blocks
} ReadRequest;

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
    int current_time = 0;
    
    // pre-process data
    vector<vector<int>> fre_del;
    vector<vector<int>> fre_write;
    vector<vector<int>> fre_read;

public:
    StorageController(int T, int M, int N, int V, int G,
                    vector<vector<int>>& del,
                    vector<vector<int>>& write,
                    vector<vector<int>>& read)
        : T(T), M(M), N(N), V(V), G(G),
          fre_del(del), fre_write(write), fre_read(read) {
        for(int i=1; i<=N; ++i)
            disks.emplace_back(i, V);
    }

    void pre_process(){
        //TODO: preprocess
        cout << "OK" << endl;
        cout.flush();
    }

    void timestamp_align(){
        string cmd;
        cin >> cmd >> current_time;
        assert(cmd == "TIMESTAMP");
        cout << "TIMESTAMP " << current_time << endl;
        cout.flush();
    }

    vector<int> handle_delete(const vector<int>& obj_ids){
        vector<int> aborted;
        for(int obj_id : obj_ids) {
            auto it = objects.find(obj_id);
            if(it == objects.end()) continue;

            // erase objs disk units
            for(auto& rep : it->second.replicas) {
                Disk& disk = disks[rep.disk_id-1];
                for(int u : rep.units){
                    auto uit = disk.used_units.find(u);
                    disk.used_units.erase(uit);
                }
            }

            // cancel corresponding requests
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


    bool write_object(int id, int size, int tag){
        // choose candidates disks
        vector<Disk*> candidates;
        for(auto& d : disks) {
            if((int)d.used_units.size() + size <= V)
                candidates.push_back(&d);
        }
        if(candidates.size() < REP_NUM) return false;

        // trival stragety
        sort(candidates.begin(), candidates.end(), [](Disk* a, Disk* b) {
            return a->used_units.size() < b->used_units.size();
        });

        Object obj;
        obj.id = id;
        obj.size = size;
        obj.tag = tag;
        obj.is_delete = false;

        // allocate units
        for(int i=0; i<3; ++i) {
            Disk* target_disk = candidates[i];
            //first find free units
            ObjectReplica rep;
            rep.disk_id = target_disk->id;
            int start = 1;
            for(int j=0; j<size; ++j) {
                while(target_disk->used_units.find(start)!=target_disk->used_units.end()){
                    start++;
                }
                rep.units.push_back(start);
                Unit unit = {start, id, j};
                target_disk->used_units[start] = unit;
            }
            obj.replicas.push_back(rep);
        }

        objects[id] = obj;
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
        if(it == objects.end()) return;
        
        ReadRequest req;
        req.req_id = req_id;
        req.obj_id = obj_id;
        req.start_time = current_time;
        requests[req_id] = req;
        it->second.active_requests.insert(req_id);
    }


    vector<string> generate_disk_actions() {
        vector<string> actions;
        // 对每个硬盘生成本时间片内的磁头动作
        for(auto &disk : disks) {
            int tokens = G/2;  // 每个磁头每个时间片最多使用G个令牌
            string act_str = "";
            // 根据上一个时间片的最后一次Read动作来决定本时间片首次Read的消耗
            int last_read_cost = 0;
            if(disk.pre_tokens > 0)
                last_read_cost = max(16, (int)ceil(disk.pre_tokens * 0.8)); // 上个时间片最后一次read消耗
            // 开始模拟磁头动作（只考虑"Read"和"Pass"两种动作）
            while(tokens > 0) {
                int pos = disk.head_pos;
                // 判断当前位置是否有对象块
                auto it = disk.used_units.find(pos);
                if(it != disk.used_units.end()) {
                    // 当前有对象块，计划执行Read动作
                    int cost = (last_read_cost == 0) ? 64 : max(16, (int)ceil(last_read_cost * 0.8));
                    if(tokens >= cost) {
                        act_str.push_back('r');
                        tokens -= cost;
                        last_read_cost = cost;  // 更新当前Read动作的令牌消耗
                        // 记录本次Read操作：该盘当前位置上的数据块被读取
                        Unit u = it->second; // 该存储单元存储的对象块信息
                        int obj_id = u.obj_id;
                        int blk_idx = u.obj_offset;
                        if(objects.find(obj_id) != objects.end()){
                            Object &obj = objects[obj_id];
                            // 将该对象块的读取记录到所有正在等待该对象的读请求中
                            for (int req_id : obj.active_requests) {
                                if(requests.find(req_id) != requests.end()){
                                    requests[req_id].read_blocks[disk.id].insert(blk_idx);
                                }
                            }
                        }
                    } else {
                        // 剩余令牌不足以进行Read，停止本盘操作
                        break;
                    }
                } else {
                    // 当前没有数据块，执行Pass动作（消耗1个令牌）
                    if(tokens >= 1) {
                        act_str.push_back('p');
                        tokens -= 1;
                        // Pass动作视为未连续Read，重置last_read_cost
                        last_read_cost = 0;
                    } else {
                        break;
                    }
                }
                // 更新磁头位置（环形排列）
                disk.head_pos = (disk.head_pos % disk.capacity) + 1;
            }
            // 在动作串末尾添加结束符'#'
            act_str.push_back('#');
            // 更新磁盘状态：如果本时间片最后一个动作为Read，则记录其令牌消耗；否则置为0
            if(act_str.size() > 1 && act_str[act_str.size()-2] == 'r')
                disk.pre_tokens = last_read_cost;
            else
                disk.pre_tokens = 0;
            actions.push_back(act_str);
        }
        return actions;
    }
    
    vector<int> check_completed_requests() {
        vector<int> completed;
        // 遍历所有读请求，判断其目标对象的每个对象块是否至少有一个副本被读取
        for(auto it = requests.begin(); it != requests.end(); ) {
            ReadRequest &req = it->second;
            if(objects.find(req.obj_id) == objects.end()){
                // 对象不存在的情况（一般已在删除操作中处理），直接删除该请求
                it = requests.erase(it);
                continue;
            }
            Object &obj = objects[req.obj_id];
            bool all_read = true;
            // 对于对象的每个块（块下标0 ~ size-1），检查是否有任一副本被读取
            for (int blk = 0; blk < obj.size; blk++) {
                bool blk_ok = false;
                for(auto &rep : obj.replicas) {
                    int disk_id = rep.disk_id;
                    if(req.read_blocks.count(disk_id) && req.read_blocks[disk_id].count(blk)) {
                        blk_ok = true;
                        break;
                    }
                }
                if(!blk_ok) {
                    all_read = false;
                    break;
                }
            }
            if(all_read) {
                completed.push_back(req.req_id);
                // 完成的请求从对应对象的active_requests中移除
                obj.active_requests.erase(req.req_id);
                // 同时从全局请求中删除，确保只上报一次完成
                it = requests.erase(it);
            } else {
                ++it;
            }
        }
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

    auto read_matrix = [](int T, int rows) {
        vector<vector<int>> mat(rows);
        for(int i=0; i<rows; ++i) {
            int cnt = (T+FRE_PER_SLICING-1)/FRE_PER_SLICING; // ceil(T/1800)
            mat[i].resize(cnt);
            for(int j=0; j<cnt; ++j)
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

