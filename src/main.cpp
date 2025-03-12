#include<bits/stdc++.h>

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
    set<int> active_requests;
} Object;

typedef struct ReadRequest_ {
    int req_id;
    int obj_id;
    int start_time;
    bool isdone;
<<<<<<< Updated upstream
    unordered_map<int, set<int>> read_blocks; // disk_id -> blocks
} ReadRequest;
=======
    unordered_map<int, unordered_set<int>> read_blocks; // 原有记录，每个磁盘读到的块
    vector<bool> block_read; // 每个对象块是否被读到，大小为对象块数，初始为 false
    int unique_blocks_read = 0; // 已读到的唯一块数量
};

>>>>>>> Stashed changes

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
<<<<<<< Updated upstream
=======
    unordered_set<int> pending_completed_requests; // 跟踪待完成的请求
>>>>>>> Stashed changes
    int current_time = 0;
    
    // pre-process data
    vector<vector<int>> fre_del;
    vector<vector<int>> fre_write;
    vector<vector<int>> fre_read;

    // 新增：tag_hotness[ tag ] 表示该 tag 的热度
    vector<double> tag_hotness; // 索引 1...M

public:
    StorageController(int T, int M, int N, int V, int G,
                    vector<vector<int>>& del,
                    vector<vector<int>>& write,
                    vector<vector<int>>& read)
        : T(T), M(M), N(N), V(V), G(G),
          fre_del(del), fre_write(write), fre_read(read) {
        for(int i=1; i<=N; ++i)
            disks.emplace_back(i, V);
        tag_hotness.resize(M+1, 0.0);
    }

    void pre_process(){
        // 对每个 tag（1~M）统计写入和读取总数
        for (int tag = 1; tag <= M; ++tag) {
            long long total_write = 0, total_read = 0;
            // fre_write 和 fre_read 的下标分别为 tag-1 行
            for (int j = 0; j < fre_write[0].size(); ++j) {
                total_write += fre_write[tag-1][j];
                total_read += fre_read[tag-1][j];
            }
            // 定义热度：读取量 / (写入量+1)
            tag_hotness[tag] = (double)total_read / (total_write + 1);
        }
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
            // 清除该对象在各盘中占用的单元
            for(auto& rep : it->second.replicas) {
                Disk& disk = disks[rep.disk_id-1];
                for(int u : rep.units){
                    auto uit = disk.used_units.find(u);
                    disk.used_units.erase(uit);
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


    bool write_object(int id, int size, int tag){
        // choose candidates disks
        vector<Disk*> candidates;
        for(auto& d : disks) {
            if((int)d.used_units.size() + size <= V)
                candidates.push_back(&d);
        }
        if(candidates.size() < REP_NUM) return false;

<<<<<<< Updated upstream
        // trival stragety
        sort(candidates.begin(), candidates.end(), [](Disk* a, Disk* b) {
            return a->used_units.size() < b->used_units.size();
=======
        // 候选盘排序：同时考虑已用空间和该盘上该 tag 的连续写入长度
        // 连续写入越长，给予的 bonus 越低，从而优先选择连续性更好的盘
        const int BASE_BONUS = 1000;
        const int FACTOR = 100;
        sort(candidates.begin(), candidates.end(), [&, tag](Disk* a, Disk* b) {
            int bonus_a = BASE_BONUS;
            int bonus_b = BASE_BONUS;
            if (a->tag_continuous.count(tag)) {
                bonus_a = max(0, BASE_BONUS - FACTOR * a->tag_continuous[tag]);
            }
            if (b->tag_continuous.count(tag)) {
                bonus_b = max(0, BASE_BONUS - FACTOR * b->tag_continuous[tag]);
            }
            // 评分 = 已用单元数 + bonus，数值越小越优
            if (a->used_units.size() + bonus_a == b->used_units.size() + bonus_b)
                return a->used_units.size() < b->used_units.size();
            return (a->used_units.size() + bonus_a) < (b->used_units.size() + bonus_b);
>>>>>>> Stashed changes
        });

        Object obj;
        obj.id = id;
        obj.size = size;
        obj.tag = tag;
        obj.is_delete = false;

<<<<<<< Updated upstream
        // allocate units
        for(int i=0; i<3; ++i) {
=======
        // 为每个副本分配空间
        for (int i = 0; i < REP_NUM; ++i) {
>>>>>>> Stashed changes
            Disk* target_disk = candidates[i];
            //first find free units
            ObjectReplica rep;
            rep.disk_id = target_disk->id;
<<<<<<< Updated upstream
            int start = 1;
            for(int j=0; j<size; ++j) {
                while(target_disk->used_units.find(start)!=target_disk->used_units.end()){
                    start++;
=======
            int start;
            // 如果盘上已有该 tag 的分配记录，则尝试从预期位置开始分配
            if (target_disk->tag_last_allocated.count(tag)) {
                int expected = (target_disk->tag_last_allocated[tag] % target_disk->capacity) + 1;
                start = expected;
            } else {
                start = target_disk->next_free_unit;
            }
            if (start > target_disk->capacity) start = 1;

            // 记录本次分配的第一个空闲单元，用于判断是否连续
            int first_alloc = start;
            // 分配 size 个存储单元
            for (int j = 0; j < size; ++j) {
                while (target_disk->used_units.count(start)) {
                    ++start;
                    if (start > target_disk->capacity) start = 1;
>>>>>>> Stashed changes
                }
                rep.units.push_back(start);
                Unit unit = {start, id, j};
                target_disk->used_units[start] = unit;
<<<<<<< Updated upstream
            }
            obj.replicas.push_back(rep);
        }

        objects[id] = obj;
=======
                // 更新 last_allocated 在本轮内将最终更新为最后分配的单元
                target_disk->tag_last_allocated[tag] = start;
                ++start;
                if (start > target_disk->capacity) start = 1;
            }
            // 更新 next_free_unit 为扫描结束后的 start
            target_disk->next_free_unit = start;

            // 判断本次分配是否与之前连续：
            // 若盘上已有该 tag 的记录，期望值为 (旧值 % capacity)+1
            if (target_disk->tag_last_allocated.count(tag)) {
                int expected = ((target_disk->tag_last_allocated[tag] - rep.units.back() + rep.units.back()) % target_disk->capacity) + 1;
                // 注意：这里我们用 first_alloc 和预期值比较
                if (target_disk->tag_last_allocated[tag] != 0) { // 若已有记录
                    int old_cont = (target_disk->tag_continuous.count(tag)) ? target_disk->tag_continuous[tag] : 0;
                    // 如果 first_alloc 与预期相等，则认为连续
                    if (first_alloc == ((target_disk->tag_last_allocated[tag] - size) % target_disk->capacity) + 1) {
                        target_disk->tag_continuous[tag] = old_cont + size;
                    } else {
                        target_disk->tag_continuous[tag] = size;
                    }
                }
            } else {
                // 没有记录则直接设为 size
                target_disk->tag_continuous[tag] = size;
            }
            obj.replicas.push_back(move(rep));
        }
        objects[id] = move(obj);
>>>>>>> Stashed changes
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
<<<<<<< Updated upstream
        // 对每个硬盘生成本时间片内的磁头动作
        for(auto &disk : disks) {
            int tokens = G;  // 每个磁头每个时间片最多使用G个令牌
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
=======
        actions.reserve(disks.size());
        for (auto &disk : disks) {
            string act_str;
            // 预设令牌数
            int tokens = G;
            act_str.reserve(64);

            // 判断是否采用 Jump 策略（仅在时间片初始允许）
            int jump_target = -1;
            const int search_limit = G; // 向前搜索上限
            int steps = 0;
            int pos = disk.head_pos;
            while (steps <= search_limit) {
                if (disk.used_units.count(pos)) {
                    Unit u = disk.used_units[pos];
                    auto obj_it = objects.find(u.obj_id);
                    if (obj_it != objects.end()) {
                        bool needed = false;
                        // 检查该对象是否有活跃请求且该块还未读取
                        for (int req_id : obj_it->second.active_requests) {
                            auto req_it = requests.find(req_id);
                            if (req_it != requests.end()){
                                if (!req_it->second.block_read[u.obj_offset]) {
                                    needed = true;
                                    break;
                                }
                            }
                        }
                        if (needed) break;
                    }
                }
                pos++;
                if (pos > disk.capacity) pos = 1;
                steps++;
            }
            // 如果空闲区间超过阈值（例如G个单位），则采用 Jump
            if (steps >= G && steps <= search_limit && tokens == G) {
                jump_target = pos;
            }
            if(jump_target != -1) {
                act_str = "j " + to_string(jump_target);
                tokens = 0; // Jump后本时间片内不再有动作
                // 更新 head_pos 为 jump_target 后的下一个位置
                disk.head_pos = jump_target + 1;
                if(disk.head_pos > disk.capacity) disk.head_pos = 1;
            } else {
                // 原有逻辑：循环执行 read 或 pass 动作
                int last_read_cost = (disk.pre_tokens > 0) ? disk.pre_tokens : 0;
                while (tokens > 0) {
                    int pos = disk.head_pos;
                    auto it = disk.used_units.find(pos);
                    if (it != disk.used_units.end()){
                        int cost = (last_read_cost == 0) ? 64 : max(16, int(ceil(float(last_read_cost) * 0.8)));
                        if (tokens >= cost) {
                            act_str.push_back('r');
                            tokens -= cost;
                            last_read_cost = cost;
                            
                            Unit u = it->second;
                            int obj_id = u.obj_id;
                            auto obj_it = objects.find(obj_id);
                            if (obj_it != objects.end()){
                                Object &obj = obj_it->second;
                                for (int req_id : obj.active_requests) {
                                    auto req_it = requests.find(req_id);
                                    if (req_it != requests.end()){
                                        ReadRequest &req = req_it->second;
                                        if (!req.block_read[u.obj_offset]) {
                                            req.block_read[u.obj_offset] = true;
                                            req.unique_blocks_read++;
                                        }
                                        // 检查是否完成
                                        if (req.unique_blocks_read >= obj.size) {
                                            pending_completed_requests.insert(req.req_id);
                                        }
                                        req.read_blocks[disk.id].insert(u.obj_offset);
                                    }
                                }
                            }
                        } else {
                            break;
                        }
                    } else {
                        if (tokens >= 1) {
                            act_str.push_back('p');
                            tokens -= 1;
                            last_read_cost = 0;
                        } else {
                            break;
                        }
                    }
                    disk.head_pos++;
                    if(disk.head_pos > disk.capacity) disk.head_pos = 1;
                }
                act_str.push_back('#');
                if (act_str.size() > 1 && act_str[act_str.size()-2] == 'r')
                    disk.pre_tokens = last_read_cost;
                else
                    disk.pre_tokens = 0;
            }
            actions.push_back(move(act_str));
>>>>>>> Stashed changes
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

<<<<<<< Updated upstream
    auto read_matrix = [](int rows) {
=======
    auto read_matrix = [&](int T, int rows) {
>>>>>>> Stashed changes
        vector<vector<int>> mat(rows);
        int cnt = (T + FRE_PER_SLICING - 1) / FRE_PER_SLICING;
        for (int i = 0; i < rows; ++i) {
            mat[i].resize(cnt);
            for (int j = 0; j < cnt; ++j)
                cin >> mat[i][j];
        }
        return mat;
    };

    auto fre_del = read_matrix(M);
    auto fre_write = read_matrix(M);
    auto fre_read = read_matrix(M);

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

