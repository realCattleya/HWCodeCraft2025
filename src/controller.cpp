#include "./include/controller.h"
#include <iostream> 
#include <cassert>
#include <cmath>
#include <algorithm>
using namespace std;

StorageController::StorageController(int T, int M, int N, int V, int G,
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

void StorageController::pre_process(){
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

// 根据当前时间段热度动态更新每块磁盘的区域规划
void StorageController::update_disk_regions() {
    for (auto &disk : disks) {
        double total_hot = 0.0;
        // 计算当前时间段所有 tag 的热度之和
        for (int tag = 1; tag <= M; ++tag) {
            total_hot += tag_hotness[tag][current_time_interval];
        }
        int current_start = 1;
        // 对每个 tag 按热度比例划分区域
        for (int tag = 1; tag <= M; ++tag) {
            int region_size = 0;
            if (total_hot == 0.0) {
                region_size = V / M;
            } else {
                region_size = int((tag_hotness[tag][current_time_interval] / total_hot) * V);
            }
            if (region_size < 1) region_size = 1;
            int region_end = current_start + region_size - 1;
            if (region_end > disk.capacity) region_end = disk.capacity;
            disk.tag_region[tag] = make_pair(current_start, region_end);
            current_start = region_end + 1;
            if (current_start > disk.capacity) break;
        }
    }
}

void StorageController::timestamp_align(){
    string cmd;
    cin >> cmd >> current_time;
    assert(cmd == "TIMESTAMP");

    // 计算当前时间片对应的时间段索引
    if (current_time_interval != current_time / FRE_PER_SLICING || current_time == 0) {
        current_time_interval = current_time / FRE_PER_SLICING;
        update_disk_regions();  // 每个新的时间段更新区域规划
    }

    cout << "TIMESTAMP " << current_time << endl;
    cout.flush();
}

vector<int> StorageController::handle_delete(const vector<int>& obj_ids){
    vector<int> aborted;
    for(int obj_id : obj_ids) {
        auto it = objects.find(obj_id);
        if(it == objects.end()) continue;
        // 清除该对象在各盘中占用的单元
        for(auto& rep : it->second.replicas) {
            Disk& disk = disks[rep.disk_id-1];
            for (int u : rep.units) {
                disk.erase(u);
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

void StorageController::delete_action(){
    // recieve input
    int n_del;
    cin >> n_del;
    vector<int> del_objs(n_del);
    for(int i=0; i<n_del; ++i)  cin >> del_objs[i];
    // handle_delete
    auto aborted = handle_delete(del_objs);
    cout << aborted.size() << endl;
    for(int id : aborted)   cout << id << endl;
    cout.flush();
}

// 辅助函数：计算指定磁盘中 tag 区域内的空闲单元数
int StorageController::free_in_region(Disk* disk, int tag) {
    if (!disk->tag_region.count(tag))
        return disk->capacity - disk->used_units.size(); // 没有规划区域则使用全盘空闲数
    auto region = disk->tag_region[tag];
    int freeCount = 0;
    for (int pos = region.first; pos <= region.second; ++pos) {
        if (!disk->used_units.count(pos))
            freeCount++;
    }
    return freeCount;
}

bool StorageController::write_object(int id, int size, int tag) {
    // 预分配候选磁盘数组空间
    vector<Disk*> candidates;
    candidates.reserve(disks.size());
    for (auto &d : disks) {
        if (d.used_units.size() + size <= V)
            candidates.push_back(&d);
    }
    if (candidates.size() < REP_NUM) return false;

    // 调整磁盘选择策略
    const int BASE_BONUS = V;  // 基础分数，数值越低越优先
    const int FACTOR_SEQ = 1;  // 连续性奖励因子
    const int FACTOR_FRE = 1;  // 负载奖励因子
    sort(candidates.begin(), candidates.end(), [&, tag](Disk* a, Disk* b) {
        int bonus_a = BASE_BONUS, bonus_b = BASE_BONUS;

        // 优先选择已存储相同 `tag` 的磁盘
        if (a->tag_continuous.count(tag)) 
            bonus_a = max(0, BASE_BONUS - FACTOR_SEQ * a->tag_continuous[tag]);
        if (b->tag_continuous.count(tag)) 
            bonus_b = max(0, BASE_BONUS - FACTOR_SEQ * b->tag_continuous[tag]);

        // 计算规划区域内的空闲单元，区域空闲越少则惩罚越大
        int free_a = free_in_region(a, tag);
        int free_b = free_in_region(b, tag);
        int region_size_a = (a->tag_region.count(tag)) ? (a->tag_region[tag].second - a->tag_region[tag].first + 1) : V;
        int region_size_b = (b->tag_region.count(tag)) ? (b->tag_region[tag].second - b->tag_region[tag].first + 1) : V;
        int penalty_a = (a->tag_region.count(tag)) ? (region_size_a - free_a) : 0;
        int penalty_b = (b->tag_region.count(tag)) ? (region_size_b - free_b) : 0;
        int score_a = a->used_units.size() * FACTOR_FRE + bonus_a + penalty_a * FACTOR_FRE;
        int score_b = b->used_units.size() * FACTOR_FRE + bonus_b + penalty_b * FACTOR_FRE;
        return score_a < score_b;
    });

    Object obj(id,size,tag);

    // 为每个副本分配空间
    for (int i = 0; i < REP_NUM; ++i) {
        Disk* target_disk = candidates[i];
        std::vector<int> target_units;
        
        // 保存写入前该 tag 在磁盘上最后一次存储的位置
        int last_alloc_before_write = target_disk->tag_last_allocated.count(tag) ? target_disk->tag_last_allocated[tag] : -1;
        
        int start;
        // 如果磁盘有为该 tag 预规划的区域，则优先在区域内分配
        if (target_disk->tag_region.count(tag)) {
            auto region = target_disk->tag_region[tag];  // (region_start, region_end)
            bool found = false;
            // 在区域内从 region.first 到 region.second 查找第一个空闲单元
            for (int pos = region.first; pos <= region.second; ++pos) {
                if (!target_disk->used_units.count(pos)) {
                    start = pos;
                    found = true;
                    break;
                }
            }
            // 若区域内没有空闲单元，则退化为全局分配：如果已有该 tag 数据，则从上次位置后开始
            if (!found) {
                if (target_disk->tag_last_allocated.count(tag))
                    start = target_disk->tag_last_allocated[tag] + 1;
                else
                    start = target_disk->next_free_unit;
            }
        } else {
            // 若没有预规划区域，则使用原来的全局分配策略
            if (target_disk->tag_last_allocated.count(tag))
                start = target_disk->tag_last_allocated[tag] + 1;
            else
                start = target_disk->next_free_unit;
        }
        if (start > target_disk->capacity) start = 1;

        // 保存本轮写入的起始分配位置，用于连续性判断
        int first_alloc = start;
        
        // 分配 size 个存储单元
        for (int j = 0; j < size; ++j) {
            while (target_disk->used_units.count(start)) {
                ++start;
                if (start > target_disk->capacity) start = 1;
            }
            target_units.push_back(start);
            // 假设 insert 函数完成插入操作：将单元 start 上写入对象 id 的第 j 块，记录 tag
            target_disk->insert(start, id, j, tag);
            ++start;
            if (start > target_disk->capacity) start = 1;
        }
        
        // 更新 next_free_unit 为扫描结束后的位置
        target_disk->next_free_unit = start;
        
        // 更新 tag 连续存储信息
        // 利用写入前保存的 last_alloc_before_write 判断连续性
        if (last_alloc_before_write != -1) {  // 只有 tag 之前已有数据时才判断
            int expected = last_alloc_before_write + 1;
            if (expected > target_disk->capacity) expected = 1;
            int old_cont = target_disk->tag_continuous.count(tag) ? target_disk->tag_continuous[tag] : 0;
            // 如果本次分配的起始位置正好等于 expected，则认为连续存储
            if (first_alloc == expected)
                target_disk->tag_continuous[tag] = old_cont + size;
            else
                target_disk->tag_continuous[tag] = size;
        } else {
            // 第一次存储，初始化连续长度为 size
            target_disk->tag_continuous[tag] = size;
        }
        
        // 将本次分配的单元记录到对象中
        obj.replica_allocate(target_disk->id, target_units);
    }

    objects[id] = move(obj);
    return true;
}

void StorageController::write_action(){
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
                for(int u : rep.units)  cout << " " << u;
                cout << endl;
            }
        }
        cout.flush();
}

void StorageController::process_read_request(int req_id, int obj_id) {
    auto obj_it = objects.find(obj_id);
    if (obj_it == objects.end()) return;
    ReadRequest req(req_id, current_time, obj_it->second);
    requests[req_id] = req;
    obj_it->second.active_requests.insert(req_id);
}
    


vector<string> StorageController::generate_disk_actions() {
    vector<string> actions;
    actions.reserve(disks.size());
    for (auto &disk : disks) {
        
        int tokens = G; 
        string act_str;
        act_str.reserve(64); // 预估动作串长度，减少扩容
        
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
            disk.head_pos = jump_target;
            disk.pre_tokens = 0;
            actions.push_back(move(act_str));
        } else {
        
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
    }
    return actions;
}
    
    
vector<int> StorageController::check_completed_requests() {
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


void StorageController::read_action(){
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
